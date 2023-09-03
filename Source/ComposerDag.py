import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocInstantiateWorkflowTemplateOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago

q3 = """
    WITH NOW_STORE
    AS
    (
    select STORE_SKID, MAX(SALES_DATE) AS LAST_DATE
    FROM `cloocus-da-solution.dw_cloocus.dw_cloocus_POS_RETAIL_FCT`
    GROUP BY STORE_SKID
    HAVING LAST_DATE = "2018-07-30"
    )
    SELECT FCT.SALES_DATE, FCT.STORE_SKID, sum(POS_VAL_SALES_AMT) as Total_Sales
    FROM `cloocus-da-solution.dw_cloocus.dw_cloocus_POS_RETAIL_FCT` AS FCT RIGHT JOIN NOW_STORE ON FCT.STORE_SKID = NOW_STORE.STORE_SKID
    RIGHT JOIN `cloocus-da-solution.dw_cloocus.dw_cloocus_STORE_DIM2` AS DIM2 ON FCT.STORE_SKID = DIM2.STORE_SKID  
    where FCT.SALES_DATE > DATE_SUB("2018-07-30", INTERVAL 1 year)
    group by SALES_DATE, STORE_SKID
"""

q4 = """
    SELECT
    STORE_SKID,
    history_timestamp AS timestamp,
    history_value,
    NULL AS forecast_value,
    NULL AS prediction_interval_lower_bound,
    NULL AS prediction_interval_upper_bound
    FROM
    (
    SELECT
        SALES_DATE AS history_timestamp,
        Total_Sales AS history_value,
        STORE_SKID
    FROM
        `cloocus-da-solution.dm_cloocus.dm_cloocus_FORECAST_BATCH_TABLE`
    )
    UNION ALL
    SELECT
    STORE_SKID,
    date(forecast_timestamp) AS timestamp,
    NULL AS history_value,
    forecast_value,
    prediction_interval_lower_bound,
    prediction_interval_upper_bound
    FROM
    ML.FORECAST(MODEL dm_cloocus.dm_cloocus_FORECAST_TOTAL_SALES_ARIMA,
                STRUCT(14 AS horizon, 0.8 AS confidence_level))
"""

q5 = """
    SELECT
    FACT.SALES_DATE
    ,STORE_DM.STORE_NAME
    ,STORE_DM.STORE_SKID
    ,PROD_DM.AJ_STD_SEG_NAME
    ,sum(FACT.POS_VAL_SALES_AMT) value

    FROM `cloocus-da-solution.dw_cloocus.dw_cloocus_POS_RETAIL_FCT` AS FACT


    INNER JOIN `cloocus-da-solution.dw_cloocus.dw_cloocus_PROD_DIM` AS PROD_DM

    ON    FACT.PROD_SKID =    PROD_DM.PROD_SKID --PROD_SKID

    INNER JOIN `cloocus-da-solution.dw_cloocus.dw_cloocus_STORE_DIM2` AS STORE_DM

    ON    FACT.STORE_SKID =      STORE_DM.STORE_SKID -- STORE_DM

    GROUP BY
    FACT.SALES_DATE
    ,STORE_DM.STORE_NAME
    ,STORE_DM.STORE_SKID
    ,PROD_DM.AJ_STD_SEG_NAME
    ORDER BY 1
"""
project_id = models.Variable.get("project_id")

default_args = {
    "start_date": days_ago(1),
    "project_id": project_id,
}

with models.DAG(
    "hellocloocus_datapipeline_final",
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
) as dag:
    
    t1 = DataprocInstantiateWorkflowTemplateOperator(
        task_id="fct_gcs_to_bq_task",
        template_id= "template-cloocus-csv-gcs-to-bq",
        project_id=project_id,
        region="asia-northeast3"
    )
    
    t2_a= DataprocInstantiateWorkflowTemplateOperator(
        task_id="dim_pubsub_to_gcs_task",
        template_id= "template-cloocus-dim-ps-to-gcs",
        project_id=project_id,
        region="asia-northeast3"
    )
    
    t2_b= DataprocInstantiateWorkflowTemplateOperator(
        task_id="dim_gcs_to_bq_task",
        template_id= "template-cloocus-dim-gcs-to-bq",
        project_id=project_id,
        region="asia-northeast3"
    )
    
    t3 = BigQueryOperator(
        task_id='create_FORECAST_BATCH_TABLE_task',
        sql=q3, 
        use_legacy_sql=False,
        destination_dataset_table='dm_cloocus.dm_cloocus_FORECAST_BATCH_TABLE',
        write_disposition='WRITE_TRUNCATE'
    )
    
    t4 = BigQueryOperator(
        task_id='create_FORECAST_PREDICT_ROW_task',
        sql=q4, 
        use_legacy_sql=False,
        destination_dataset_table='dm_cloocus.dm_cloocus_FORECAST_PREDICT_ROW',
        write_disposition='WRITE_TRUNCATE'
    )
    
    t5 = BigQueryOperator(
        task_id='create_FORECAST_VISUALIZATION_task',
        sql=q5, 
        use_legacy_sql=False,
        destination_dataset_table='cloocus-da-solution.dm_cloocus.dm_cloocus_FORECAST_VISUALIZATION',
        write_disposition='WRITE_TRUNCATE'
    )
    
    t1 >> t3
    
    t2_a >> t2_b >> t3
    
    t3 >> t4 >> t5