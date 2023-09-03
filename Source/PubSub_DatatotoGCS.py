from google.cloud import bigquery
import os
from datetime import datetime
from pytz import timezone


client = bigquery.Client()


table_id = "cloocus-da-solution.dw_cloocus.dw_cloocus_POSDS_CUST_DIM"
bucket_name = "cloocus-taw-retail-dimension"

KST = timezone('Asia/Seoul')
now = datetime.now(KST)
year = str(now.year)
month = str(now.month)
day = str(now.day)
hour = str(now.hour)

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("CUST_ID", "INTEGER"),
        bigquery.SchemaField("CUST_NAME_ABBR", "STRING"),
        bigquery.SchemaField("CUST_NAME", "STRING"),
        bigquery.SchemaField("RETLR", "STRING"),
        bigquery.SchemaField("GLOBL_REGN", "STRING"),
        bigquery.SchemaField("CUST_GRP", "STRING"),
        bigquery.SchemaField("CNTRY_NAME", "STRING"),
        bigquery.SchemaField("CNTRY_ID", "STRING"),
        bigquery.SchemaField("ISO_CNTRY_NUM", "INTEGER"),
        bigquery.SchemaField("ISO_CNTRY_CODE", "STRING"),
        bigquery.SchemaField("CRNCY_ISO_NAME", "STRING"),
        bigquery.SchemaField("CRNCY_ISO_NUM", "INTEGER"),
        bigquery.SchemaField("CRNCY_NAME", "STRING"),
        bigquery.SchemaField("USD_EXCHG_RATE", "FLOAT"),
        bigquery.SchemaField("CUST_LOGO_URL", "STRING"),
        bigquery.SchemaField("CUST_ICON_URL", "STRING"),
        bigquery.SchemaField("CUST_HEX_COLOR_CODE",	"STRING"),
        bigquery.SchemaField("REAL_WK_DAY", "STRING"),
        bigquery.SchemaField("CUST_WK_DAY", "STRING"),
        bigquery.SchemaField("CAL_WK_DAY", "STRING"),
        bigquery.SchemaField("REAL_SCNAR_WK", "STRING"),
        bigquery.SchemaField("CUST_SCNAR_WK", "STRING"),
        bigquery.SchemaField("CAL_SCNAR_WK", "STRING"),
        bigquery.SchemaField("REAL_SCNAR_MTH", "STRING"),
        bigquery.SchemaField("CUST_SCNAR_MTH", "STRING"),
        bigquery.SchemaField("CAL_SCNAR_MTH", "STRING"),
        bigquery.SchemaField("ANTITRUST_VOL_DAY_NUM", "STRING"),
        bigquery.SchemaField("ANTITRUST_TRNVR_DAY_NUM", "STRING"),
        bigquery.SchemaField("ANTITRUST_OTHER_DAY_NUM", "STRING"),
        bigquery.SchemaField("ANTITRUST_SHELF_PRICE_DAY_NUM", "STRING"),
        bigquery.SchemaField("DAILY_FLAG", "BOOLEAN"),
        bigquery.SchemaField("WKLY_FLAG", "BOOLEAN"),
        bigquery.SchemaField("MTHLY_FLAG", "BOOLEAN"),
        bigquery.SchemaField("PROD_COMPL_TYPE", "STRING"),
        bigquery.SchemaField("ANTI_TRUST_FLAG", "INTEGER"),
        bigquery.SchemaField("COMP_DATA_FLAG", "STRING"),
        bigquery.SchemaField("ANTI_TRUST_EXCL_RATIONALE", "STRING"),
        bigquery.SchemaField("ECOM_FLAG", "STRING"),
        bigquery.SchemaField("VALUES_INCL_SALES_TAX_FLAG", "STRING"),
        bigquery.SchemaField("STORE_INV_AVAIL_FLAG", "STRING"),
        bigquery.SchemaField("WHSE_INV_AVAIL_FLAG", "STRING"),
        bigquery.SchemaField("PURCH_ORDR_INF_AVAIL_FLAG", "STRING"),
        bigquery.SchemaField("CUST_ABBR", "STRING"),
        bigquery.SchemaField("store_whse_link_strat", "STRING"),
        bigquery.SchemaField("ALT_CAL_ID", "STRING"),
        bigquery.SchemaField("PRDMT_GEO_ID", "INTEGER"),
        bigquery.SchemaField("GEO_NAME", "STRING"),
        bigquery.SchemaField("REGION_NAME", "STRING"),
        bigquery.SchemaField("AREA_NAME", "STRING"),
        bigquery.SchemaField("GROUP_NAME", "STRING"),
        bigquery.SchemaField("REPORTING_COUNTRY_NAME", "STRING"),
        bigquery.SchemaField("COUNTRY_NAME", "STRING"),	
        bigquery.SchemaField("mkt_wk_day", "STRING"),
        bigquery.SchemaField("mkt_scnar_wk", "STRING"),
        bigquery.SchemaField("mkt_scnar_mth", "STRING"),
        bigquery.SchemaField("LATEST_INV_WK_SCENARIO", "STRING"),
        bigquery.SchemaField("CUST_PG_MTH_PATTERN", "STRING"), 
        bigquery.SchemaField("MKT_PG_MTH_PATTERN", "STRING"),
        bigquery.SchemaField("POS_INV_COLUMN", "STRING"),
        bigquery.SchemaField("WH_INV_COLUMN", "STRING"),
        bigquery.SchemaField("CUST_CALENDAR_FLAG", "STRING"),
        bigquery.SchemaField("CUST_LOGO_ENCODED", "STRING"),
    ],

    skip_leading_rows=1,

    source_format=bigquery.SourceFormat.CSV,
    write_disposition="WRITE_TRUNCATE",
)

uri = f"gs://{bucket_name}/batch_data/year={year}/month={month}/day={day}/retail_data*.csv"
load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.
load_job.result()  # Waits for the job to complete.
destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))