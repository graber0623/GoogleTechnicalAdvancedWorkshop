gcloud dataproc workflow-templates create $TEMPLATE_ID --region=$REGION --project=$PROJECT_ID

gcloud beta dataproc workflow-templates set-managed-cluster $TEMPLATE_ID \

> --region $REGION \
--project=$PROJECT_ID \
--cluster-name $WORK_CLUSTER_NAME \
--metadata 'PIP_PACKAGES=pyspark numpy pandas google-cloud-storage google-cloud-bigquery gcsfs google-oauth google-cloud-pubsub' \
--image-version 1.4 \
--properties core:fs.defaultFS=gs://$BUCKET_NAME \
--initialization-actions=gs://goog-dataproc-initialization-actions-asia-northeast3/python/pip-install.sh \
--service-account cloocus-sa-test@cloocus-da-solution.iam.gserviceaccount.com
> 

gcloud dataproc workflow-templates add-job pyspark \

> gs://$BUCKET_NAME/$PYTHON_FILE \
--step-id $STEP_ID \
--workflow-template $TEMPLATE_ID \
--region $REGION \
--properties spark.jars.packages='org.apache.spark:spark-avro_2.11:2.4.0' \
--project=$PROJECT_ID