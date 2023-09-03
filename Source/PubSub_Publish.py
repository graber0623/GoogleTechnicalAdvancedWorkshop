from concurrent import futures
from google.cloud import pubsub_v1
from google.cloud import bigquery
import os
import pandas as pd
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/graber1991/gcpstudy/gcp_taw/cloocus-da-solution-79da86c23698.json"

bq_df= pd.read_csv("~/gcpstudy/gcp_taw/POSDS_CUST_DIM_enhanced_AJ.csv")

project_id = "cloocus-da-solution"
topic_id = "hellocloocus-dim-topic-v4"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

publish_futures=[]
print(bq_df.shape[0])
for i in range(bq_df.shape[0]):
    row_json = bq_df.iloc[[i]].to_json(orient='records')
    print(row_json)
    publish_future = publisher.publish(topic_path, row_json.encode("utf-8"))
    print(publish_future.result())

print(f"Published messages to {topic_path}.")