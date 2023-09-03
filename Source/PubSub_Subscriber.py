from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import storage
from datetime import datetime
from pytz import timezone
from google.api_core import retry

import os
import pandas as pd
import numpy as np
import json
from io import BytesIO

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/graber1991/gcpstudy/gcp_taw/cloocus-da-solution-79da86c23698.json"

project_id = "cloocus-da-solution"
subscription_id = 'hellocloocus-dim-topic-v4-sub'

timeout = 360.0
message_count = 0

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)


def subscription_pulling_function():
    global message_count
    
    KST = timezone('Asia/Seoul')
    now = datetime.now(KST)
    year = str(now.year)
    month = str(now.month)
    day = str(now.day)
    hour = str(now.hour)
    
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    NUM_MESSAGES = 1000
    
    bucket_name = "cloocus-taw-retail-dimension"
    storage_client = storage.Client(project_id)
    bucket = storage_client.bucket(bucket_name)
    
    with subscriber:
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": NUM_MESSAGES},
            retry = retry.Retry(deadline=300),
        )
        
    if len(response.received_messages) == 0:
        print("NO MESSAGES")
        return
    
    #ack_ids = []

    for received_message in response.received_messages:
        result = received_message.message.data.decode('utf-8')
        
        data = json.loads(result)
        data_pd = pd.DataFrame(data)
        data_pd = data_pd.drop(columns=["Unnamed: 0"])
        print(data_pd.head())

        bucket.blob(f'batch_data/year={year}/month={month}/day={day}/retail_data{str(message_count)}.csv').upload_from_string(data_pd.to_csv(index=False), 'text/csv')
        
        
        message_count +=1
        print(message_count)
        
    print(f"message updated to {bucket_name}")

subscription_pulling_function()