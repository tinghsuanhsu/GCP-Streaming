
from sodapy import Socrata
from time import sleep
from google.cloud import pubsub_v1
import json
from google.auth import jwt
import argparse
from parkingUtil import get_arg


def create_topic():

    service_account_info = json.load(open("service_account_info.json"))
    audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"
    credentials = jwt.Credentials.from_service_account_info(service_account_info, audience=audience)

    # publisher = pubsub_v1.PublisherClient(credentials = credentials)   
    publisher = pubsub_v1.PublisherClient()        
     
    topic_path = publisher.topic_path("parking-streaming", "test2")

    try: 
        topic = publisher.create_topic(topic_path)
        print("Topic created: {}".format(topic))
    except Exception as ex:
            print('Exception while connecting to record source.')
            print(str(ex)) 

create_topic()