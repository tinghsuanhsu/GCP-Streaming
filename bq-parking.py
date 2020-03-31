
from google.cloud import bigquery
# Imports the Google Cloud client library
from google.cloud import storage
import argparse
import json
from google.cloud import pubsub_v1


def get_arg(argv=None):

	parser = argparse.ArgumentParser()
	parser.add_argument('--dataset', required=True, default=None, help='enter a name for your BigQuery dataset')
	parser.add_argument('--bucket_name', required=True, default=None, help='enter the bucket name to store the streaming data')
	parser.add_argument('--table_name', required=True, default=None, help='enter the table name to store the processed data in BigQuery')
	parser.add_argument('--pubsub_topic', required=True, default=None, help='enter a name for your pubsub topic')
	parser.add_argument('--subscription', required=True, default=None, help='enter a name for your pubsub subscription')
	parser.add_argument('--project_id', required=True, default=None, help='enter project ID you want to create the pubsub topic under')

	args = parser.parse_args()

	return args

def bq_create_dataset(dataset):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset)

    try:
        bigquery_client.get_dataset(dataset_ref)
        print ('Dataset existing')
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))

def bq_create_table(table_name):
	SCHEMA = [
	        bigquery.SchemaField('full_name', 'STRING', mode='required'),
	        bigquery.SchemaField('age', 'INTEGER', mode='required'),
	    ]
    table_ref = dataset.table(table_name)
    table = bigquery.Table(table_ref, schema=SCHEMA)
    table = client.create_table(table)      # API request

    assert table.table_id == 'my_table'

def gcs_create_bucket(bucket_name):
	# Instantiates a client
	storage_client = storage.Client()

	# Creates the new bucket
	bucket = storage_client.create_bucket(bucket_name)

	print("Bucket {} created.".format(bucket.name))

def pubsub_create_topic(project_id, topic):

    service_account_info = json.load(open("service_account_info.json"))
    audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"
    # credentials = jwt.Credentials.from_service_account_info(service_account_info, audience=audience)

    # publisher = pubsub_v1.PublisherClient(credentials = credentials) 
    publisher = pubsub_v1.PublisherClient()        
    topic_path = publisher.topic_path(project_id, topic)

    try: 
        topic = publisher.create_topic(topic_path)
        print("Topic created: {}".format(topic))
    except Exception as ex:
            print('Cannot create topic.')
            print(str(ex)) 

def pubsub_create_subscription(project_id, topic, subscription_name):

    subscriber = pubsub_v1.SubscriberClient()
    topic_path = subscriber.topic_path(project_id, topic)
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    try:
        subscription = subscriber.create_subscription(subscription_path, topic_path)
        print("Subscription created: {}".format(subscription))
    except Exception as ex:
            print('Cannot create subscription.')
            print(str(ex)) 
    

if __name__ == '__main__':
    arg=get_arg()
    bq_create_dataset(arg.dataset)
    bq_create_table(arg.table)
    gcs_create_bucket(arg.bucket_name)
    pubsub_create_topic(arg.project_id, arg.pubsub_topic)
    pubsub_create_subscription(arg.project_id, arg.pubsub_topic, arg.subscription)
