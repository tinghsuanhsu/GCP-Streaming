
import argparse



def get_arg(argv=None):

	parser = argparse.ArgumentParser()
	parser.add_argument('--pubsub_topic', required=True, default=None, help='enter a name for your pubsub topic')
	parser.add_argument('--api_topic', required=False, default='vh2v-4nfs', help='enter the dataset you want to retrieve')
	parser.add_argument('--api_record_limit', required=False, default=10, help='enter the number of records you want to retrieve')
	parser.add_argument('--api_record_format', required=False, default='json', help='enter the format of the retrieved data')
	parser.add_argument('--project_id', required=True, default=None, help='enter project ID you want to create the pubsub topic under')
	parser.add_argument('--time_interval', required=True, default=None, help='time interval')
	args = parser.parse_args()


	return args

