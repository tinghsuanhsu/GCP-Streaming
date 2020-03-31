import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
import json
import argparse
from datetime import datetime, timezone
import pytz
from tzlocal import get_localzone
import logging


###########
# TODO: explore the difference between Map and ParDo)
###########

# class parse_pubsub(beam.DoFn):

#     def process(self, message): 

#         formatted = json.loads(message)
#         address=json.loads(formatted['location']['human_address'])
#         formatted_message = {'bay_id': formatted['bay_id'],
#                             'st_marker_id': formatted['st_marker_id'],
#                             'status': formatted['status'],
#                             'latitude': formatted['location']['latitude'],
#                             'longitude': formatted['location']['longitude'],
#                             'address': address['address'],
#                             'state': address['state'],
#                             'state': address['state'],
#                             'zip': address['zip']
#                         }
#             # 'timestamp': timestamp
#         yield formatted_message
def process(message, timestamp=beam.DoFn.TimestampParam): 

    formatted = json.loads(message)
    tz=pytz.timezone('Australia/Melbourne')
    timestamp=tz.fromutc(timestamp.to_utc_datetime()).strftime('%Y-%m-%d %H:%M:%S')
    # timestamp=timestamp.to_utc_datetime().astimezone(get_localzone()).strftime('%Y-%m-%d %H:%M:%S')
    # timestamp=datetime.utcfromtimestamp(timestamp).replace(tzinfo=AEST).strftime('%Y-%m-%d %H:%M:%S')

    address=json.loads(formatted['location']['human_address'])
    formatted_message = {'bay_id': formatted['bay_id'],
                        'st_marker_id': formatted['st_marker_id'],
                        'status': formatted['status'],
                        'latitude': formatted['location']['latitude'],
                        'longitude': formatted['location']['longitude'],
                        'address': address['address'],
                        'state': address['state'],
                        'city': address['city'],
                        'zip': address['zip'],
                        'timestamp': timestamp
                    }
        
    return formatted_message



parser = argparse.ArgumentParser()
parser.add_argument('--input_subscription')
parser.add_argument('--input_mode', default='stream')
parser.add_argument('--input_topic', default='parking')
parser.add_argument('--table', default='parking')
parser.add_argument('--dataset', default='mydataset')
# parser.add_argument('--project', default='parking-streaming')


known_args, pipeline_args = parser.parse_known_args()

pipeline_options = PipelineOptions(pipeline_args)
pipeline_options.view_as(SetupOptions).save_main_session = True

if known_args.input_mode == 'stream':
    pipeline_options.view_as(StandardOptions).streaming = True



def run():
    with beam.Pipeline(options=pipeline_options) as p:
        table_schema = {
            'fields': [
            {'name': 'bay_id', 'type': 'NUMERIC', 'mode': 'REQUIRED'}, 
            {'name': 'st_marker_id', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
            {'name': 'status', 'type': 'STRING', 'mode': 'REQUIRED'}, 
            {'name': 'latitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'longitude', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'}, 
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'}, 
            {'name': 'zip', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
            {'name': 'message_publish_time', 'type': 'DATETIME', 'mode': 'NULLABLE'}
            ], 
        }

        
        if known_args.input_subscription:
            lines = ( p | 'Read from PubSub' >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription) 
                       # | 'Parse data' >> beam.ParDo(parse_pubsub())
                       | 'Parse data' >> beam.Map(process)
                       | 'Write to BigQuery' >> beam.io.WriteToBigQuery(table=known_args.table,schema=table_schema, dataset=known_args.dataset, project='parking-streaming'))
                       # | 'Print output' >> beam.Map(print))
        else:
            lines = ( p | beam.io.ReadStringsFromPubSub(topic=known_args.input_topic))

    # result = p.run()
    # result.wait_until_finish()
       
    


if __name__ == '__main__':
    # logging.getLogger().setLevel(logging.INFO)
    logging.info('Pipe Start')

    run()
    logging.info('Pipe Completed')


       