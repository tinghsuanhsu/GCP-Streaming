##### testing1 
## ParkingListener working 


from sodapy import Socrata
from time import sleep
from google.cloud import pubsub_v1
import json
from google.auth import jwt
import argparse
from parkingUtil import get_arg


class ParkingListener:
    
    
    def __init__(self, 
                pubsub_topic=None, 
                api_topic=None, 
                api_record_limit=None, 
                api_record_format=None,
                project_id=None,
                time_interval=None,
                client=None):
        
        self.api_results = None
        self.pubsub_topic=pubsub_topic
        self.api_topic=api_topic
        self.api_record_limit=int(api_record_limit)
        self.api_record_format=api_record_format
        self.project_id=project_id
        self.time_interval=int(time_interval)
        self.client=None

        
    def connect_source(self):
        app_token = 'Cv7NqKwuRPR6O2lwRdNWR4JmH'

        try:
            self.client = Socrata("data.melbourne.vic.gov.au", app_token)
        except Exception as ex:
            print('Exception while connecting to record source.')
            print(str(ex)) 
         
        finally:
            return self.client

    # to do: 
    # 1. aybe need to use paging to retrieve the next set of records?  
    # 2. how to increase limit? 
    # 3. what's the max number of record? 
    def retrieve_record(self):
    
        try:
            self.api_results = self.client.get('vh2v-4nfs', limit=10, content_type='json')
            
            if len(self.api_results) > 0:
                print ('-------------------------------------------')
                print ('Retrieved record successfully. \nTotal records:{} \n'.format(len(self.api_results)))
                # print ('Sample: ', self.api_results[0])
            sleep(self.time_interval)
            return self.api_results
    
    
        except Exception as ex:
            print('Exception while connecting to record source.')
            print(str(ex)) 
            sleep(self.time_interval)


    # function to write to pubsub 
    def to_pubsub(self):
        service_account_info = json.load(open("service_account_info.json"))
        audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"
        # credentials = jwt.Credentials.from_service_account_info(service_account_info, audience=audience)

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(self.project_id, self.pubsub_topic)
        data = self.api_results
        
        for r in data:
            print (json.dumps(r))
      
            try: 
                publisher.publish(topic_path, json.dumps(r).encode('utf-8'))

#                                   record=json.dumps({'bay_id': r['bay_id'],
#                                                    'st_marker_id': r['st_marker_id'],
#                                                     'status': r['status'],
#                                                     'latitude': r['location']['latitude'],
#                                                     'longitude': r['location']['longitude'],
#                                                     'address': r['location']['human_address']['address'],
#                                                     'city': r['location']['human_address']['city'],
#                                                     'state': r['location']['human_address']['state'],
#                                                     'zip': r['location']['human_address']['zip']
#                                                 }).encode('utf-8'))
                print('Publish successfully.')

            except Exception as ex:
                print('Exception while connecting to record source.')
                print(str(ex)) 

    # improvement: make it run for X seconds rather than using while loop

    def main(self):
        n = 1
        self.connect_source()
        while n < 10:
            self.retrieve_record() # specific the interval 
            self.to_pubsub()
            n += 1
            
if __name__ == '__main__':
    arg=get_arg()
    p = ParkingListener(arg.pubsub_topic,
                        arg.api_topic,
                        arg.api_record_limit,
                        arg.api_record_format,
                        arg.project_id,
                        arg.time_interval)


    p.main()

