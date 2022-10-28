import argparse
import csv
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


API_KEY = 'UDQ7NE6Q2RP7QTA2'
ENDPOINT_SCHEMA_URL  = 'https://psrc-mw731.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = '13yluUNlbrWp0i38wjqXzBBSRCwFvq1cejsrb3PzAse/yTC1xbk08jIwlzII7YKY'
BOOTSTRAP_SERVER = 'pkc-lzvrd.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'OOA5LMECANNMKTQL'
SCHEMA_REGISTRY_API_SECRET = '0B6Gw/a1nN0hJGRuY+HnICU43Sebc+2SHQz6ZD3CXGy/mF6dnQ98Zg5ah7xcs5T0'

FILE_PATH = "C:/Users/Admin/Desktop/kafka_assignment/output.csv"


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Restaurant:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_restaurant(data:dict,ctx):
        return Restaurant(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    subjects = schema_registry_client.get_subjects()
    schema = schema_registry_client.get_latest_version(subject_name='restaurent-take-away-data-value')
    ver=schema.version
    sch_id=schema.schema_id
    sch_str=schema.schema.schema_str
    json_deserializer = JSONDeserializer(sch_str,
                                         from_dict=Restaurant.dict_to_restaurant)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    count=0
    f = open(FILE_PATH, 'w',newline='', encoding='utf-8')
    writer = csv.writer(f)
    a=[]
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            restaurant = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if restaurant is not None:
                print("User record {}: restaurant: {}\n"
                      .format(msg.key(), restaurant))
                #print("Values",restaurant.record.values())
                x=map(lambda x: x, restaurant.record.values())
                x1=list(x)
                a.append(x1)
                #print(a)
                #writer.writerows(x1)
                #print(x1)
                count=count+1
                print(count)
        except KeyboardInterrupt:
            break
    writer.writerows(a)
    f.close()
    consumer.close()

main("restaurent-take-away-data")