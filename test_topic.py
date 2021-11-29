from kafka import KafkaConsumer
from json import loads
import csv
from time import sleep
import os
#import pandas

consumer = KafkaConsumer(
    'vku-test',
     bootstrap_servers=['192.168.129.123:9093'],
     auto_offset_reset='earliest',
     group_id = 'vijay reading',
     value_deserializer=lambda x: x.decode('utf-8'))

#consumer_latest = KafkaConsumer(
#    'vku-test',
#     bootstrap_servers=['192.168.129.123:9092'],
#     auto_offset_reset='latest',
#     group_id = 'vijay reading 1',
#     value_deserializer=lambda x: loads(x.decode('utf-8')))


for message in consumer:   
    message = message.value
    print(message)
    consumer.commit()
