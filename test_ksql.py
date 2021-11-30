from kafka import KafkaConsumer
from json import loads
import csv
import logging
from time import sleep
import os
from ksql import KSQLAPI
#import pandas

consumer = KafkaConsumer(
    'vku-test',
     bootstrap_servers=['192.168.129.123:9093'],
     auto_offset_reset='earliest',
     group_id = 'vijay reading',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

consumer_latest = KafkaConsumer(
    'vku-test',
     bootstrap_servers=['192.168.129.123:9093'],
     auto_offset_reset='latest',
     group_id = 'vijay reading 1',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

logging.basicConfig(level=logging.DEBUG)
client = KSQLAPI('http://192.168.129.120:8088')

# to look for topics
#topics = client.ksql('show topics') #working fine
#for item in topics: print(item)

# to look for tables
#test
#table = client.ksql('show tables')
#for item in table: print(item)

#client.create_stream(table_name= 'vijay_test',
#                     columns_type=['ID varchar','DATA double','quality int'],
#                     topic='vku-test',
#                     value_format='JSON')
client.create_table (table_name= 'vijay_test_table1',
                     columns_type=['ID varchar KEY'],
                     topic='vku-test',
                     value_format='JSON', )

#table = client.ksql('show tables')
#for item in table: print(item)

#query1 = client.query("SELECT * from DLR_TEST_TABLE WHERE MRID = '85ead6fb9c97478eacaf0f4f317d0b65'", use_http2=True)
#query1 = client.query("""select * From DLR_TEST Where MRID = '178de1a30fdd4c948f3a3109f75355ed' """, use_http2=True)
#for item1 in query1: print(item1)




