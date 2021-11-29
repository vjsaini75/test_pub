from kafka import KafkaConsumer
from json import loads
import csv
from time import sleep
import os
#import pandas

consumer = KafkaConsumer(
    'VIJAY_TEST_TABLE2',
     bootstrap_servers=['192.168.129.123:9093'],
     auto_offset_reset='earliest',
     group_id = 'vijay reading vku',
     value_deserializer=lambda x: x.decode('utf-8'))

#consumer_latest = KafkaConsumer(
#    'vku-test',
#     bootstrap_servers=['192.168.129.123:9092'],
#     auto_offset_reset='latest',
#     group_id = 'vijay readingvku',
#     value_deserializer=lambda x: loads(x.decode('utf-8')))
#i=0
#ofile = open('til_scada.csv', "w",newline='' )
#csv_writer = csv.writer(ofile)
#csv_writer.writerow("hi")
#ofile.close()
#count = 0
while True:
    #sleep(1)
    try:  
    #    ofile = open('til_scada.csv', "w",newline='' )
    #    csv_writer = csv.writer(ofile)
    #    #csv_writer.writerow('vijay')
        for message in consumer:
    #        count = count + 1 
            message = message.value
            #print('{} added'.format(message))
            #print(message.split(sep=","))
            #message = message.replace('key = ',"")
            #message = message.replace(' Data = ',",")
            #message = message.replace(' Quality = ',",")
    #        print(message.split(sep=","))
            print(message)
    #       csv_writer.writerow(message.split(sep=","))
    #        if (count >4):
    #            count =0 
    #            ofile.close()
    #            os.rename('til_scada.csv','til_scada1.csv')
    #            ofile = open('til_scada.csv', "w",newline='' )
    #            csv_writer = csv.writer(ofile)
            consumer.commit()
    except IOError: 
        print("data")

       
    
