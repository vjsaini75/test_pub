from time import sleep
from json import dumps
from json import loads
from kafka import KafkaProducer
import csv
import time
import logging
import os
#import avro

#logging.basicConfig(level=logging.DEBUG)

producer = KafkaProducer(bootstrap_servers=['192.168.129.123:9093'],
                         value_serializer=lambda x: 
                         x.encode('utf-8'))

#dir= os.getcwd()
#print(dir)
#print("Connected")
old_stamp = 'init'
count = 0
file_SOLAR = '/data/SOLAR_ANALOG.IMP'
while count < 10:
    new_stamp = time.ctime(os.path.getmtime(file_SOLAR))
    if ( new_stamp != old_stamp):
        line_count = 0
        ifile  = open(file_SOLAR, "r")
        csv_reader = csv.reader(ifile)
        old_stamp = new_stamp
        for row in csv_reader:
            ID = 'ID = ' + row[0]
            data = 'Data = ' + row[1]
            quality = 'Quality = ' + row[2]
            full_record = ID + ' ' + data +' ' + quality
            data_dict = {'ID': row[0],'data': float(row[1]),'QUALITY': int(row[2])}
            #print(data_dict)
            producer.send('vku-test', value=dumps(data_dict))
            line_count += 1
        count = count +1  
        #print (line_count)
    ifile.close()
    time.sleep(3)
print(count)
    



