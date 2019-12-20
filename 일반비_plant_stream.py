import csv
import json
import boto3
import gzip
import time
import base64
import io
import re
import os
import sys

from io import BytesIO
from io import StringIO
import multiprocessing

from s3_utility import *
from importlib import reload ## (python3)*

def stream_run(plant_num):
   power_plant_num = plant_num
   print('power_plant_num = ' + power_plant_num)

   power_plant_prefix = []
   power_plant_objects = []

   #1월부터8월까지 리스트
   for i in range(1, 9):
      power_plant_prefix.append('clean-csv-gzip/power_plant_id='+str(power_plant_num)+'/year=2019/month='+ str(i))
	
   print("power_plant_prefix = " + power_plant_prefix[0])
   
   suffix = 'gz'
   bucket = 'ddi-poc-data-origin'
   s3 = boto3.client('s3', region_name='us-east-1')
   
   #plant별 s3에 저장된 list
   for i in range(len(power_plant_prefix)):
      for obj in get_matching_s3_keys(s3,bucket,power_plant_prefix[i],suffix):
         power_plant_objects.append(obj)
	        
   #s3에 저장된 9753, 9754 리스트 목록??
   #power_plat_object_list = [power_plant_9753_objects, power_plant_9754_objects]
   
   kinesis = boto3.client('kinesis', region_name='us-east-1')
   bigdataStreamName = "fuelcell_data_stream" ## kinesis stream
	
   s3_res = boto3.resource('s3')
	
   partition_key = 'power_plant_' + str(power_plant_num)
   print("partition_key" + partition_key)
   print("power_plant_objects = "+ str(len(power_plant_objects)))
	
   for key in power_plant_objects:
      print(" key = "+key)
      obj = s3_res.Object(bucket,key)
      bytestream = BytesIO(obj.get()['Body'].read())
      got_text = gzip.GzipFile(mode='rb', fileobj=bytestream).read().decode('utf-8')
      content = got_text.splitlines()
      #print("content =" + content)
	
      #print('key = ' + key)
      reader = csv.DictReader(content)
      #json포맷 kinesis전송
      for row in reader:
         data = json.dumps(row)
         kinesis.put_record(StreamName=bigdataStreamName, Data=data, PartitionKey=partition_key)
         time.sleep(1)
        
plant_list = ['9753', '9754', '9755', '9756', '9757', '9758', '9759', '9825', '9826']

if __name__ == '__main__':
   pool = multiprocessing.Pool(processes=9)
   pool.map(stream_run, plant_list)
   pool.close()
   pool.join()

