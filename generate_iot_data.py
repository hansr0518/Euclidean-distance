import json
import datetime
import random
import boto3

kinesis = boto3.client('kinesis', region_name='us-east-1')

def getData(sensorType, sensorName, lowVal, highVal):
    data = {}
    data['sensorType'] = sensorType
    data['sensorName'] = sensorName
    data['sensorValue'] = random.randint(lowVal, highVal)
    now = datetime.datetime.now()
    str_now = now.isoformat()
    data['sensorTimestamp'] = str_now
    return data
    
def lambda_handler(event, context):

    bigdataStreamName = "ddi_kjsng_stream" ## change to your kinesis stream 
    count = 1
    for _ in range(0, 500000):
        rnd = random.random()
        if (rnd < 0.01):
            if count==1:
                data = json.dumps(getData(count, 'ShaftSpeedSensor', 18000, 20000))
                kinesis.put_record(StreamName=bigdataStreamName, Data=data, PartitionKey="ShaftSpeedSensor")
               
            elif count==2:
                data = json.dumps(getData(count, 'ChamberPressureSensor', 4200, 5000))  
                kinesis.put_record(StreamName=bigdataStreamName, Data=data, PartitionKey="ChamberPressureSensor")
                
            elif count==3:
                data = json.dumps(getData(count, 'DischargeTempSensor', 3500, 3800))  
                kinesis.put_record(StreamName=bigdataStreamName, Data=data, PartitionKey="DischargeTempSensor")
              
            elif count==4:
                data = json.dumps(getData(count, 'FuelOxidizerMixRatioSensor', 5, 10))  
                kinesis.put_record(StreamName=bigdataStreamName, Data=data, PartitionKey="FuelOxidizerMixRatioSensor")
       
        else:
            if count==1:
                #13,800 rpm avg
                data = json.dumps(getData(count, 'ShaftSpeedSensor', 13000, 14000))  
                kinesis.put_record(StreamName=bigdataStreamName, Data=data, PartitionKey="ShaftSpeedSensor")
               
            elif count==2:
                #3,722 psi avg
                data = json.dumps(getData(count, 'ChamberPressureSensor', 3500, 3800))  
                kinesis.put_record(StreamName=bigdataStreamName, Data=data, PartitionKey="ChamberPressureSensor")
               
            elif count==3:
                #3,127c avg
                data = json.dumps(getData(count, 'DischargeTempSensor', 3000, 3200))  
                kinesis.put_record(StreamName=bigdataStreamName, Data=data, PartitionKey="DischargeTempSensor")
                
            elif count==4:
                #3 avg
                data = json.dumps(getData(count, 'FuelOxidizerMixRatioSensor', 2, 4))  
                kinesis.put_record(StreamName=bigdataStreamName, Data=data, PartitionKey="FuelOxidizerMixRatioSensor")
        
        # print(data)
        count = 1 if count>= 4 else count+1
    return "complete"