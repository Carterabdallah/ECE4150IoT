import pandas as pd
import json
import matplotlib.pyplot as plt
from env import AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY, AWS_REGION
import boto3
from dynamodb_json import json_util as json
from boto3.dynamodb.conditions import Key, Attr
import time


dynamodb = boto3.resource('dynamodb', aws_access_key_id=AWS_ACCESS_KEY,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                region_name=AWS_REGION)

table = dynamodb.Table('AirQualityData')


while True: 
    now=int(time.time())
    timestampold=now-86400

    response = table.scan(
        FilterExpression=Attr('timestamp').gt(timestampold)
    )

    items = response['Items']

    df = pd.DataFrame(items)
    df = df.merge(pd.json_normalize(df.data))
    df.drop(columns=['data'], inplace=True)
    df['co']=df['co'].astype(float)
    dfSt5 = df[df.stationID == 'ST105']
    dfSt2 = df[df.stationID == 'ST102']
    dfSt5.plot(x ='timestamp', y='co', kind = 'line')
    plt.xlabel('TimeStamp adjusted', fontsize='17', horizontalalignment='center')
    plt.ylabel('Co2', fontsize='17', horizontalalignment='center')
    plt.suptitle('Station 105 Co2 at Time' + str(time.time()), fontsize=20)
    plt.draw()
    plt.pause(0.2)
    plt.close('all')


