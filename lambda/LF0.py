import json
import boto3
import pandas as pd
import time
from datetime import datetime
import pytz
import random
nowTime = datetime.now()

def lambda_handler(event, context):
    
    #table name
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('posts')
    str1 = event['q'];
    dict1 = {'posts': str1}
    df = pd.DataFrame(dict1,index=['posts'])

    randomNumber = random.randint(500000, 1500000)
    df['id'] = randomNumber
    
    df['date'] = nowTime.strftime("%d/%m/%Y %H:%M:%S")


    df.dropna(inplace=True)
    df['json'] = df.apply(lambda x: x.to_dict(), axis=1)
    payloads = df['json'].to_list()

    table.put_item(Item=payloads[0])
    print(payloads[0])    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
