import boto3
import json
import logging
import requests
from variables import *

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from requests_aws4auth import AWS4Auth 

def lambda_handler(event, context):

    url = 'https://search-post1-faq4b5et2ggsffxvhd2fnhkhfq.us-east-1.es.amazonaws.com' + '/posts' + '/_search'
    
    numOfPosts = 3
    
    query = {
        "size": numOfPosts,
        "query": {
            "multi_match": {
                "query": event['q']
            }
        }
    }

    jsonQuery = json.dumps(query)

    header = { "Content-Type": "application/json" }
    response = requests.get(url, auth=(USER,PASS), headers=header, data=jsonQuery)
    
    response = response.json()
    
    ids = []
    for matches in response['hits']["hits"]:
        ids.append(matches["_id"])
   
    print("ids: ",ids)
    
    # Read posts from DynamoDB via ids received from ElasticSearch
    dynamodb = boto3.client('dynamodb')
    
    posts = { 'item': []}
    
    for id in ids:
        response = dynamodb.get_item(TableName="posts",
            Key={
                "id" :{
                    "N":id
                }
            }
        )
        
        posts['item'].append(response['Item']['posts'])
    
    length = len(posts['item'])
    
    if (length == 0):
        posts = 'no answers found for this category'
        
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(posts,indent=1)
    }