import boto3
import json
import logging
import requests
from variables import *

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from requests_aws4auth import AWS4Auth 

def getPosts(key):
    url = 'https://search-post1-faq4b5et2ggsffxvhd2fnhkhfq.us-east-1.es.amazonaws.com' + '/posts' + '/_search'
    
    numOfPosts = 3
    
    query = {
        "size": numOfPosts,
        "query": {
            "multi_match": {
                "query": key
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
        
    return posts
        
def sendEmail(posts):
    logging.basicConfig(format="[%(levelname)s] [%(name)s] [%(asctime)s]: %(message)s",
    level="INFO")
    logger = logging.getLogger(__name__)
    
    sns = boto3.client('sns', region_name='us-east-1')
    
    sns.publish(TopicArn='arn:aws:sns:us-east-1:031356061063:cloudPosts', 
    Message=json.dumps(posts))

def lambda_handler(event, context):
    
    #print(event["currentIntent"]["slots"]["cloudKeywordType"])
    
    # Get Lex Client
    client = boto3.client('lex-runtime')
    
    # Get user query
    #query = event["currentIntent"]["slots"]["cloudKeywordType"]
    print(event)
    query = event['q']
    
    # Post user query to Lex & get response
    response_lex = client.post_text(
        botName='CloudBot',
        botAlias="botTwo",
        userId="vinu4794",
        inputText=query
    )
    
    # Print Lex Response
    print(response_lex)
    
    if 'slots' in response_lex:
        keys = [response_lex['slots']['slotOne'],response_lex['slots']['slotTwo']]
        print(keys)
        
    finalPosts = []
    for key in keys: 
        if key is not None:
            finalPosts.append(getPosts(key))
        
    posts = finalPosts
    
    sendEmail(posts);
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps("Success!",indent=1)
    }