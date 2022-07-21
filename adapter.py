# Adapter
# Reference: https://aws.amazon.com/blogs/compute/developing-evolutionary-architecture-with-aws-lambda/#:~:text=Hexagonal%20architecture%20with%20Lambda%20functions&text=In%20Lambda%20functions%2C%20hexagonal%20architecture,domain%20logic%20from%20the%20infrastructure.
# https://medium.com/@domagojk/serverless-event-sourcing-in-aws-lambda-dynamodb-sqs-7237d79aed27

import json
import logging
import os
import time
import uuid
from functions import createRessource,createOrUpdateRessource,deleteResource,getResources,getResource, resourceCreatedEventHandler, resourceDeletedEventHandler, resourceUpdatedEventHandler
import boto3
import base64
#enverenment variables
#https://docs.aws.amazon.com/lambda/latest/dg/current-supported-versions.html
import os
table_name=os.environ['TABLE NAME']

def commandHandler(event, context):
    # Add code here
    httpMethod = event['httpMethod']
    path = event['path']
    params=event['queryStringParameters']
    kenesis=boto3.client('kenesis')

    if httpMethod=='POST' :
        return createRessource(params,kenesis)
    
    if httpMethod=='PUT':
        return createOrUpdateRessource(params,kenesis,patch=False)

    if httpMethod=='PATCH':
        return createOrUpdateRessource(params,kenesis,patch=True)
    
    if httpMethod=='DELETE':
        return deleteResource(params,kenesis)


    message = "lambda HTTP adapter for command handler "

    print(message)
    print(event)

    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }



def queryHandler(event, context):
    # Add code here
    httpMethod = event['httpMethod']
    path = event['path']
    dynamodb=boto3.resource('dynamodb')
    params=event['queryStringParameters']
    table=dynamodb.Table(table_name)

    if httpMethod=='GET' :
        if params['identifier']:
            result = getResource(params['identifier'],table)
            return result

        else:
            result = getResources(table)
            return result


    message = "lambda HTTP adapter for query handler "

    print(message)
    print(event)

    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }

def eventHandler(event, context):
    # Add code here
    httpMethod = event['httpMethod']
    path = event['path']
    dynamodb=boto3.resource('dynamodb')
    table=dynamodb.Table(table_name)
    #consume kenesis stream
    for record in event['Records']:
        data=json.load(
            base64.b64decode(record['kinesis']['data']).decode('utf-8')
        )
        if data['eventType']=='resourceCreatedEvent':

            result = resourceCreatedEventHandler(data['data'],table)
            return result
        if data['eventType']=='resourceUpdatedEvent':
            result = resourceUpdatedEventHandler(data['data'],table)
            return result
        if data['eventType']=='resourceDeletedEvent':
            result = resourceDeletedEventHandler(data['data'],table)
            return result
    

    message = "lambda HTTP adapter for event handler "

    print(message)
    print(event)

    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }





