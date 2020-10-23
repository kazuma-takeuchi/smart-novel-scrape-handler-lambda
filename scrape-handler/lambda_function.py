import json
import boto3
import os

from boto3.dynamodb.conditions import Key

PKEY = os.getenv('PKEY')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("control")


def get_candidates(pkey):
    queryData = table.query(
        KeyConditionExpression = Key("pkey").eq(pkey)
    )
    items=queryData['Items']
    return items

def lambda_handler(event, context):
    candidates = get_candidates(PKEY)
    targets = []
    for cand in candidates:
        if cand['active'] == 1:
            targets.append(cand)
    print(targets)
    # TODO implement
    return {
        'statusCode': 200,
        'targets': targets
    }
