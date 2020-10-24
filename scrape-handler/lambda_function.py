import json
import boto3
import os
import logging

from boto3.dynamodb.conditions import Key

from connections import build_client_dynamo, build_client_stepfunctions

PKEY = os.getenv('PKEY')
TABLE_NAME = os.getenv('TABLE_NAME')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_targets(pkey, table_name):
    table = build_client_dynamo(table_name=table_name)
    targets = table.query(
        KeyConditionExpression = Key("pkey").eq(pkey)
    )
    return targets['Items']


def lambda_handler(event, context):
    targets = get_targets(PKEY, TABLE_NAME)
    stepfunctions = build_client_stepfunctions()
    for target in targets:
        if target["active"] == 1:
            logger.info(f"execute {target['trigger']}")
            stepfunctions.start_execution(
                **{
                    'input': '{"Comment": "Periodic execution"}',
                    'stateMachineArn': target['trigger']
                }
            )
    return {
        'statusCode': 200,
    }
