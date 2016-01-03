from __future__ import print_function
import boto3
import json

print('Loading temperature from dynamo')

sqs = boto3.resource('sqs')
queue = sqs.Queue('https://sqs.us-east-1.amazonaws.com/238322712540/rpi-test')


def lambda_handler(event, context):
    inserts = get_inserts(event['Records'])
    sqs_items = map(convert_to_sqs, inserts)
    for i in sqs_items:
        send_message(i)


def get_inserts(data):
    inserts = [i for i in data if is_insert(i)]
    return inserts


def convert_to_sqs(data):
    item = data['dynamodb']['NewImage']
    return {
        'time': get_value(item['time']),
        'device': get_value(item['device']),
        'temperature': get_value(item['temperature'])}


def get_value(item):
    if 'S' in item:
        return item['S']
    if 'N' in item:
        return item['N']


def send_message(message):
    queue.send_message(MessageBody=json.dumps(message))


def is_insert(i):
    return i.get('eventName') == 'INSERT'
