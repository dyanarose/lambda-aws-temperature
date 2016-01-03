from __future__ import print_function

import boto3

print('Loading function')

tableName = 'HouseTempLogging'


def lambda_handler(event, context):
    '''Provide an event that contains the following keys:

      - time: the time the reading was taken
      - temperature: the temperature taken
      - device: the device used
    '''
    # print("Received event: " + json.dumps(event, indent=2))

    err = data_error(event)
    if err:
        raise ValueError('malformed data: ' + err)

    dynamo = boto3.resource('dynamodb').Table(tableName)
    result = dynamo.put_item(TableName=tableName, Item=event)
    return result


def data_error(data):
    if not data:
        return 'data is empty'
    if 'time' not in data or not data['time']:
        return 'time is missing or empty'
    if 'device'not in data or not data['device'] or data['device'].isspace():
        return 'device is missing or empty'
    if 'temperature' not in data or not data['temperature']:
        return 'temperature is missing or empty'

    return False
