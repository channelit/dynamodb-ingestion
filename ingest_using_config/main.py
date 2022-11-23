import os

from boto3 import client
import botocore
import yaml

dynamodb = client('dynamodb')
config_file = '/Users/hardikpatel/workbench/projects/ingestion/config.yml'
csv_file = 'user.csv'
tableName = "table"


def get_config(filename):
    stream = open(filename, "r")
    docs = yaml.load_all(stream, yaml.FullLoader)
    return docs


def main():
    print(config_file)
    docs = get_config(config_file)
    for doc in docs:
        response = import_to_dynamo(doc)
        print(response)
        if 'ImportTableDescription' in response:
            print('Arn: {}'.format(response['ImportTableDescription']['ImportArn']))
        else:
            print('Error: {}'.format(response['Message']))
    return print("Done")


def import_to_dynamo(doc):
    response = ""
    try:
        response = dynamodb.import_table(
            S3BucketSource={
                'S3Bucket': doc['s3Bucket'],
                'S3KeyPrefix': doc['s3KeyPrefix']
            },
            InputFormat='CSV',
            InputFormatOptions={
                'Csv': {
                    'Delimiter': ',',
                }
            },
            InputCompressionType='NONE',
            TableCreationParameters={
                'TableName': doc["tableName"],
                'AttributeDefinitions': [
                    {
                        'AttributeName': doc['keyField'],
                        'AttributeType': 'S'
                    },
                ],
                'KeySchema': [
                    {
                        'AttributeName': doc['keyField'],
                        'KeyType': 'HASH'
                    },
                ],
                'BillingMode': 'PAY_PER_REQUEST'
            }
        )
    except botocore.exceptions.ClientError as error:
        response = error.response['Error']
    except botocore.exceptions.ParamValidationError as error:
        raise ValueError('The parameters you provided are incorrect: {}'.format(error))
    return response


if __name__ == '__main__':
    main()
