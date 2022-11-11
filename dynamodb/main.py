import boto3
import yaml

dynamodb = boto3.client('dynamodb')
config_file = 'config.yml'
csv_file = 'user.csv'
tableName = "table"


def get_config(filename):
    stream = open(filename, "r")
    docs = yaml.load_all(stream, yaml.FullLoader)
    return docs


def main():
    docs = get_config(config_file)
    for doc in docs:
        import_to_dynamo(doc)
    return print("Done")


def import_to_dynamo(doc):
    response = dynamodb.import_table(
        S3BucketSource={
            'S3Bucket': 'clamav-cits',
            'S3KeyPrefix': 'users.csv'
        },
        InputFormat='CSV',
        InputFormatOptions={
            'Csv': {
                'Delimiter': ',',
            }
        },
        InputCompressionType='NONE',
        TableCreationParameters={
            'TableName': 'test',
            'AttributeDefinitions': [
                {
                    'AttributeName': 'USER_ID',
                    'AttributeType': 'S'
                },
            ],
            'KeySchema': [
                {
                    'AttributeName': 'USER_ID',
                    'KeyType': 'HASH'
                },
            ],
            'BillingMode': 'PAY_PER_REQUEST'
        }
    )


if __name__ == '__main__':
    main()
