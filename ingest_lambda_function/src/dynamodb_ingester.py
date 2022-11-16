import boto3

dynamodb = boto3.client('dynamodb')


class Ingester:

    def __int__(self):
        self.dynamodb = boto3.client('dynamodb')

    def import_to_dynamo(self, s3_bucket: str, s3_key_prefix: str, table_name: str, key_field: str):
        response = self.dynamodb.import_table(
            S3BucketSource={
                'S3Bucket': s3_bucket,
                'S3KeyPrefix': s3_key_prefix
            },
            InputFormat='CSV',
            InputFormatOptions={
                'Csv': {
                    'Delimiter': ',',
                }
            },
            InputCompressionType='NONE',
            TableCreationParameters={
                'TableName': table_name,
                'AttributeDefinitions': [
                    {
                        'AttributeName': key_field,
                        'AttributeType': 'S'
                    },
                ],
                'KeySchema': [
                    {
                        'AttributeName': key_field,
                        'KeyType': 'HASH'
                    },
                ],
                'BillingMode': 'PAY_PER_REQUEST'
            }
        )


if __name__ == '__main__':
    pass
