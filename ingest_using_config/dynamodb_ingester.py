from boto3 import client
import botocore
from botocore.exceptions import WaiterError


class IngesterService:

    def __init__(self, client_token: str):
        self.client = client('dynamodb')
        self._client_token = client_token

    def import_to_dynamo(self, s3_bucket: str, s3_key_prefix: str, table_name: str, key_field: str):
        response = ""
        try:
            response = self.client.import_table(
                # ClientToken=self._client_token,
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
        except botocore.exceptions.ClientError as error:
            response = error.response['Error']
        except botocore.exceptions.ParamValidationError as error:
            raise ValueError('The parameters you provided are incorrect: {}'.format(error))
        print(response)
        if 'ImportTableDescription' in response:
            import_arn = response['ImportTableDescription']['ImportArn']
            print('Arn: {}'.format(import_arn))
            print("Checking status")
            import_status = self.dynamo_import_status(import_arn)
            print("Import Status : {}".format(import_status))
            self.wait_for_import(table_name=table_name)
        else:
            print('Error: {}'.format(response['Message']))
        return response

    def dynamo_import_status(self, import_arn: str):
        response = self.client.describe_import(ImportArn=import_arn)
        status = response['ImportTableDescription']['ImportStatus']
        return status

    def wait_for_import(self, table_name: str):
        delay = 5
        max_attempts = 100
        waiter = self.client.get_waiter('table_exists')
        try:
            w = waiter.wait(
                TableName=table_name,
                WaiterConfig={
                    'Delay': 5,
                    'MaxAttempts': 100
                }
            )
            print("Table created")
        except WaiterError as e:
            if "Max attempts exceeded" in e.message:
                print("Table not created after {} seconds.".format(delay * max_attempts))
            else:
                print(e.message)

    def wait_for_delete(self, table_name: str):
        delay = 5
        max_attempts = 100
        waiter = self.client.get_waiter('table_not_exists')
        try:
            waiter.wait(
                TableName=table_name,
                WaiterConfig={
                    'Delay': delay,
                    'MaxAttempts': max_attempts
                }
            )
            print("Table deleted")
        except WaiterError as e:
            if "Max attempts exceeded" in e.message:
                print("Table not deleted after {} seconds.".format(delay * max_attempts))
            else:
                print(e.message)


if __name__ == '__main__':
    pass
