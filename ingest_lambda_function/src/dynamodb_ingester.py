from boto3 import client
import botocore
from botocore.exceptions import WaiterError


class IngesterService:

    def __init__(self, table_name: str):
        self.client = client('dynamodb')
        self._client_token = table_name
        self._table_name = table_name

    def import_to_dynamo(self, s3_bucket: str, s3_key_prefix: str, key_field: str, delete_if_exists: bool):
        response = ""
        if delete_if_exists:
            self.delete_dynamo_table_if_exists(table_name=self._table_name)
        try:
            response = self.client.import_table(
                ClientToken=self._client_token,
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
                    'TableName': self._table_name,
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
            print("Checking status of table {}".format(self._table_name))
            import_status = self.dynamo_import_status(import_arn)
            print("Import Status : {}".format(import_status))
            print("Waiting creation of table {}".format(self._table_name))
            self.wait_for_import(table_name=self._table_name)
            import_status = self.dynamo_import_status(import_arn)
            print("Import Status : {}".format(import_status))
        else:
            print('Error: {}'.format(response['Message']))
        return response

    def dynamo_import_status(self, import_arn: str):
        response = self.client.describe_import(ImportArn=import_arn)
        print(response)
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
            print("Deleted table {}".format(table_name))
        except WaiterError as e:
            if "Max attempts exceeded" in e.message:
                print("Table not deleted after {} seconds.".format(delay * max_attempts))
            else:
                print(e.message)

    def delete_dynamo_table_if_exists(self, table_name: str):
        response = ""
        try:
            response = self.client.delete_table(
                TableName=table_name
            )
        except botocore.exceptions.ClientError as error:
            response = error.response['Error']
        except botocore.exceptions.ParamValidationError as error:
            raise ValueError('The parameters you provided are incorrect: {}'.format(error))
        print(response)
        if 'TableDescription' in response:
            delete_status = response['TableDescription']['TableStatus']
            print("Delete Status : {}".format(delete_status))
            print("Waiting to delete table {}".format(table_name))
            self.wait_for_delete(table_name=table_name)
        else:
            print('Error: {}'.format(response['Message']))
        return response


if __name__ == '__main__':
    pass
