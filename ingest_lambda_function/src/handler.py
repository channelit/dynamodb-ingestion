from dynamodb_ingester import IngesterService
import boto3


def handler(event, context):
    print(event)
    s3_bucket: str = event.get("s3Bucket") or "none"
    s3_key_prefix: str = event.get("s3KeyPrefix") or "none"
    table_name: str = event.get("tableName") or "none"
    key_field: str = event.get("keyField") or "none"
    client = boto3.client('dynamodb')
    response = IngesterService().import_to_dynamo(client=client, s3_bucket=s3_bucket, s3_key_prefix=s3_key_prefix,
                                                  table_name=table_name,
                                                  key_field=key_field)
    print(response)
    return response
