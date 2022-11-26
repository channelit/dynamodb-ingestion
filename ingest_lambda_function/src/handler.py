from dynamodb_ingester import IngesterService
import boto3


def handler(event, context):
    print(event)
    s3_bucket: str = event.get("s3Bucket") or "none"
    s3_key_prefix: str = event.get("s3KeyPrefix") or "none"
    table_name: str = event.get("tableName") or "none"
    key_field: str = event.get("keyField") or "none"
    response = IngesterService(table_name=table_name).import_to_dynamo(s3_bucket=s3_bucket, s3_key_prefix=s3_key_prefix,
                                                                       key_field=key_field, delete_if_exists=True)
    print(response)
    return response
