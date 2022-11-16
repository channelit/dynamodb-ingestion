import json
from dynamodb_ingester import Ingester
import os


def handler(event, context):
    s3_bucket: str = event.get("s3Bucket") or "none"
    s3_key_prefix: str = event.get("s3KeyPrefix") or "none"
    table_name: str = event.get("tableName") or "none"
    key_field: str = event.get("keyField") or "none"
    if s3_bucket != "none" and s3_key_prefix != 'none' and table_name != 'none' and key_field != 'none':
        ingester = Ingester()
        return ingester.import_to_dynamo(s3_bucket=s3_bucket, s3_key_prefix=s3_key_prefix, table_name=table_name,
                                         key_field=key_field)
