from dynamodb_ingester import IngesterService


def handler(event, context):
    print(event)
    s3_bucket: str = event.get("s3Bucket") or "none"
    s3_key_prefix: str = event.get("s3KeyPrefix") or "none"
    table_name: str = event.get("tableName") or "none"
    key_field: str = event.get("keyField") or "none"
    delete_if_exists: bool = event.get("deleteIfExists") or False
    response = IngesterService(table_name=table_name).import_to_dynamo(s3_bucket=s3_bucket, s3_key_prefix=s3_key_prefix,
                                                                       key_field=key_field,
                                                                       delete_if_exists=delete_if_exists)
    print(response)
    return response
