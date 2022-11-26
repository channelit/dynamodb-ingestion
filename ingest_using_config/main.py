from boto3 import client
import yaml
from dynamodb_ingester import IngesterService

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
        s3_bucket: str = doc['s3Bucket'] or "none"
        s3_key_prefix: str = doc['s3KeyPrefix'] or "none"
        table_name: str = doc["tableName"] or "none"
        key_field: str = doc['keyField'] or "none"
        response = IngesterService(table_name=table_name).import_to_dynamo(s3_bucket=s3_bucket,
                                                                           s3_key_prefix=s3_key_prefix,
                                                                           key_field=key_field, delete_if_exists=True)
        print(response)
        if 'ImportTableDescription' in response:
            print('Arn: {}'.format(response['ImportTableDescription']['ImportArn']))
        else:
            print('Error: {}'.format(response['Message']))
    return print("Done")


if __name__ == '__main__':
    main()
