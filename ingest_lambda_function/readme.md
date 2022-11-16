#### Run lambda container locally
```shell
pip freeze > requirements.txt
docker build --platform=linux/amd64 -t lambda-dynamodb-ingest .

```

##### Push to AWS
```shell
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 122936777114.dkr.ecr.us-east-1.amazonaws.com
docker tag lambda-dynamodb-ingest:latest 122936777114.dkr.ecr.us-east-1.amazonaws.com/lambda-dynamodb-ingest:latest
docker push 122936777114.dkr.ecr.us-east-1.amazonaws.com/lambda-dynamodb-ingest:latest
```