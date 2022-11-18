#### Run lambda container locally
```shell
pip freeze > requirements.txt
docker buildx build --platform=linux/arm64 -t lambda-dynamodb-ingest .
docker build --platform=linux/amd64 -t lambda-dynamodb-ingest .
docker run -p 9000:8080  lambda-dynamodb-ingest:latest --name=lambda-dynamodb-ingest
docker run -p 9000:8080 -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION=$AWS_REGION lambda-dynamodb-ingest:latest
```

##### Push to AWS
```shell
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 122936777114.dkr.ecr.us-east-1.amazonaws.com
docker tag lambda-dynamodb-ingest:latest 122936777114.dkr.ecr.us-east-1.amazonaws.com/lambda-dynamodb-ingest:latest
docker push 122936777114.dkr.ecr.us-east-1.amazonaws.com/lambda-dynamodb-ingest:latest
```