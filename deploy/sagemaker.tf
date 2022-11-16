resource "aws_sagemaker_model" "model_sagemaker_2" {
  name = "cits-sagemaker-2"
  execution_role_arn = data.aws_iam_role.role_sagemaker_2.arn
  # vpc_config = data.aws_vpc.vpc_sagemaker_2.arn

  primary_container {
    image = "122936777114.dkr.ecr.us-east-1.amazonaws.com/cits_byo:latest"
    model_data_url = "s3://cits-byotf/model/model-original.tar.gz"
  }

  tags = {
    model = "cits_sagemaker_2"
  }
}
data "aws_iam_role" "role_sagemaker_2" {
  name = "cits_sagemaker"
}

data "aws_vpc" "vpc_sagemaker_2" {
  id = "vpc-05382184f7192f833"
}


resource "aws_sagemaker_endpoint_configuration" "endpoing_config_sagemaker_2" {
  name = "endpoint-config-cits-sagemaker-2"

  production_variants {
    variant_name = "variant-1"
    model_name = aws_sagemaker_model.model_sagemaker_2.name
    initial_instance_count  = 1
    instance_type = "ml.t2.medium"
    initial_variant_weight  = 1
  }

  tags = {
    model = "cits_sagemaker_2"
  }
}

resource "aws_sagemaker_endpoint" "endpoint_sagemaker_2" {
  name = "endpoint-cits-sagemaker-2"
  endpoint_config_name = aws_sagemaker_endpoint_configuration.endpoing_config_sagemaker_2.name

  tags = {
    model = "cits_sagemaker_2"
  }
}
