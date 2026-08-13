terraform {
  required_version = ">= 1.6.0"
  required_providers {
    # aws travado abaixo de 5.67: a partir dela o provider valida a definicao de
    # Step Functions via ValidateStateMachineDefinition, API que o LocalStack
    # community nao implementa -- o apply quebra com 501 mesmo com a definicao certa.
    aws     = { source = "hashicorp/aws", version = ">= 5.60.0, < 5.67.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true
  s3_use_path_style           = true

  # FinOps comeca em tag: com todo recurso carimbado na origem, o Cost
  # Explorer quebra a fatura por projeto/centro de custo sem depender de
  # disciplina manual. Chaves distintas das tags por recurso (Project,
  # Layer) de proposito, para somar em vez de conflitar.
  default_tags {
    tags = {
      ManagedBy  = "terraform"
      CostCenter = "dados-pagamentos"
    }
  }

  endpoints {
    s3             = "http://localhost:4566"
    iam            = "http://localhost:4566"
    sts            = "http://localhost:4566"
    dynamodb       = "http://localhost:4566"
    lambda         = "http://localhost:4566"
    stepfunctions  = "http://localhost:4566"
    sns            = "http://localhost:4566"
    sqs            = "http://localhost:4566"
    cloudwatchlogs = "http://localhost:4566"
    cloudwatch     = "http://localhost:4566"
    events         = "http://localhost:4566"
    kinesis        = "http://localhost:4566"
    firehose       = "http://localhost:4566"
    glue           = "http://localhost:4566"
  }
}
