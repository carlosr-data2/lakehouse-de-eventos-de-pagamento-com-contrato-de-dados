# Mapa das camadas do lakehouse. Centralizar aqui evita nome divergente
# entre recursos e mantem coerencia com o codigo Python/Spark.
locals {
  layers      = ["bronze", "silver", "gold", "quarantine", "artifacts"]
  versioned   = ["bronze", "quarantine"]
  glue_dbs    = ["bronze", "silver", "gold"]
  bucket_arns = [for l in local.layers : "arn:aws:s3:::${var.project}-${l}"]
}

# Um bucket por camada do lakehouse. Bucket e a fronteira natural de policy,
# ciclo de vida e apuracao de custo no S3 - por isso nao usamos prefixos.
resource "aws_s3_bucket" "layer" {
  for_each      = toset(local.layers)
  bucket        = "${var.project}-${each.value}"
  force_destroy = true

  tags = {
    Project = var.project
    Layer   = each.value
  }
}

# Versionamento apenas nas camadas nao reconstrutiveis (bronze e quarentena).
# Silver e gold sao derivados: versionar seria pagar storage por algo
# que o pipeline regenera em minutos.
resource "aws_s3_bucket_versioning" "layer" {
  for_each = toset(local.versioned)
  bucket   = aws_s3_bucket.layer[each.value].id
  versioning_configuration {
    status = "Enabled"
  }
}

# Ciclo de vida = FinOps aplicado. Bronze e volumoso e raramente relido,
# entao migra para classes mais baratas; versoes antigas expiram para nao
# acumular custo invisivel. Sob flag: o emulador community nao converge.
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  count  = var.enable_lifecycle ? 1 : 0
  bucket = aws_s3_bucket.layer["bronze"].id

  rule {
    id     = "bronze-tiering"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 120
      storage_class = "GLACIER_IR"
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Quarentena tem retencao curta e definida: dado rejeitado serve para
# diagnostico, nao para arquivo eterno. Mesma flag do bronze.
resource "aws_s3_bucket_lifecycle_configuration" "quarantine" {
  count  = var.enable_lifecycle ? 1 : 0
  bucket = aws_s3_bucket.layer["quarantine"].id

  rule {
    id     = "quarantine-retention"
    status = "Enabled"

    filter {
      prefix = ""
    }

    expiration {
      days = 90
    }
  }
}

# Tabela de metricas por execucao. Chave composta (run_id, stage) permite
# ler todos os estagios de uma execucao com um unico Query - e isso que o
# quality gate faz antes de aprovar ou reprovar o pipeline.
resource "aws_dynamodb_table" "run_metrics" {
  name         = "${var.project}-run-metrics"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "run_id"
  range_key    = "stage"

  attribute {
    name = "run_id"
    type = "S"
  }
  attribute {
    name = "stage"
    type = "S"
  }

  tags = { Project = var.project }
}

# Role unica assumida por Lambda, Step Functions e Glue. Em producao seriam
# tres roles com permissao minima; a simplificacao esta declarada de
# proposito e a policy ja usa ARN especifico para facilitar a separacao.
resource "aws_iam_role" "pipeline" {
  name = "${var.project}-pipeline-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Principal = {
        Service = [
          "lambda.amazonaws.com",
          "states.amazonaws.com",
          "glue.amazonaws.com"
        ]
      }
    }]
  })
}

# Permissoes do pipeline com ARN explicito por recurso. Curinga so onde o
# servico exige (logs e criacao de log stream dinamica).
resource "aws_iam_policy" "pipeline" {
  name = "${var.project}-pipeline-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = local.bucket_arns
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [for a in local.bucket_arns : "${a}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem", "dynamodb:Query", "dynamodb:GetItem"]
        Resource = aws_dynamodb_table.run_metrics.arn
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      }
    ]
  })
}

# Vincula a policy a role. Recurso separado do role e da policy: e assim que
# o Terraform mantem o grafo de dependencia explicito e o destroy limpo.
resource "aws_iam_role_policy_attachment" "pipeline" {
  role       = aws_iam_role.pipeline.name
  policy_arn = aws_iam_policy.pipeline.arn
}

# Bancos do Glue Data Catalog, um por camada. Criados apenas quando
# enable_glue = true, porque a emulacao de Glue exige LocalStack Pro.
resource "aws_glue_catalog_database" "layer" {
  for_each    = var.enable_glue ? toset(local.glue_dbs) : toset([])
  name        = replace("${var.project}_${each.value}", "-", "_")
  description = "Camada ${each.value} do lakehouse de eventos de pagamento"
}

# Saidas usadas pelos passos seguintes (scripts e comandos CLI).
output "buckets" {
  value = { for l, b in aws_s3_bucket.layer : l => b.id }
}

output "pipeline_role_arn" {
  value = aws_iam_role.pipeline.arn
}

output "metrics_table" {
  value = aws_dynamodb_table.run_metrics.name
}
