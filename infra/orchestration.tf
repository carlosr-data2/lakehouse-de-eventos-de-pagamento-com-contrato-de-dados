# Empacota a pasta lambda/ em zip no momento do plan. Sem passo manual de
# build: o artefato e derivado do codigo versionado.
data "archive_file" "pipeline_ops" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda"
  output_path = "${path.module}/.build/pipeline_ops.zip"
}

# Funcao de plano de controle. As variaveis de ambiente carregam os nomes
# reais dos recursos criados no passo 1 - nada e hardcoded no Python.
resource "aws_lambda_function" "pipeline_ops" {
  function_name    = "${var.project}-pipeline-ops"
  role             = aws_iam_role.pipeline.arn
  handler          = "pipeline_ops.handler"
  runtime          = "python3.12"
  filename         = data.archive_file.pipeline_ops.output_path
  source_code_hash = data.archive_file.pipeline_ops.output_base64sha256
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      PROJECT         = var.project
      METRICS_TABLE   = aws_dynamodb_table.run_metrics.name
      MAX_REJECT_RATE = tostring(var.max_reject_rate)
    }
  }
}

# Topico de alertas do pipeline (sucesso e falha).
resource "aws_sns_topic" "alerts" {
  name = "${var.project}-pipeline-alerts"
}

# Fila assinando o topico. Existe para tornar a notificacao verificavel
# localmente: assinatura por e-mail exigiria confirmacao manual.
resource "aws_sqs_queue" "alerts_inbox" {
  name = "${var.project}-alerts-inbox"
}

resource "aws_sns_topic_subscription" "alerts_to_sqs" {
  topic_arn            = aws_sns_topic.alerts.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.alerts_inbox.arn
  raw_message_delivery = true
}

# Log group da maquina de estados, com retencao curta (FinOps: log de
# pipeline diario raramente e util depois de duas semanas).
resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${var.project}-daily-pipeline"
  retention_in_days = 14
}

# Definicao da maquina de estados em ASL. Escrita com jsonencode para que o
# Terraform valide a estrutura e interpole ARNs reais em vez de string solta.
locals {
  state_machine_definition = jsonencode({
    Comment = "Pipeline diario do lakehouse de eventos de pagamento"
    StartAt = "ValidateLanding"
    States = {

      # Pre-condicao: a particao do dia precisa existir no bronze.
      ValidateLanding = {
        Type     = "Task"
        Resource = aws_lambda_function.pipeline_ops.arn
        Parameters = {
          "action"   = "validate_landing"
          "run_id.$" = "$.run_id"
          "dt.$"     = "$.dt"
        }
        ResultPath = "$.landing"
        Retry = [{
          ErrorEquals     = ["Lambda.ServiceException", "Lambda.TooManyRequestsException"]
          IntervalSeconds = 3
          MaxAttempts     = 2
          BackoffRate     = 2
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "ProcessStages"
      }

      # Percorre os estagios em ordem (MaxConcurrency 1 porque gold depende
      # de silver). Paralelizar no futuro e so aumentar esse numero.
      # O dt vem de $.landing.dt (saida do ValidateLanding), nao do input:
      # e o que permite ao agendamento do EventBridge passar dt="auto" e a
      # Lambda resolver D-1 - execucao manual com dt explicito continua
      # funcionando, porque a Lambda ecoa o dt recebido.
      ProcessStages = {
        Type           = "Map"
        ItemsPath      = "$.stages"
        MaxConcurrency = 1
        Parameters = {
          "action"   = "checkpoint_stage"
          "run_id.$" = "$.run_id"
          "dt.$"     = "$.landing.dt"
          "stage.$"  = "$$.Map.Item.Value"
        }
        Iterator = {
          StartAt = "CheckpointStage"
          States = {
            CheckpointStage = {
              Type     = "Task"
              Resource = aws_lambda_function.pipeline_ops.arn
              Retry = [{
                ErrorEquals     = ["StageOutputMissingError"]
                IntervalSeconds = 10
                MaxAttempts     = 3
                BackoffRate     = 2
              }]
              End = true
            }
          }
        }
        ResultPath = "$.stage_results"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "QualityGate"
      }

      # Portao de qualidade: le as metricas gravadas e decide PASS/FAIL.
      QualityGate = {
        Type     = "Task"
        Resource = aws_lambda_function.pipeline_ops.arn
        Parameters = {
          "action"   = "quality_gate"
          "run_id.$" = "$.run_id"
          "dt.$"     = "$.landing.dt"
        }
        ResultPath = "$.gate"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
        Next = "EvaluateGate"
      }

      EvaluateGate = {
        Type = "Choice"
        Choices = [{
          Variable     = "$.gate.status"
          StringEquals = "PASS"
          Next         = "NotifySuccess"
        }]
        Default = "NotifyFailure"
      }

      # Notificacao de sucesso usando a integracao otimizada com SNS.
      NotifySuccess = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.alerts.arn
          Subject     = "Pipeline concluido"
          "Message.$" = "$.gate.message"
        }
        Next = "Done"
      }

      # Notificacao de falha. Usa o objeto de contexto ($$) porque este
      # estado e alcancado por caminhos diferentes, com payloads diferentes.
      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.alerts.arn
          Subject     = "Pipeline falhou"
          "Message.$" = "$$.Execution.Name"
        }
        Next = "PipelineFailed"
      }

      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "Falha de validacao, de estagio ou reprovacao no quality gate"
      }

      Done = { Type = "Succeed" }
    }
  })
}

# A maquina de estados propriamente dita, com log completo habilitado.
resource "aws_sfn_state_machine" "daily_pipeline" {
  name       = "${var.project}-daily-pipeline"
  role_arn   = aws_iam_role.pipeline.arn
  definition = local.state_machine_definition

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.daily_pipeline.arn
}

output "alerts_queue_url" {
  value = aws_sqs_queue.alerts_inbox.id
}
