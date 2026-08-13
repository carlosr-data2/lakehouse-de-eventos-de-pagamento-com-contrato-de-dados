# Monitoramento CONTINUO do pipeline - a peca que transforma "alerta por
# execucao" (SNS no fim da state machine) em serie temporal com limiar:
# a Lambda de plano de controle publica metricas custom no CloudWatch a cada
# run (namespace ${var.project}/pipeline), e o alarme abaixo observa a taxa
# de rejeicao ao longo do tempo - se ela passar do limite do quality gate,
# o MESMO topico SNS de alertas e acionado, sem depender de ninguem olhar
# o resultado de uma execucao individual.
#
# Limite do emulador, documentado como sempre: o LocalStack community
# aceita PutMetricData e o CRUD de alarmes, mas a AVALIACAO periodica do
# alarme e best-effort. O teste honesto local e disparar a transicao na
# mao (aws cloudwatch set-alarm-state) e conferir a mensagem na fila SQS;
# na AWS real a avaliacao e automatica e nada aqui muda.
resource "aws_cloudwatch_metric_alarm" "reject_rate" {
  alarm_name          = "${var.project}-reject-rate-alto"
  alarm_description   = "Taxa de rejeicao do contrato acima do limite do quality gate"
  namespace           = "${var.project}/pipeline"
  metric_name         = "reject_rate"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = var.max_reject_rate
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]

  tags = { Project = var.project }
}

# Automacao do disparo: o pipeline deixa de depender de um humano rodar
# start-execution. A regra agenda a state machine todo dia as 06:00 UTC
# (03:00 BRT), depois da janela de chegada dos eventos do dia anterior.
resource "aws_cloudwatch_event_rule" "daily" {
  name                = "${var.project}-daily-schedule"
  description         = "Dispara o pipeline diario do lakehouse (D-1)"
  schedule_expression = "cron(0 6 * * ? *)"

  tags = { Project = var.project }
}

# O alvo passa dt="auto": quem resolve a data e a propria Lambda de
# validacao (D-1 em UTC), porque EventBridge nao faz aritmetica de data no
# input. O run_id herda o id unico do evento de agendamento - e o que
# mantem o checkpoint de cada dia separado no DynamoDB.
resource "aws_cloudwatch_event_target" "start_pipeline" {
  rule     = aws_cloudwatch_event_rule.daily.name
  arn      = aws_sfn_state_machine.daily_pipeline.arn
  role_arn = aws_iam_role.pipeline.arn

  input_transformer {
    input_paths = {
      id = "$.id"
    }
    input_template = <<-EOT
      {"run_id": "agendado-<id>", "dt": "auto", "stages": ["silver", "gold"]}
    EOT
  }
}

# Permissao minima do agendador: iniciar exatamente esta state machine.
# Inline e separada da policy principal de proposito - quando a role unica
# for dividida em producao, esta permissao ja esta isolada.
resource "aws_iam_role_policy" "events_start_pipeline" {
  name = "${var.project}-events-start-pipeline"
  role = aws_iam_role.pipeline.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = aws_sfn_state_machine.daily_pipeline.arn
    }]
  })
}
