# Ingestao near real time, paralela ao caminho batch: eventos entram num
# Kinesis Data Stream e o Firehose os entrega na CAMADA BRONZE em micro
# lotes, sob o prefixo events_nrt/ - separado do events/ do batch de
# proposito, para os dois caminhos coexistirem sem se misturar. O contrato
# de dados nao muda: silver continua validando o que quer que chegue.
#
# Trade-off central (e resposta de entrevista): Kinesis+Firehose entrega
# latencia de ~1 minuto sem NENHUM servidor para operar; Kafka entregaria
# latencia menor e replay mais rico ao custo de um cluster para administrar.
# Para eventos de pagamento com consumo analitico, o buffer de 60s do
# Firehose e imperceptivel - o caso que inverteria a decisao e consumo
# transacional/operacional em milissegundos.
resource "aws_kinesis_stream" "events_nrt" {
  name             = "${var.project}-events-nrt"
  shard_count      = 1
  retention_period = 24

  tags = { Project = var.project }
}

# Firehose: o "ultimo quilometro" gerenciado entre o stream e o S3.
# Buffering minimo (60s ou 1MB, o que vier primeiro) porque o objetivo
# aqui e demonstrar o fluxo; em producao o buffer e uma decisao de FinOps:
# lotes maiores = menos objetos pequenos no S3 = menos requests e menos
# overhead de listagem pro Spark.
resource "aws_kinesis_firehose_delivery_stream" "nrt_to_bronze" {
  name        = "${var.project}-nrt-to-bronze"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.events_nrt.arn
    role_arn           = aws_iam_role.pipeline.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.pipeline.arn
    bucket_arn          = aws_s3_bucket.layer["bronze"].arn
    prefix              = "events_nrt/dt=!{timestamp:yyyy-MM-dd}/"
    error_output_prefix = "events_nrt_errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/"
    buffering_interval  = 60
    buffering_size      = 1
  }

  tags = { Project = var.project }
}

# Leitura do stream pelo Firehose - inline e separada da policy principal,
# mesmo racional da permissao do agendador em monitoring.tf.
resource "aws_iam_role_policy" "firehose_read_stream" {
  name = "${var.project}-firehose-read-stream"
  role = aws_iam_role.pipeline.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "kinesis:DescribeStream",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords",
        "kinesis:ListShards"
      ]
      Resource = aws_kinesis_stream.events_nrt.arn
    }]
  })
}

output "nrt_stream_name" {
  value = aws_kinesis_stream.events_nrt.name
}
