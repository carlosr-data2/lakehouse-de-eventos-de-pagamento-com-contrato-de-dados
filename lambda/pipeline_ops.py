"""Plano de controle do pipeline: a Lambda unica das acoes da state machine.

Nenhum dado passa por aqui (quem processa e o Spark); a Lambda so valida
pre-condicoes, registra checkpoints e decide o portao de qualidade. O
campo "action" do payload escolhe a acao:

* validate_landing: a particao do dia chegou na bronze?
* checkpoint_stage: o estagio produziu saida? Persiste metricas no
  DynamoDB e publica a serie temporal no CloudWatch.
* quality_gate: taxa de rejeicao do silver dentro do limite?

Configuracao via ambiente: PROJECT, METRICS_TABLE, MAX_REJECT_RATE
(limite do gate, default 0.05) e AWS_REGION.
"""

import json
import os
from datetime import datetime, timedelta, timezone

import boto3

PROJECT = os.environ["PROJECT"]
METRICS_TABLE = os.environ["METRICS_TABLE"]
MAX_REJECT_RATE = float(os.environ.get("MAX_REJECT_RATE", "0.05"))
REGION = os.environ.get("AWS_REGION", "us-east-1")

# Prefixo de saida de cada estagio. Fonte unica de verdade sobre onde cada
# camada escreve - o Spark usa exatamente os mesmos caminhos.
STAGE_OUTPUT = {
    "silver": (f"{PROJECT}-silver", "events"),
    "gold": (f"{PROJECT}-gold", "merchant_daily"),
}


# Erros de dominio com nome proprio: o Step Functions faz Retry/Catch pelo
# nome da classe, entao errar aqui quebra a politica de resiliencia.
class LandingEmptyError(Exception):
    pass


class StageOutputMissingError(Exception):
    pass


def _client(service):
    """Cria o cliente boto3 certo para o ambiente em que a Lambda roda.

    Dentro do LocalStack a Lambda enxerga o emulador pelo hostname injetado
    em LOCALSTACK_HOSTNAME. Na AWS real a variavel nao existe e o boto3
    resolve o endpoint padrao -- o mesmo codigo serve para os dois
    ambientes.
    """
    host = os.environ.get("LOCALSTACK_HOSTNAME")
    if host:
        return boto3.client(service, endpoint_url=f"http://{host}:4566", region_name=REGION)
    return boto3.client(service, region_name=REGION)


def _count_objects(bucket, prefix):
    """Conta objetos sob um prefixo usando paginacao.

    list_objects_v2 devolve no maximo 1000 chaves por chamada; sem
    paginator a contagem ficaria errada em particao grande.
    """
    s3 = _client("s3")
    total = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        total += page.get("KeyCount", 0)
    return total


def _read_stage_metrics(stage, dt):
    """Le o arquivo de metricas que o job Spark escreveu para o estagio.

    O Spark grava um diretorio com part-*.json, entao localizamos a
    primeira chave .json em vez de assumir um nome fixo.

    Returns:
        Dict de metricas do estagio, ou {} se ainda nao ha arquivo.
    """
    s3 = _client("s3")
    bucket = f"{PROJECT}-artifacts"
    prefix = f"metrics/{stage}/dt={dt}/"
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in resp.get("Contents", []):
        if obj["Key"].endswith(".json"):
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode("utf-8")
            return json.loads(body.strip().splitlines()[0])
    return {}


def _publish_metrics(dimensions, values):
    """Publica metricas custom no CloudWatch (serie que o alarme observa).

    Best-effort de proposito: metrica indisponivel nao pode derrubar o
    pipeline (o dado dela ja esta no DynamoDB de qualquer forma); a falha
    vai pro log e a execucao segue.

    Args:
        dimensions: Dimensoes CloudWatch, como {"Stage": "silver"}.
        values: Mapa nome da metrica -> valor numerico.
    """
    try:
        _client("cloudwatch").put_metric_data(
            Namespace=f"{PROJECT}/pipeline",
            MetricData=[
                {
                    "MetricName": name,
                    "Dimensions": [
                        {"Name": k, "Value": v} for k, v in dimensions.items()
                    ],
                    "Value": float(value),
                    "Timestamp": datetime.now(timezone.utc),
                }
                for name, value in values.items()
            ],
        )
    except Exception as exc:  # noqa: BLE001 - best-effort, nunca derruba o run
        print(f"[metrics] falha ao publicar no CloudWatch: {exc}")


def validate_landing(event):
    """Acao 1: valida se a particao do dia chegou na bronze.

    E a pre-condicao do pipeline inteiro -- falhar cedo aqui evita queimar
    recurso de cluster.

    dt="auto" (ou ausente) resolve para D-1 em UTC: e assim que o
    agendamento do EventBridge dispara sem saber aritmetica de data -- os
    estados seguintes leem o dt resolvido daqui ($.landing.dt), nao do
    input.

    Args:
        event: Payload com dt opcional (YYYY-MM-DD, "auto" ou ausente).

    Returns:
        Dict com o dt resolvido e a contagem de objetos na landing.

    Raises:
        LandingEmptyError: Se nao ha nenhum arquivo na particao -- o Step
            Functions faz Retry/Catch por este nome de classe.
    """
    dt = event.get("dt")
    if not dt or dt == "auto":
        dt = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    bucket = f"{PROJECT}-bronze"
    prefix = f"events/dt={dt}/"
    count = _count_objects(bucket, prefix)
    if count == 0:
        raise LandingEmptyError(f"Nenhum arquivo em s3://{bucket}/{prefix}")
    return {"dt": dt, "landing_objects": count}


def checkpoint_stage(event):
    """Acao 2: confirma a saida do estagio e persiste o checkpoint.

    Confere que o estagio produziu objetos no bucket esperado, le as
    metricas que o job publicou e grava tudo no DynamoDB; depois publica a
    serie temporal no CloudWatch.

    Args:
        event: Payload com dt, stage ("silver"/"gold") e run_id.

    Returns:
        Dict com o estagio, a contagem de objetos e as metricas lidas.

    Raises:
        StageOutputMissingError: Se o estagio nao produziu saida -- erro
            retentavel na politica do Step Functions.
    """
    dt, stage, run_id = event["dt"], event["stage"], event["run_id"]
    bucket, root = STAGE_OUTPUT[stage]
    prefix = f"{root}/dt={dt}/"
    count = _count_objects(bucket, prefix)
    if count == 0:
        raise StageOutputMissingError(f"Estagio {stage} sem saida em s3://{bucket}/{prefix}")

    metrics = _read_stage_metrics(stage, dt)
    _client("dynamodb").put_item(
        TableName=METRICS_TABLE,
        Item={
            "run_id": {"S": run_id},
            "stage": {"S": stage},
            "dt": {"S": dt},
            "output_objects": {"N": str(count)},
            "metrics": {"S": json.dumps(metrics)},
            "recorded_at": {"S": datetime.now(timezone.utc).isoformat()},
        },
    )

    # Serie temporal por estagio. Dimensao so por Stage - dt como dimensao
    # criaria uma serie nova por dia (alta cardinalidade), o anti-padrao
    # classico de custo em metrica custom.
    _publish_metrics(
        {"Stage": stage},
        {
            "output_objects": count,
            "input_records": int(metrics.get("input_records", 0)),
            "rejected_records": int(metrics.get("rejected_records", 0)),
        },
    )
    return {"stage": stage, "output_objects": count, "metrics": metrics}


def quality_gate(event):
    """Acao 3: portao de qualidade da execucao.

    Le as metricas da execucao no DynamoDB e compara a taxa de rejeicao do
    silver contra MAX_REJECT_RATE.

    Args:
        event: Payload com run_id e dt.

    Returns:
        Dict com status PASS/FAIL, a taxa, o limite e a mensagem que sera
        publicada no SNS.
    """
    run_id, dt = event["run_id"], event["dt"]
    resp = _client("dynamodb").query(
        TableName=METRICS_TABLE,
        KeyConditionExpression="run_id = :r",
        ExpressionAttributeValues={":r": {"S": run_id}},
    )
    silver = {}
    for item in resp.get("Items", []):
        if item["stage"]["S"] == "silver":
            silver = json.loads(item["metrics"]["S"])

    total = int(silver.get("input_records", 0))
    rejected = int(silver.get("rejected_records", 0))
    rate = (rejected / total) if total else 1.0
    status = "PASS" if rate <= MAX_REJECT_RATE else "FAIL"

    # E esta metrica que o alarme reject-rate-alto observa: o quality gate
    # decide POR EXECUCAO; o alarme acompanha a TENDENCIA entre execucoes.
    _publish_metrics(
        {"Gate": "quality"},
        {"reject_rate": rate, "gate_pass": 1.0 if status == "PASS" else 0.0},
    )

    return {
        "status": status,
        "reject_rate": round(rate, 4),
        "threshold": MAX_REJECT_RATE,
        "message": (
            f"[{status}] run={run_id} dt={dt} "
            f"registros={total} rejeitados={rejected} "
            f"taxa={rate:.2%} limite={MAX_REJECT_RATE:.2%}"
        ),
    }


ACTIONS = {
    "validate_landing": validate_landing,
    "checkpoint_stage": checkpoint_stage,
    "quality_gate": quality_gate,
}


def handler(event, context):
    """Ponto de entrada da Lambda: despacha o payload para a acao pedida.

    Acao desconhecida vira erro explicito em vez de retorno silencioso
    vazio, que seria muito pior de diagnosticar.

    Raises:
        ValueError: Se event["action"] nao esta em ACTIONS.
    """
    action = event.get("action")
    if action not in ACTIONS:
        raise ValueError(f"Acao desconhecida: {action}")
    return ACTIONS[action](event)
