"""Gera .out/visao-dados.html: as entradas e saidas de cada camada do lake.

Pagina local com contagens + amostras de bronze, NRT, silver, quarentena,
gold e metricas, lidas DIRETO do LocalStack.

Autosservico: rode `make visao` depois de qualquer passo e abra no
navegador. Camada que ainda nao existe aparece como "ainda nao gerada",
nao como erro.
"""
import argparse
import html
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ESTILO = """
body { font-family: system-ui, sans-serif; margin: 2rem auto; max-width: 72rem;
       padding: 0 1rem; background: #F6F8F5; color: #1B241F; line-height: 1.5; }
h1 { font-family: Georgia, serif; }
h2 { font-family: Georgia, serif; margin-top: 2.2rem; border-bottom: 2px solid #1E6B4E;
     padding-bottom: .2rem; }
.meta { color: #5B665F; font-size: .9rem; }
.falta { background: #F5EBD8; border-radius: .5rem; padding: .7rem 1rem; color: #8A5A14; }
.num { background: #E3EFE8; border-radius: .4rem; padding: .1rem .5rem; font-weight: 600;
       color: #14523A; }
table { border-collapse: collapse; font-family: ui-monospace, monospace; font-size: .78rem;
        background: #fff; }
.rolagem { overflow-x: auto; border: 1px solid #DFE5DF; border-radius: .5rem; margin: .8rem 0; }
th, td { text-align: left; padding: .3rem .55rem; border-bottom: 1px solid #DFE5DF;
         white-space: nowrap; }
th { color: #8A948D; font-size: .68rem; text-transform: uppercase; }
pre { background: #EEF2EE; border: 1px solid #DFE5DF; border-radius: .5rem; padding: .8rem;
      overflow-x: auto; font-size: .76rem; }
"""


def build_spark(endpoint):
    """Cria a SparkSession com a mesma configuracao S3A dos jobs do lake."""
    return (
        SparkSession.builder.appName("visao_dados")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def tabela(rows, colunas):
    """Renderiza Rows do Spark como tabela HTML com rolagem horizontal."""
    ths = "".join(f"<th>{html.escape(c)}</th>" for c in colunas)
    corpo = []
    for r in rows:
        d = r.asDict()
        tds = []
        for c in colunas:
            v = d.get(c)
            if isinstance(v, list):
                v = ", ".join(str(x) for x in v)
            texto = "<em>null</em>" if v is None else html.escape(str(v))
            tds.append(f"<td>{texto}</td>")
        corpo.append("<tr>" + "".join(tds) + "</tr>")
    return (f'<div class="rolagem"><table><thead><tr>{ths}</tr></thead>'
            f'<tbody>{"".join(corpo)}</tbody></table></div>')


def secao(titulo, passo, fn):
    """Monta uma secao da pagina tolerando ausencia da camada.

    Caminho que nao existe = passo ainda nao feito: qualquer falha vira o
    aviso "ainda nao gerada" com o passo que produz a camada, nao um erro.
    """
    try:
        return f"<h2>{titulo}</h2>\n" + fn()
    except Exception as exc:  # noqa: BLE001 - qualquer falha vira aviso legivel
        breve = str(exc).splitlines()[0][:160]
        return (f"<h2>{titulo}</h2>\n<p class='falta'>ainda não gerada — "
                f"produzida no {passo}. ({html.escape(breve)})</p>")


def main():
    """Le cada camada, monta as secoes e grava a pagina HTML."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default="evt-lakehouse")
    ap.add_argument("--endpoint", default="http://localstack:4566")
    ap.add_argument("--saida", default="/out/visao-dados.html")
    ap.add_argument("--amostra", type=int, default=8)
    args = ap.parse_args()
    p, n = args.project, args.amostra
    spark = build_spark(args.endpoint)
    partes = []

    def bronze():
        df = spark.read.text(f"s3a://{p}-bronze/events/")
        total = df.count()
        linhas = "\n".join(html.escape(r["value"]) for r in df.limit(6).collect())
        return (f"<p><span class='num'>{total:,}</span> linhas JSON (todas as partições, "
                f"batch + CDC). Uma linha = um evento, como chegou:</p><pre>{linhas}</pre>")

    def nrt():
        df = spark.read.text(f"s3a://{p}-bronze/events_nrt/")
        total = df.count()
        linhas = "\n".join(html.escape(r["value"]) for r in df.limit(4).collect())
        return (f"<p><span class='num'>{total:,}</span> linhas entregues pelo Firehose "
                f"(micro lotes de até 60s):</p><pre>{linhas}</pre>")

    def silver():
        df = spark.read.parquet(f"s3a://{p}-silver/events/")
        total = df.count()
        por_dt = df.groupBy("dt").count().orderBy("dt").collect()
        dts = " · ".join(f"{r['dt']}: {r['count']:,}" for r in por_dt)
        cols = ["dt", "event_id", "occurred_at", "customer_id", "merchant_id",
                "payment_method", "currency", "amount", "status", "occurred_hour"]
        return (f"<p><span class='num'>{total:,}</span> registros válidos ({dts}).</p>"
                + tabela(df.select(*cols).limit(n).collect(), cols))

    def quarentena():
        df = spark.read.parquet(f"s3a://{p}-quarantine/events/")
        total = df.count()
        motivos = (df.select(F.explode("rejection_reasons").alias("motivo"))
                   .groupBy("motivo").count().orderBy(F.desc("count")).collect())
        mot = " · ".join(f"{r['motivo']}: {r['count']:,}" for r in motivos)
        cols = ["dt", "event_id", "rejection_reasons", "amount_cents", "currency",
                "occurred_at", "customer_id", "status"]
        return (f"<p><span class='num'>{total:,}</span> rejeitados com motivo ({mot}).</p>"
                + tabela(df.select(*cols).limit(n + 2).collect(), cols))

    def gold():
        df = spark.read.parquet(f"s3a://{p}-gold/merchant_daily/")
        total = df.count()
        cols = ["dt", "merchant_id", "merchant_name", "category", "tx_total",
                "taxa_aprovacao", "gmv_aprovado", "ticket_medio", "share_pix",
                "rank_categoria", "variacao_gmv_pct"]
        amostra = (df.orderBy(F.desc("dt"), "rank_categoria").select(*cols)
                   .limit(n + 2).collect())
        return (f"<p><span class='num'>{total:,}</span> linhas merchant/dia.</p>"
                + tabela(amostra, cols))

    def metricas():
        df = spark.read.option("recursiveFileLookup", "true").json(
            f"s3a://{p}-artifacts/metrics/")
        rows = df.orderBy("stage", "dt").collect()
        cols = [c for c in ["stage", "dt", "input_records", "duplicates_removed",
                            "valid_records", "rejected_records", "reject_rate",
                            "merchants", "generated_at"] if c in df.columns]
        return ("<p>O que o plano de controle (Lambda checkpoint_stage) lê a cada run:</p>"
                + tabela([r for r in rows], cols))

    partes.append(secao("📥 Bronze — entrada crua (JSON Lines)", "Passo 3", bronze))
    partes.append(secao("📡 Bronze NRT — entregue pelo Kinesis/Firehose", "Passo 9", nrt))
    partes.append(secao("✅ Silver — válidos pós-contrato (Parquet)", "Passo 4", silver))
    partes.append(secao("🚧 Quarentena — rejeitados com motivo", "Passo 4", quarentena))
    partes.append(secao("🥇 Gold — merchant_daily analítico", "Passo 5", gold))
    partes.append(secao("📊 Métricas por execução (artifacts)", "Passos 4-5", metricas))

    agora = datetime.now(timezone.utc).isoformat(timespec="seconds")
    pagina = (f"<!doctype html><meta charset='utf-8'><title>Visão dos dados — {p}</title>"
              f"<style>{ESTILO}</style><h1>🔎 Visão dos dados — {p}</h1>"
              f"<p class='meta'>gerada em {agora} UTC direto do LocalStack; "
              f"rode <code>make visao</code> de novo após cada passo.</p>"
              + "\n".join(partes))
    with open(args.saida, "w", encoding="utf-8") as f:
        f.write(pagina)
    print(f"[visao] pagina gravada em {args.saida}")
    spark.stop()


if __name__ == "__main__":
    main()
