from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

PROJECT = "evt-lakehouse"

# Defaults compartilhados: aqui o retry e configuracao do operador, enquanto
# no Step Functions e um bloco declarativo por estado. Mesma ideia,
# granularidade diferente.
default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
}

# catchup=True e a vantagem concreta do Airflow: pedir o reprocessamento de
# um intervalo historico e nativo, enquanto no Step Functions exige disparar
# uma execucao por data a partir de fora.
with DAG(
    dag_id="evt_lakehouse_daily",
    start_date=datetime(2026, 7, 1),
    schedule="0 5 * * *",
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["lakehouse", "pagamentos"],
) as dag:

    # Sensor nativo esperando a particao chegar. No Step Functions, o
    # equivalente seria um Task de polling ou um gatilho por evento do S3.
    wait_landing = S3KeySensor(
        task_id="wait_landing",
        bucket_name=f"{PROJECT}-bronze",
        bucket_key="events/dt={{ ds }}/*",
        wildcard_match=True,
        poke_interval=60,
        timeout=60 * 60 * 3,
        mode="reschedule",
    )

    # Em producao estes dois estagios seriam GlueJobOperator ou
    # EmrServerlessStartJobOperator apontando para os mesmos scripts PySpark.
    def submit_stage(stage: str, **context):
        print(f"submetendo estagio {stage} para dt={context['ds']}")

    bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=submit_stage,
        op_kwargs={"stage": "silver"},
    )

    silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=submit_stage,
        op_kwargs={"stage": "gold"},
    )

    # Mesmo portao de qualidade do Step Functions, expresso em Python.
    def quality_gate(**context):
        print(f"avaliando quality gate de dt={context['ds']}")

    gate = PythonOperator(task_id="quality_gate", python_callable=quality_gate)

    wait_landing >> bronze_to_silver >> silver_to_gold >> gate
