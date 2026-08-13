.PHONY: up down fmt validate test apply ingest silver gold pipeline smoke destroy retomar nrt cdc gold-pg

DATES ?= 2026-07-01 2026-07-02 2026-07-03
ENDPOINT ?= http://localhost:4566
# bitnamilegacy: a Broadcom encerrou o catalogo gratuito da Bitnami no Docker
# Hub e as tags versionadas de bitnami/spark passaram a dar "manifest
# unknown"; o acervo congelado vive em bitnamilegacy/ (mesma imagem, mesmos
# paths). Licao de pinning que vale entrevista: a tag pinada garante o
# CONTEUDO, nao a DISPONIBILIDADE - em producao, espelhe a imagem num
# registry proprio (ECR) em vez de depender do Hub. Alternativa mantida
# aberta: apache/spark (paths, usuario e entrypoint diferentes).
SPARK = docker run --rm --network lakehouse-net --user root \
	-v $(PWD)/jobs:/opt/jobs -v $(PWD)/.ivy:/root/.ivy2 bitnamilegacy/spark:3.5.1 spark-submit \
	--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262

up:
	docker compose up -d
	until curl -s $(ENDPOINT)/_localstack/health | grep -q '"s3"'; do sleep 2; done

retomar:
	bash scripts/retomar.sh $(if $(COM_DADOS),--com-dados)

fmt:
	terraform -chdir=infra fmt -check -recursive

validate:
	terraform -chdir=infra init -backend=false && terraform -chdir=infra validate

test:
	docker run --rm --user root -v $(PWD):/app -w /app bitnamilegacy/spark:3.5.1 \
		bash -c "pip install pytest --quiet && python -m pytest tests -q"

apply:
	terraform -chdir=infra init && terraform -chdir=infra apply -auto-approve

ingest:
	python ingest/generate_events.py --dates $(DATES) --endpoint $(ENDPOINT)

silver:
	for d in $(DATES); do $(SPARK) --py-files /opt/jobs/contract.py \
		/opt/jobs/bronze_to_silver.py --dt $$d --endpoint http://localstack:4566; done

gold:
	for d in $(DATES); do $(SPARK) /opt/jobs/silver_to_gold.py \
		--dt $$d --endpoint http://localstack:4566; done

pipeline: up apply ingest silver gold

# Caminho NRT: producer -> Kinesis -> Firehose -> bronze/events_nrt (ate 60s)
nrt:
	python ingest/nrt_producer.py --endpoint $(ENDPOINT)

# CDC simulado: re-emite eventos do primeiro dt como upserts; depois rode
# `make silver` de novo e confira a dedup ficando com o mais recente
cdc:
	python ingest/generate_cdc_updates.py --dt $(word 1,$(DATES)) --endpoint $(ENDPOINT)

# Materializa a gold num Postgres local e roda validacao + EXPLAIN ANALYZE
gold-pg:
	bash scripts/redshift_pg/rodar_gold_pg.sh $(word 2,$(DATES))

smoke:
	python ci/smoke_test.py --endpoint $(ENDPOINT)

destroy:
	terraform -chdir=infra destroy -auto-approve
	docker compose down -v
