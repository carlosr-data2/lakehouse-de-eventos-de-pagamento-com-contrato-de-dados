# Decisões de arquitetura

Registro das decisões estruturais do projeto em formato ADR compacto — contexto, decisão, alternativas consideradas e consequências. A tabela-resumo está no [README](../README.md); aqui vive a justificativa completa.

## ADR-001 — Um bucket S3 por camada, não prefixos num bucket único

**Contexto.** O lakehouse tem camadas com requisitos opostos: bronze precisa ser imutável e versionado; gold é derivado e recriável; quarentena é evidência de rejeição.

**Decisão.** Cinco buckets (`evt-lakehouse-bronze/silver/gold/quarantine/artifacts`), um por camada.

**Alternativas consideradas.** Bucket único com prefixos `bronze/`, `silver/`... — mais simples de criar, mas IAM, versionamento e ciclo de vida são configurados **por bucket**: todas as camadas herdariam a mesma política.

**Consequências.** Política sob medida por camada; contagem de recursos maior no Terraform; em contas com muitos domínios de dados, exigiria convenção de nomes disciplinada.

## ADR-002 — Versionamento seletivo: só bronze e quarantine

**Contexto.** Versionar tudo dobra o custo de storage silenciosamente.

**Decisão.** Versionamento habilitado apenas nos dados **não recuperáveis**: bronze (fonte da verdade crua) e quarantine (evidência de rejeição).

**Alternativas consideradas.** Versionar todos os buckets (custo por algo que o pipeline reconstrói) ou nenhum (perda irreversível em caso de sobrescrita indevida na fonte).

**Consequências.** Silver/gold corrompidos se recuperam por reprocessamento, não por versão — o procedimento está no [runbook](OPERACAO.md).

## ADR-003 — Quarentena como área de primeira classe

**Contexto.** Eventos que reprovam no contrato de dados precisam de um destino.

**Decisão.** Bucket próprio, com o **motivo da rejeição gravado** junto do dado.

**Alternativas consideradas.** Descartar (o número some sem explicação — o jeito mais rápido de perder a confiança no lake) ou corrigir automaticamente (esconde o problema do produtor do dado).

**Consequências.** Rejeição vira métrica por motivo; reprocesso é possível; exige fluxo operacional pra revisitar a quarentena.

## ADR-004 — Separação plano de controle / plano de dados

**Contexto.** Orquestrar e processar são responsabilidades com perfis de recurso diferentes.

**Decisão.** Step Functions + Lambda (`evt-lakehouse-pipeline-ops`) validam pré-condições, medem (checkpoint no DynamoDB `evt-lakehouse-run-metrics`) e decidem; Spark só transforma dados.

**Alternativas consideradas.** Lambda processando dados — esbarra no teto de 15 minutos e na memória; lógica de decisão dentro dos jobs Spark — mistura responsabilidades e dificulta teste.

**Consequências.** Jobs são funções puras DataFrame→DataFrame, testáveis com pytest sem cluster; o pipeline ganha um ponto único de decisão auditável.

## ADR-005 — PySpark puro, sem GlueContext/DynamicFrame

**Contexto.** O mesmo job pode rodar em Glue, EMR, EMR Serverless, Databricks ou container local.

**Decisão.** Nenhuma API específica do Glue nos jobs.

**Alternativas consideradas.** GlueContext traria bookmarks nativos e `resolveChoice` — ao custo de prender a carga no Glue.

**Consequências.** Mover a carga entre motores é mudar o submit — alavanca real de FinOps; bookmarks precisam ser resolvidos por particionamento + idempotência (o que o projeto faz).

## ADR-006 — Step Functions para orquestração, com DAG Airflow equivalente

**Contexto.** Pipeline majoritariamente AWS-nativo, execução diária, necessidade de retry/backoff/catch declarativos.

**Decisão.** Máquina de estados `evt-lakehouse-daily-pipeline`; a comparação com Airflow está concreta em [`airflow/dag_evt_lakehouse.py`](../airflow/dag_evt_lakehouse.py).

**Alternativas consideradas.** Airflow — ecossistema de operadores maior, backfill nativo, UI superior; em troca, infraestrutura pra manter. A regra usada: muitas integrações heterogêneas, dependências entre DAGs ou backfill constante inverteriam a decisão.

**Consequências.** Zero infraestrutura de orquestração; backfill precisa ser orquestrado por fora (ver [limitações](LIMITACOES.md)).

## ADR-007 — LocalStack para desenvolvimento e CI

**Contexto.** Ciclo de feedback e custo de uma conta AWS real durante o desenvolvimento.

**Decisão.** Todo o ciclo local e o estágio de integração do CI rodam no LocalStack; o mesmo Terraform aponta pra AWS real trocando o bloco de endpoints.

**Alternativas consideradas.** Conta de desenvolvimento real (custo e lentidão de feedback) ou mocks de unidade apenas (não provam que a infraestrutura sobe).

**Consequências.** Onde a emulação diverge, a diferença é **codificada** — ver ADR-008 e a flag `enable_lifecycle` em [`infra/variables.tf`](../infra/variables.tf) — nunca improvisada.

## ADR-008 — Provider AWS travado em `>= 5.60.0, < 5.67.0`

**Contexto.** A partir do 5.67, o provider valida a definição de Step Functions chamando `ValidateStateMachineDefinition` — API que o LocalStack community não implementa; o apply quebra com 501 mesmo com a definição correta.

**Decisão.** Trava explícita de versão em `required_providers`, com o motivo comentado no código.

**Alternativas consideradas.** `~> 5.60` solto — deixa o init resolver pra versão mais nova e é um bug latente que dispara sozinho no futuro (foi exatamente assim que o CI o encontrou).

**Consequências.** Atualizar o provider vira decisão explícita; ao migrar pra AWS real, a trava pode (e deve) ser removida.

## ADR-009 — dbt no warehouse de serving, não no lake

**Contexto.** A modelagem de consumo (silver→gold) é SQL por decisão do projeto, e a área de negócio audita essa camada. dbt operacionaliza SQL — deploy, testes, documentação, linhagem — mas precisa de um lugar pra rodar, e o pipeline tem dois candidatos: dentro do lake (S3/Spark) ou no warehouse de serving (o Postgres do `make gold-pg`, o papel que o Redshift faria em produção).

**Decisão.** dbt apenas no serving ([`dbt/`](../dbt/)): source declarado sobre `analytics.merchant_daily`, staging como view com chave explícita, dois marts (table com `post-hook ANALYZE` e incremental com `unique_key`), testes nos três andares que a ferramenta oferece — nativos (`unique`/`not_null`/`accepted_values`), genérico com argumentos (macro `entre`) e singular de reconciliação mart × fonte — com targets dev/prod separados por schema e `dbt docs` como linhagem extraída do pipeline. O job PySpark de contrato bronze→silver continua testado com pytest.

**Alternativas consideradas.** dbt sobre o lake via dbt-spark/dbt-athena (o LocalStack community não emula Athena/Glue e o ganho seria reescrever em outra ferramenta o que já é função pura testada); substituir o job de contrato por testes de dbt (regressão: teste de dbt reprova, mas não roteia a linha rejeitada pra quarentena com o motivo gravado).

**Consequências.** A taxonomia fica explícita — pytest testa código Spark, dbt testa modelos SQL, a quarentena vigia o dado em runtime — e a promoção dev→prod dos modelos vira gesto declarativo (`--target prod`). O serving local usa senha de laboratório versionada em `dbt/profiles.yml` (nada ali é segredo); contra um warehouse real, as credenciais sairiam do profile pra `env_var`/secret do CI. O estágio `dbt` do CI prova os modelos numa máquina limpa com uma gold mínima semeada ([`ci/seed_gold_pg.sql`](../ci/seed_gold_pg.sql)).

## ADR-010 — Open Table Formats: Iceberg no silver, Delta no gold

**Contexto.** Parquet particionado com sobrescrita por partição era idempotente, mas não atômico: durante o overwrite existia uma janela em que um leitor concorrente via a partição pela metade; não havia histórico (um overwrite errado destruía o estado anterior do silver/gold sem volta barata) nem evolução de schema sem reprocesso. Formato de tabela transacional era a limitação nº 2 assumida do projeto.

**Decisão.** Silver vira tabela **Iceberg** (`lake.silver.events`): catálogo `hadoop` com warehouse no próprio bucket (`s3a://evt-lakehouse-silver/warehouse`), `format-version 2` (row-level deletes por merge-on-read, a base para um futuro `MERGE INTO` de CDC) e escrita por `overwritePartitions` — o commit atômico substitui a sobrescrita dinâmica do Parquet. Gold vira tabela **Delta** no mesmo caminho físico de sempre (`s3a://evt-lakehouse-gold/merchant_daily/`), com `replaceWhere` por `dt` — a transação cobre exatamente o padrão de reprocesso diário do job. A **quarentena permanece Parquet**: é evidência raramente lida, append por partição, e o versionamento do bucket (ADR-002) já cobre a proteção que um formato transacional daria.

**Por que dois formatos, não um.** Tecnicamente um único formato bastaria — e seria até mais simples. Os dois entram porque cada um está no ponto onde joga a favor (schema evolution e particionamento evolutivo pesam no silver, que recebe o dado do contrato; `replaceWhere` casa 1:1 com o reprocesso por `dt` do gold) e porque manter ambos no mesmo pipeline expõe a comparação real — layout de metadados (`metadata/` + snapshots vs `_delta_log/` + versões), time travel por snapshot-id vs por versão, catálogo vs caminho — em vez de opinião decorada.

**Alternativas consideradas.** Um formato só (menos jars e menos conceitos; perderia a comparação concreta e o argumento de escolha por camada); quarentena também transacional (custo de metadados por algo que é evidência forense, não fonte de leitura); manter Parquet e aceitar a janela de leitura parcial (era a limitação que esta decisão paga).

**Consequências.** Todo leitor precisa dos jars — `PACKAGES` no Makefile leva `iceberg-spark-runtime` e `delta-spark` a jobs, visão e export do serving; ler o gold com `spark.read.parquet` cru passa a ser **bug** (somaria versões antigas). As garantias viraram teste ([`tests/test_table_formats.py`](../tests/test_table_formats.py): overwrite idempotente, time travel, schema evolution). O catálogo `hadoop` e a manutenção de tabelas (compaction, expiração de snapshots) entram como a nova limitação assumida nº 2 ([limitações](LIMITACOES.md)).
