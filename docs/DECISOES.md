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
