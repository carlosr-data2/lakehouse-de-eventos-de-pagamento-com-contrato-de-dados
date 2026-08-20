# Notebooks — a bancada de laboratório do repositório

Notebooks aqui têm um papel definido: **explorar e demonstrar** interativamente o
código dos jobs — rodar uma função com dado fabricado, olhar o resultado no meio
do caminho, testar uma hipótese em segundos. Eles **não substituem** a suíte
pytest: notebook prova que funcionou *quando você rodou*; a suíte prova que
*continua funcionando* a cada mudança, numa máquina limpa, no CI. O fluxo maduro
é circular: descobriu um caso interessante no notebook → ele vira teste em
`tests/` → a descoberta vira proteção permanente.

## Os notebooks

| Notebook | Explora | Par na suíte |
|---|---|---|
| [`01_laboratorio_contrato.ipynb`](01_laboratorio_contrato.ipynb) | As funções puras do contrato (`jobs/contract.py`): cast tolerante, dedup determinística, acúmulo de motivos, quarentena, modelagem do silver — e a distribuição de rejeições usando o próprio gerador | `tests/test_contract.py` |
| [`02_laboratorio_gold_sql.ipynb`](02_laboratorio_gold_sql.ipynb) | O `GOLD_SQL` (`jobs/silver_to_gold.py`) desmontado: filtro de moeda, `FILTER (WHERE ...)`, broadcast join, `DENSE_RANK`/`LAG`, o NULL do estreante e leitura de plano com `explain()` | `tests/test_gold_sql.py` |
| [`03_laboratorio_ingestao_nrt.ipynb`](03_laboratorio_ingestao_nrt.ipynb) | A mecânica da ingestão (`ingest/`): evento limpo, defeitos com probabilidade medida, duplicatas "at least once", JSONL+gzip da bronze e a semântica NRT — PartitionKey, ordem por chave e o hot shard simulado (sem Spark, sem Kinesis) | `tests/test_contract.py` (dedup) |
| [`04_laboratorio_cdc.ipynb`](04_laboratorio_cdc.ipynb) | O upsert CDC absorvido pela dedup do contrato, por construção: reprocesso com update, o contrafactual sem dedup (GMV dobrado) e a prova de que ordem de chegada não importa | `tests/test_contract.py` |

Nada aqui toca S3/LocalStack — tudo roda em memória, com dados fabricados
(os mesmos helpers `_row`/`raw_df` dos testes). Para explorar o **dado real**
do lake, os caminhos são `make visao` (página HTML de todas as camadas) e
`make gold-pg` (gold num Postgres local).

## De passo do projeto para bancada

Estudando um passo do Projeto 1 (a trilha da vaga, no app)? A coluna da direita
diz qual bancada abrir — notebook quando é lógica de dados em memória; `make`,
console ou doc quando é infraestrutura e operação (notebook ali seria só um
invólucro de comandos, pior que o Makefile).

| Passo | Tema | Bancada |
|---|---|---|
| 1 | Provisionamento: storage, IAM, métricas | `make retomar` + [`docs/ARQUITETURA.md`](../docs/ARQUITETURA.md) |
| 2 | Provisionamento: plano de controle | `make retomar` + ADR-004 em [`docs/DECISOES.md`](../docs/DECISOES.md) |
| 3 | Geração e ingestão na bronze | **notebook 03** + `make visao` (a bronze de verdade) |
| 4 | Contrato bronze→silver, quarentena, testes | **notebook 01** + `tests/test_contract.py` |
| 5 | SQL analítico silver→gold | **notebook 02** + `tests/test_gold_sql.py` |
| 6 | Orquestração ponta a ponta | `make pipeline` + `make visao`; a comparação está em [`airflow/`](../airflow/) |
| 7 | CI/CD, FinOps, fechamento | [`docs/CI.md`](../docs/CI.md) (cada estágio reproduzível localmente) |
| 8 | Spark tuning: skew, salting, AQE, SparkUI | **notebook 02 §6** (`explain`) + **notebook 03 §6** (o skew na versão shard) |
| 9 | NRT (Kinesis→Firehose) e CDC | **notebooks 03 e 04**; o caminho real com `make nrt` / `make cdc` |
| 10 | Monitoramento e agendamento | `make visao` (métricas) + [`docs/OPERACAO.md`](../docs/OPERACAO.md) |
| 11 | Governança, Data Mesh, gold num banco vivo | `make gold-pg` + [`docs/GOVERNANCA.md`](../docs/GOVERNANCA.md) e [`docs/DATA-MESH.md`](../docs/DATA-MESH.md) |
| 12 | Analytics Engineering com dbt | `make dbt` + `cd dbt && dbt docs serve`; ADR-009 |

## Setup (uma vez)

```bash
# na raiz do repositório
sudo apt install -y openjdk-17-jre-headless   # a única dependência de sistema
python3 -m venv .venv && source .venv/bin/activate
pip install pyspark==3.5.1 boto3 jupyterlab   # mesma versão de Spark dos jobs/CI
```

## Rodar

```bash
source .venv/bin/activate
jupyter lab notebooks/
```

`pip install pyspark` embute o Spark inteiro (os jars vêm no pacote) — não
existe "instalar o Spark separado", basta a JVM. É a mesma receita do estágio
`unit` do CI, explicada em `docs/CI.md`.

## Convenções (o "melhor padrão" desta pasta)

1. **Roda de cima a baixo.** Toda célula assume apenas o estado das células
   anteriores. Antes de commitar um notebook, `Kernel → Restart Kernel and Run
   All` — se não passa limpo, não está pronto.
2. **Markdown antes, código depois.** Cada célula de código é precedida do
   *porquê* em markdown; o código carrega só comentários pontuais (mesmo padrão
   ASCII dos jobs).
3. **Dados fabricados e determinísticos.** Seed fixa, linhas mínimas e
   cirúrgicas — os mesmos princípios de `tests/` (dado real é grande, aleatório
   e instável; péssimo para demonstrar lógica).
4. **Outputs limpos no git.** Antes de commitar: `Edit → Clear Outputs of All
   Cells` (ou `jupyter nbconvert --clear-output --inplace notebooks/*.ipynb`).
   Diff de output é ruído que enterra o diff de conteúdo.
5. **`spark.stop()` na última célula.** A JVM do Spark não morre com o kernel
   ocioso; célula final libera os ~1-2 GB dela.
6. **Descoberta vira teste.** Caso de borda interessante encontrado aqui deve
   ser promovido a teste na suíte — os exercícios ao fim de cada notebook
   terminam de propósito nesse gesto.
