# Governança de dados — ownership, dicionário, linhagem e acesso

Governança aqui não é um comitê: é o conjunto de respostas verificáveis a
quatro perguntas — **de quem é o dado, o que cada campo significa, de onde
ele veio e quem pode lê-lo**. Este documento responde as quatro para este
pipeline, e aponta a evolução de cada resposta em escala de empresa.

## Ownership

| Ativo | Dono | Responsabilidade |
|---|---|---|
| Contrato de entrada (`jobs/contract.py`) | domínio de pagamentos | evoluir schema e regras com versionamento explícito |
| Camadas bronze/silver/quarantine | domínio de pagamentos | retenção, reprocesso, resposta a incidente de qualidade |
| Gold `merchant_daily` | domínio de pagamentos, para consumo analítico | estabilidade de schema (mudança = conversa com consumidores antes) |
| Dimensão `dim_merchants` | cadastro (origem), espelhada aqui | atualização e correção vêm da origem, nunca editadas no lake |

## Dicionário de dados — gold `merchant_daily`

A interface pública do produto. Mudar tipo ou semântica de qualquer coluna
é mudança de contrato com consumidor — versiona, comunica, deprecia.

| Coluna | Tipo | Significado |
|---|---|---|
| `dt` | date (partição) | dia do fato (data do evento, não da ingestão) |
| `merchant_id` | string | chave de negócio do estabelecimento |
| `merchant_name`, `category`, `state` | string | atributos da dimensão no momento da carga |
| `tx_total` / `tx_aprovadas` | long | transações no dia / aprovadas |
| `clientes_unicos` | long | `COUNT(DISTINCT customer_id)` no dia |
| `taxa_aprovacao` | decimal(9,4) | `tx_aprovadas / tx_total` |
| `gmv_aprovado` | decimal(18,2) | soma de `amount` com status aprovado (BRL) |
| `valor_estornado` | decimal(18,2) | soma de `amount` com status refunded |
| `ticket_medio` | decimal(18,2) | `gmv_aprovado / tx_aprovadas` |
| `share_pix` | decimal(9,4) | fração do GMV aprovado via PIX |
| `rank_categoria` | int | posição do merchant na categoria no dia (por GMV) |
| `gmv_dia_anterior` / `variacao_gmv_pct` | decimal | comparação D-1 do mesmo merchant (LAG) |

Classificação: os eventos carregam identificadores de cliente
(`customer_id`) — dado pessoal pseudonimizado. A gold agrega por merchant e
**não expõe** `customer_id`; é a fronteira onde o dado deixa de ser sujeito
a pedido de titular (LGPD) e vira estatística.

## Linhagem

```
gerador batch ──────────► bronze events/dt=…          (imutável, versionado)
producer NRT ─► Kinesis ─► Firehose ─► bronze events_nrt/dt=…
CDC simulado ──────────► bronze events/dt=…/cdc-updates-…

bronze ─[contract.py: tipagem, regras, dedup]─► silver  (válidos)
                                            └─► quarantine (rejeitados + motivo)
silver + dim_merchants ─[GOLD_SQL]─► gold merchant_daily
cada job ─► artifacts metrics/… ─► DynamoDB + CloudWatch (métricas por run)
```

A linhagem é curta e legível porque cada transformação vive num arquivo com
nome do movimento (`bronze_to_silver`, `silver_to_gold`). Em escala, isso
migra para linhagem automatizada — OpenLineage nos jobs, ou o que o
catálogo da plataforma oferecer (Glue/DataZone/Unity) — mas a regra que não
muda é esta: **linhagem se extrai do pipeline, não se desenha em slide.**

## Catálogo e descoberta

- Local: bancos do Glue Data Catalog por camada, atrás de `enable_glue`
  (emulação exige LocalStack Pro) — a estrutura já nasce catalogável.
- AWS real: crawlers ou registro direto das tabelas + **DataZone** por cima
  para metadados de negócio (glossário, formulários, assets) — automação de
  criação de assets/glossário via Lambda e Terraform é o desenho que já
  implementei em produção na Logicalis, e é o degrau entre "catálogo
  técnico" e "descoberta para gente de negócio".

## Acesso

Hoje: role IAM única de laboratório (limitação assumida nº 1). O desenho de
produção, já preparado pela separação de buckets por camada:

- **Por camada**: consumidor analítico lê gold; silver é do time do
  domínio; bronze/quarantine, só pipeline e resposta a incidente.
- **No warehouse**: grants por schema no Redshift; RLS por categoria ou
  regional quando o consumo exigir recorte; colunas sensíveis ficam de fora
  da gold por construção (ver classificação acima), que é a forma mais
  barata de column-level security — não expor.
- **Auditoria**: CloudTrail + access logs do S3 na AWS real; localmente, os
  logs do LocalStack cumprem o papel didático.
