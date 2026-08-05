-- OPCAO A: Redshift Spectrum. Le o Parquet direto do S3, sem copia e sem
-- custo de armazenamento no cluster. Ideal para dado que muda diariamente e
-- e consultado de forma esporadica ou exploratoria.
CREATE EXTERNAL SCHEMA IF NOT EXISTS lakehouse_gold
FROM DATA CATALOG
DATABASE 'evt_lakehouse_gold'
IAM_ROLE 'arn:aws:iam::000000000000:role/evt-lakehouse-pipeline-role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Consulta tipica sobre a tabela externa. O filtro por dt aciona partition
-- pruning: sem ele, o Spectrum varre todo o historico e o custo dispara.
SELECT category, SUM(gmv_aprovado) AS gmv, AVG(taxa_aprovacao) AS aprovacao
FROM lakehouse_gold.merchant_daily
WHERE dt = '2026-07-03'
GROUP BY category
ORDER BY gmv DESC;


-- OPCAO B: tabela interna materializada. Custa armazenamento no cluster e
-- um COPY diario, mas entrega latencia baixa e previsivel para dashboard
-- consultado o tempo todo.
CREATE TABLE IF NOT EXISTS analytics.merchant_daily (
    dt                DATE          NOT NULL,
    merchant_id       VARCHAR(32)   NOT NULL,
    merchant_name     VARCHAR(128),
    category          VARCHAR(64),
    state             CHAR(2),
    tx_total          BIGINT,
    tx_aprovadas      BIGINT,
    clientes_unicos   BIGINT,
    taxa_aprovacao    DECIMAL(9,4),
    gmv_aprovado      DECIMAL(18,2),
    valor_estornado   DECIMAL(18,2),
    ticket_medio      DECIMAL(18,2),
    share_pix         DECIMAL(9,4),
    rank_categoria    INTEGER,
    gmv_dia_anterior  DECIMAL(18,2),
    variacao_gmv_pct  DECIMAL(9,4)
)
-- DISTKEY por merchant_id: distribui uniformemente e coloca no mesmo slice
-- as linhas que serao unidas a outras tabelas por estabelecimento.
DISTSTYLE KEY
DISTKEY (merchant_id)
-- SORTKEY comecando por dt: praticamente toda consulta filtra por data, e a
-- ordenacao permite ao Redshift pular blocos inteiros na leitura.
COMPOUND SORTKEY (dt, category);

-- Carga incremental idempotente: apaga a particao logica do dia e recarrega.
DELETE FROM analytics.merchant_daily WHERE dt = '2026-07-03';

COPY analytics.merchant_daily
FROM 's3://evt-lakehouse-gold/merchant_daily/dt=2026-07-03/'
IAM_ROLE 'arn:aws:iam::000000000000:role/evt-lakehouse-pipeline-role'
FORMAT AS PARQUET;

-- ANALYZE apos carga: sem estatistica atualizada o planejador escolhe
-- ordem de juncao ruim, e o ganho de DISTKEY/SORTKEY se perde.
ANALYZE analytics.merchant_daily;
