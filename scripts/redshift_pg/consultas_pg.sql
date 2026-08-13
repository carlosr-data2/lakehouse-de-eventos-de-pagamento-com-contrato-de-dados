-- Consultas de validacao da gold materializada no Postgres - as mesmas
-- perguntas de negocio da versao Redshift, mais um EXPLAIN ANALYZE para
-- fechar o ciclo "otimizacao de consulta" num banco relacional VIVO.

-- 1) Agregacao tipica por categoria no dia (a mesma do redshift_gold.sql).
SELECT category, SUM(gmv_aprovado) AS gmv, AVG(taxa_aprovacao) AS aprovacao
FROM analytics.merchant_daily
WHERE dt = :'dia'
GROUP BY category
ORDER BY gmv DESC;

-- 2) Top 5 merchants por GMV no dia, com o rank ja calculado no lake.
SELECT merchant_id, merchant_name, category, gmv_aprovado, rank_categoria
FROM analytics.merchant_daily
WHERE dt = :'dia'
ORDER BY gmv_aprovado DESC
LIMIT 5;

-- 3) Sanidade da carga: contagem e janela de datas presentes.
SELECT COUNT(*) AS linhas, MIN(dt) AS primeiro_dt, MAX(dt) AS ultimo_dt
FROM analytics.merchant_daily;

-- 4) O plano de execucao, de verdade: com o indice (dt, category), o filtro
-- por dt deve aparecer como Index Scan / Bitmap Index Scan em vez de
-- Seq Scan - o "pular blocos" que o SORTKEY faria no Redshift. Ler custo
-- estimado x tempo real aqui e o mesmo musculo do EXPLAIN do Redshift.
EXPLAIN ANALYZE
SELECT category, SUM(gmv_aprovado) AS gmv
FROM analytics.merchant_daily
WHERE dt = :'dia'
GROUP BY category;
