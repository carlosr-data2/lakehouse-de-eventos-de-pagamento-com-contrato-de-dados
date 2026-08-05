-- Reconciliacao entre camadas: bronze deduplicado deve fechar com
-- silver valido + quarentena rejeitado, dia a dia. Se nao fechar,
-- existe dado sumindo em algum lugar do pipeline.
SELECT
    s.dt,
    s.validos,
    q.rejeitados,
    s.validos + q.rejeitados AS total_processado
FROM (SELECT dt, COUNT(*) AS validos FROM silver_events GROUP BY dt) s
FULL OUTER JOIN
     (SELECT dt, COUNT(*) AS rejeitados FROM quarantine_events GROUP BY dt) q
  ON s.dt = q.dt
ORDER BY s.dt;

-- Top motivos de rejeicao no periodo. E a pauta objetiva da conversa com
-- o time que produz o dado na origem.
SELECT reason, COUNT(*) AS ocorrencias
FROM quarantine_events
LATERAL VIEW EXPLODE(rejection_reasons) t AS reason
GROUP BY reason
ORDER BY ocorrencias DESC;

-- Estabilidade da qualidade ao longo do tempo. Base para trocar o limite
-- fixo do quality gate por um limite dinamico baseado no historico.
SELECT dt,
       COUNT(*) AS eventos,
       ROUND(AVG(taxa_aprovacao), 4) AS aprovacao_media,
       ROUND(SUM(gmv_aprovado), 2)   AS gmv_total
FROM gold_merchant_daily
GROUP BY dt
ORDER BY dt;
