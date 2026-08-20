-- Gold minima e deterministica pro estagio dbt do CI: linhas que respeitam
-- o contrato dos testes (chave unica, categorias do gerador, fracoes em
-- [0,1]) mais o caso de borda real -- merchant sem dia anterior fica com
-- variacao_gmv_pct NULL ("nulo e o comportamento certo, zero mentiria",
-- tests/test_gold_sql.py). Roda depois da DDL de
-- scripts/redshift_pg/ddl_postgres.sql, que cria schema e tabela vazios.
INSERT INTO analytics.merchant_daily
    (dt, merchant_id, merchant_name, category, state, tx_total, tx_aprovadas,
     clientes_unicos, taxa_aprovacao, gmv_aprovado, valor_estornado,
     ticket_medio, share_pix, rank_categoria, gmv_dia_anterior, variacao_gmv_pct)
VALUES
    ('2026-07-01', 'm-001', 'Padaria Aurora',   'alimentacao', 'SP', 120, 110, 95, 0.9167, 5500.00, 120.00,  50.00, 0.4500, 1, NULL,    NULL),
    ('2026-07-01', 'm-002', 'Tur Horizonte',    'viagem',      'RJ',  40,  30, 28, 0.7500, 9000.00, 300.00, 300.00, 0.2000, 1, NULL,    NULL),
    ('2026-07-01', 'm-003', 'Vestuario Prisma', 'varejo',      'MG',  80,  72, 60, 0.9000, 3600.00,  80.00,  50.00, 0.5100, 1, NULL,    NULL),
    ('2026-07-02', 'm-001', 'Padaria Aurora',   'alimentacao', 'SP', 130, 117, 99, 0.9000, 6200.00, 100.00,  52.99, 0.4700, 1, 5500.00, 0.1273),
    ('2026-07-02', 'm-002', 'Tur Horizonte',    'viagem',      'RJ',  35,  28, 25, 0.8000, 8400.00, 250.00, 300.00, 0.2100, 1, 9000.00, -0.0667),
    ('2026-07-02', 'm-004', 'Clinica Vitali',   'saude',       'PR',  25,  24, 22, 0.9600, 4800.00,   0.00, 200.00, 0.3000, 1, NULL,    NULL);
