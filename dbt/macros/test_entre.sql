-- Teste GENERICO com argumentos: a mesma regra "fracao entre minimo e
-- maximo" serve pra taxa_aprovacao, share_pix e qualquer coluna futura,
-- sem copiar SQL. Convencao do dbt: retornar linhas = falhar.
{% test entre(model, column_name, minimo, maximo) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ minimo }} or {{ column_name }} > {{ maximo }})
{% endtest %}
