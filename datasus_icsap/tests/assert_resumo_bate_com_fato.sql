-- Falha se o total do resumo for diferente do total da fato
with fato as (
    select count(*) as internacoes, 
           sum(valor_total) as valor
    from {{ ref('fct_internacoes') }}
),
resumo as (
    select sum(internacoes) as internacoes, 
           sum(valor_total) as valor
    from {{ ref('mart_evitaveis_resumo') }}
)
select *
from fato 
cross join resumo
where fato.internacoes <> resumo.internacoes
   or fato.valor <> resumo.valor