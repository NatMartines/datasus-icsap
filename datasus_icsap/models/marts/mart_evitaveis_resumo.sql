with fato as (
    select * from {{ ref('fct_internacoes') }}
),
doencas as (
    select * from {{ ref('dim_doenca') }}
)

select
    f.ano,
    f.mes,
    f.cod_municipio_residencia,
    f.cod_hospital,
    f.eh_evitavel,
    coalesce(d.nome_grupo_icsap, 'Não evitável')   as grupo_evitavel,

    count(*)                    as internacoes,
    sum(f.valor_total)          as valor_total,
    sum(f.dias_permanencia)     as dias_permanencia,
    sum(f.obito)                as obitos

from fato f

left join doencas d 
   on d.cod_cid = f.cod_cid_principal
   
group by 1, 2, 3, 4, 5, 6