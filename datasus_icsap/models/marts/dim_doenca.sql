with doencas as (
    select * from {{ ref('stg_doencas') }}
),
categorias as (
    select * from {{ ref('stg_doencas_categorias') }}
),
grupos as (
    select * from {{ ref('stg_doencas_grupos') }}
),
capitulos as (
    select * from {{ ref('stg_doencas_capitulos') }}
),
evitaveis as (
    select * from {{ ref('stg_internacoes_evitaveis') }}
)

select
    d.cod_cid,
    d.nome_doenca,
    d.nome_doenca_abrev,
    c.cod_categoria,
    c.nome_categoria,
    g.nome_grupo,
    cap.num_capitulo,
    cap.nome_capitulo,

    case when e.cod_cid is not null then 1 
         else 0 
    end as eh_evitavel,
    
    e.num_grupo_icsap,
    e.nome_grupo_icsap

from doencas d

left join categorias c   
   on c.cod_cid = d.cod_cid

left join grupos g 
   on g.cod_cid = d.cod_cid

left join capitulos cap 
   on cap.cod_cid = d.cod_cid

left join evitaveis e   
   on e.cod_cid   = d.cod_cid