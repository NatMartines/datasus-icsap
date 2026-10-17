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
),

-- doenças com 4 caracteres (ex.: J189)
subcategorias as (
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
        end   as eh_evitavel,

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
       on e.cod_cid = d.cod_cid
),

-- categorias com 3 caracteres (ex.: J18), usadas quando o médico não detalhou
so_categorias as (
    select distinct
        c.cod_categoria                 as cod_cid,
        c.nome_categoria                as nome_doenca,
        c.nome_categoria                as nome_doenca_abrev,
        c.cod_categoria,
        c.nome_categoria,
        g.nome_grupo,
        cap.num_capitulo,
        cap.nome_capitulo,
        0                               as eh_evitavel,
        cast(null as integer)           as num_grupo_icsap,
        cast(null as varchar)           as nome_grupo_icsap

    from categorias c

    left join grupos g   
       on g.cod_cid = c.cod_cid

    left join capitulos cap 
       on cap.cod_cid = c.cod_cid

    where c.cod_categoria not in (select cod_cid from doencas)
)

select * from subcategorias
union all
select * from so_categorias