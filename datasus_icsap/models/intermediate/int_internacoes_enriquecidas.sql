with internacoes as (
    select * from {{ ref('stg_internacoes') }}
),
municipios as (
    select * from {{ ref('stg_municipios') }}
),
hospitais as (
    select * from {{ ref('stg_hospitais') }}
),
doencas as (
    select * from {{ ref('stg_doencas') }}
),
capitulos as (
    select * from {{ ref('stg_doencas_capitulos') }}
),
evitaveis as (
    select * from {{ ref('stg_internacoes_evitaveis') }}
)

select
    i.*,

    -- onde o paciente mora
    m.nome_municipio                as municipio_residencia,
    m.regiao_imediata               as regiao_residencia,

    -- onde foi internado
    h.nome_hospital,
    h.tipo_unidade,

    -- doença
    d.nome_doenca,
    c.nome_capitulo,

    -- evitável?
    case when e.cod_cid is not null then 1 else 0 end   as eh_evitavel,
    e.nome_grupo_icsap                                  as grupo_evitavel,

    -- faixa etária
    case
        when i.idade_anos < 5  then '00 a 04'
        when i.idade_anos < 15 then '05 a 14'
        when i.idade_anos < 30 then '15 a 29'
        when i.idade_anos < 45 then '30 a 44'
        when i.idade_anos < 60 then '45 a 59'
        when i.idade_anos < 75 then '60 a 74'
        else '75 ou mais'
    end                                                 as faixa_etaria

from internacoes i
left join municipios m on m.cod_municipio = i.cod_municipio_residencia
left join hospitais  h on h.cod_hospital  = i.cod_hospital
left join doencas    d on d.cod_cid       = i.cod_cid_principal
left join capitulos  c on c.cod_cid       = i.cod_cid_principal
left join evitaveis  e on e.cod_cid       = i.cod_cid_principal