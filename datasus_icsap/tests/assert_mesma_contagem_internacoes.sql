-- Falha se o join duplicar ou perder internações
with stg as (
    select count(*) as linhas from {{ ref('stg_internacoes') }}
),
enriq as (
    select count(*) as linhas from {{ ref('int_internacoes_enriquecidas') }}
)
select stg.linhas as linhas_stg, enriq.linhas as linhas_enriq
from stg cross join enriq
where stg.linhas <> enriq.linhas