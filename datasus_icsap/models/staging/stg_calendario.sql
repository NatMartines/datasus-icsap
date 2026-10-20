with limites as (
    select
        date_trunc('year', min(data_internacao))                                       as data_inicio,
        date_trunc('year', max(data_internacao)) + interval '1' year - interval '1' day as data_fim
    from {{ ref('stg_internacoes') }}
)

select
    date_add('day', n, data_inicio) as data
from limites
cross join unnest(sequence(0, date_diff('day', data_inicio, data_fim))) as t(n)