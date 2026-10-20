select
    date_add('day', n, date '2023-01-01') as data
from unnest(sequence(0, date_diff('day', date '2023-01-01', date '2024-12-31'))) as t(n)