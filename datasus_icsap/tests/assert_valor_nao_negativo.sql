-- Falha se alguma internação tiver valor pago negativo
select num_aih, valor_total
from {{ ref('fct_internacoes') }}
where valor_total < 0