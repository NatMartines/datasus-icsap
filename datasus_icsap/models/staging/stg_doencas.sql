select
    subcat          as cod_cid,
    descricao       as nome_doenca,
    descrabrev      as nome_doenca_abrev
from {{ source('datasus_raw', 'doencas') }}