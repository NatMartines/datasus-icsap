select
    subcat          as cod_cid,
    cat             as cod_categoria,
    categoria       as nome_categoria
from {{ source('datasus_raw', 'doencas_categorias') }}