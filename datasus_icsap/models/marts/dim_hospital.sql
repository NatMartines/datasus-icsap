select
    cod_hospital,
    nome_hospital,
    municipio_hospital,
    tipo_unidade,
    natureza_juridica,
    leitos_total,
    leitos_sus
from {{ ref('stg_hospitais') }}