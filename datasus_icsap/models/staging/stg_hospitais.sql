select
    cnes                                        as cod_hospital,
    nome_estabelecimento                        as nome_hospital,
    municipio                                   as municipio_hospital,
    ds_tipo_unidade                             as tipo_unidade,
    desc_natureza_juridica                      as natureza_juridica,
    try_cast(leitos_existentes as integer)      as leitos_total,
    try_cast(leitos_sus as integer)             as leitos_sus
from {{ source('datasus_raw', 'hospitais') }}