select
    cod_hospital,
    nome_hospital,
        trim(
        replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
          regexp_replace(lower(nome_hospital), '(^|\s)(\w)', x -> x[1] || upper(x[2])),
          'Hospital Das Clinicas', 'HC'),
          'Hc Da Fmusp', 'HC FMUSP'),
          'HC FMUSP HC', 'HC FMUSP'),
          'Hospital', 'Hosp.'),
          'Instituto', 'Inst.'),
          'Faculdade De Medicina', 'FM'),
          'Sao Paulo', 'SP'),
          ' Da ', ' da '),
          ' De ', ' de '),
          ' Do ', ' do '),
          ' Das ', ' das '),
          ' E ', ' e ')
        ) as nome_hospital_curto,
    municipio_hospital,
    tipo_unidade,
    natureza_juridica,
    leitos_total,
    leitos_sus
from {{ ref('stg_hospitais') }}