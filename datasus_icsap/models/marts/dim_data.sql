with datas as (
    select date_add('day', n, date '2023-01-01') as data
    from unnest(sequence(0, date_diff('day', date '2023-01-01', date '2024-12-31'))) as t(n)
)

select
    data,
    year(data)                          as ano,
    quarter(data)                       as trimestre,
    month(data)                         as mes,
    case month(data)
        when 1 then 'Janeiro'   when 2 then 'Fevereiro' when 3 then 'Março'
        when 4 then 'Abril'     when 5 then 'Maio'      when 6 then 'Junho'
        when 7 then 'Julho'     when 8 then 'Agosto'    when 9 then 'Setembro'
        when 10 then 'Outubro'  when 11 then 'Novembro' when 12 then 'Dezembro'
    end                                 as nome_mes,
    day(data)                           as dia,
    day_of_week(data)                   as dia_semana,   -- 1 = segunda ... 7 = domingo
    case day_of_week(data)
        when 1 then 'Segunda' when 2 then 'Terça'  when 3 then 'Quarta'
        when 4 then 'Quinta'  when 5 then 'Sexta'  when 6 then 'Sábado'
        when 7 then 'Domingo'
    end                                 as nome_dia_semana,
    day_of_week(data) in (6, 7)         as eh_fim_de_semana
from datas