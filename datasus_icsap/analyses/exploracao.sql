--1. Quantas internações houve por mês?

select mes, 
       count(*) as total_internacoes
from datasus_raw.internacoes
group by mes 
order by mes asc;

--2. Qual o valor total pago no ano?

select ano,
       cast(sum(cast(val_tot as double)) as decimal(18,2)) as total_pago
from datasus_raw.internacoes
group by ano;


--3. Quais são os 10 diagnósticos mais comuns, com o nome da doença?

select d.descrabrev,
       count(i.diag_princ) as total_diagnostico
       
from datasus_raw.internacoes i

inner join datasus_raw.doencas d
  on i.diag_princ = d.subcat

group by 1
order by total_diagnostico desc
limit 10;


--4. Quantas internações ficam sem hospital no join?

select h.cnes,
       h.nome_estabelecimento,
       count(i.n_aih) as total_internacoes
from datasus_raw.internacoes i

left join datasus_raw.hospitais h
   on i.cnes = h.cnes

where h.nome_estabelecimento is null

group by 1, 2;


--5. Quantas internações são evitáveis, e quanto custaram?

select count(i.n_aih) as total_internacoes,
       cast(sum(cast(val_tot as double)) as decimal(18,2)) as total_gasto
       
from datasus_raw.internacoes i 

inner join datasus_raw.internacoes_evitaveis_lista iv
   on i.diag_princ = iv.subcat;

--6. O que significam ident, morte, idade e cod_idade?

SELECT ident,
       morte,  
       cod_idade 
FROM datasus_raw.internacoes
WHERE mes = 1
GROUP BY 1, 2, 3
order by cod_idade asc;