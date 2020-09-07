/* RECOMPUTE INTERACTIONS */

/* DROP */
drop table if exists tests_es.dacc_cops_interactions;

/*CREATE AS SELECT*/
create table if not exists tests_es.dacc_cops_interactions as
select
x_msisdn_original as msisdn,
x_identification as ident,
create_date as call_date,
bucket as tipology,
sub_bucket as sub_tipology
from raw_es.callcentrecalls_interactionvf
left join tests_es.dacc_cops_interactions_buckets
on callcentrecalls_interactionvf.reason_1 = dacc_cops_interactions_buckets.int_tipo
and callcentrecalls_interactionvf.reason_2 = dacc_cops_interactions_buckets.int_subtipo
and callcentrecalls_interactionvf.reason_3 = dacc_cops_interactions_buckets.int_razon
and callcentrecalls_interactionvf.result_td = dacc_cops_interactions_buckets.int_resultado
where direction in ('De entrada')
and type_td in ('Llamada')
and x_msisdn_original is not null
and x_msisdn_original != ''
and bucket is not null
and x_workgroup not like 'BO%'
and x_workgroup not like 'IVR%'
and x_workgroup not like 'SIVA%'
and x_workgroup not like 'B.O%'
and stack = 'VF';
