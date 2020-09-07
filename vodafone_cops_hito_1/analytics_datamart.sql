/* QUERY DE CREACIÓN DEL PANEL A FECHA DE 13/03/2018 */
drop table if exists tests_es.dacc_cops_datamart;
create table if not exists tests_es.dacc_cops_datamart as
with
billing_cycle_ids as
    (
    select
    distinct cast(postpago.year as int) * 10000 + cast(postpago.month as int) * 100 + cast(postpago.x_dia_emision as int) as billing_cycle_id
    from raw_es.vf_pos_ac_final postpago
    ),
msisdn_billing_cycle_posibilities as 
    (
    select billing_cycle_ids.*,
    x_id_red as msisdn,
    postpago.x_num_ident as ident
    from billing_cycle_ids left join raw_es.vf_pos_ac_final postpago
    on billing_cycle_ids.billing_cycle_id = cast(postpago.year as int) * 10000 + cast(postpago.month as int) * 100 + cast(postpago.x_dia_emision as int)
    ),
interactions_billing_cycle as
    (
    select
    interactions.msisdn,
    interactions.ident,
    interactions.tipology,
    case
    when day(interactions.call_date) >=  cast(postpago.x_dia_emision as int) 
    then
    year(interactions.call_date) * 10000 + month(interactions.call_date) * 100 + cast(postpago.x_dia_emision as int)
    when day(interactions.call_date) < cast(postpago.x_dia_emision as int)
    then
        case
        when month(interactions.call_date) - 1 = 0
        then (year(interactions.call_date) - 1) * 10000 + 12 * 100 + cast(postpago.x_dia_emision as int)
        else
        year(interactions.call_date) * 10000 + (month(interactions.call_date) - 1) * 100 + cast(postpago.x_dia_emision as int)
        end
    else null
    end as billing_cycle_id
    from tests_es.dacc_cops_interactions interactions
    inner join raw_es.vf_pos_ac_final postpago
    on postpago.x_id_red = interactions.msisdn
    and postpago.x_num_ident = interactions.ident
    and postpago.year = year(interactions.call_date)
    and postpago.month = month(interactions.call_date)
    ),
interactions_agg as
    (
    select
    msisdn,
    ident,
    billing_cycle_id,
    count(case tipology when 'Billing - Postpaid' then 1 else null end) as n_calls_billing_c,
    count(case tipology when 'Churn/Cancellations' then 1 else null end) as n_calls_churn_c,
    count(case tipology when 'Tariff management' then 1 else null end) as n_calls_tariff_c,
    count(case tipology when 'DSL/FIBER incidences and support' then 1 else null end) as n_calls_dsl_inc_c,
    count(case tipology when 'Voice and mobile data incidences and support' then 1 else null end) as n_calls_mobile_inc_c,
    count(case tipology when 'Device upgrade' then 1 else null end) as n_calls_device_upgr_c,
    count(case tipology when 'Device delivery/repair' then 1 else null end) as n_calls_device_del_rep_c,
    count(case tipology when 'New adds process' then 1 else null end) as n_calls_new_adds_c,
    count(case tipology when 'Product and Service management' then 1 else null end) as n_calls_ser_man_c
    from interactions_billing_cycle
    group by 
    msisdn, 
    billing_cycle_id, 
    ident
    ),
panel_calls as
    (
    select
    msisdn_billing_cycle_posibilities.msisdn as msisdn,
    msisdn_billing_cycle_posibilities.ident as ident,
    msisdn_billing_cycle_posibilities.billing_cycle_id as billing_cycle_id,
    coalesce(interactions_agg.n_calls_billing_c, 0) as n_calls_billing_c,
    lag(coalesce(interactions_agg.n_calls_billing_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_billing_c_minus_1,
    lead(coalesce(interactions_agg.n_calls_billing_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_billing_c_plus_1,
    coalesce(interactions_agg.n_calls_churn_c, 0) as n_calls_churn_c,
    lag(coalesce(interactions_agg.n_calls_churn_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_churn_c_minus_1,
    lead(coalesce(interactions_agg.n_calls_churn_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_churn_c_plus_1,
    coalesce(interactions_agg.n_calls_tariff_c, 0) as n_calls_tariff_c,
    lag(coalesce(interactions_agg.n_calls_tariff_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_tariff_c_minus_1,
    lead(coalesce(interactions_agg.n_calls_tariff_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_tariff_c_plus_1,
    coalesce(interactions_agg.n_calls_dsl_inc_c, 0) as n_calls_dsl_inc_c,
    lag(coalesce(interactions_agg.n_calls_dsl_inc_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_dsl_inc_c_minus_1,
    lead(coalesce(interactions_agg.n_calls_dsl_inc_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_dsl_inc_c_plus_1,
    coalesce(interactions_agg.n_calls_mobile_inc_c, 0) as n_calls_mobile_inc_c,
    lag(coalesce(interactions_agg.n_calls_mobile_inc_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_mobile_inc_c_minus_1,
    lead(coalesce(interactions_agg.n_calls_mobile_inc_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_mobile_inc_c_plus_1,
    coalesce(interactions_agg.n_calls_device_upgr_c, 0) as n_calls_device_upgr_c,
    lag(coalesce(interactions_agg.n_calls_device_upgr_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_device_upgr_c_minus_1,
    lead(coalesce(interactions_agg.n_calls_device_upgr_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_device_upgr_c_plus_1,
    coalesce(interactions_agg.n_calls_device_del_rep_c, 0) as n_calls_device_del_rep_c,
    lag(coalesce(interactions_agg.n_calls_device_del_rep_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_device_del_rep_c_minus_1,
    lead(coalesce(interactions_agg.n_calls_device_del_rep_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_device_del_rep_c_plus_1,
    coalesce(interactions_agg.n_calls_new_adds_c, 0) as n_calls_new_adds_c,
    lag(coalesce(interactions_agg.n_calls_new_adds_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_new_adds_c_minus_1,
    lead(coalesce(interactions_agg.n_calls_new_adds_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_new_adds_c_plus_1,
    coalesce(interactions_agg.n_calls_ser_man_c, 0) as n_calls_ser_man_c,
    lag(coalesce(interactions_agg.n_calls_ser_man_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_ser_man_c_minus_1,
    lead(coalesce(interactions_agg.n_calls_ser_man_c, 0),1) over (partition by msisdn_billing_cycle_posibilities.msisdn, msisdn_billing_cycle_posibilities.ident order by msisdn_billing_cycle_posibilities.billing_cycle_id) as n_calls_ser_man_c_plus_1
    from
    msisdn_billing_cycle_posibilities left join interactions_agg
    on msisdn_billing_cycle_posibilities.msisdn = interactions_agg.msisdn
    and msisdn_billing_cycle_posibilities.ident = interactions_agg.ident
    and msisdn_billing_cycle_posibilities.billing_cycle_id = interactions_agg.billing_cycle_id
    ),
panel_calls_plans as
    (
    select 
    panel_calls.*,
    postpago.x_plan as voice_plan_c,
    postpago.plandatos as data_plan_c,
    case postpago.x_plan when lag(postpago.x_plan) over (partition by panel_calls.msisdn, panel_calls.ident order by panel_calls.billing_cycle_id) then 0 else 1 end as voice_plan_change,
    case postpago.plandatos when lag(postpago.plandatos) over (partition by panel_calls.msisdn, panel_calls.ident order by panel_calls.billing_cycle_id) then 0 else 1 end as data_plan_change
    from 
    panel_calls
    left join raw_es.vf_pos_ac_final postpago
    on postpago.x_id_red = panel_calls.msisdn
    and postpago.x_num_ident = panel_calls.ident
    and panel_calls.billing_cycle_id = cast(postpago.year as int) * 10000 + cast(postpago.month as int) * 100 + cast(postpago.x_dia_emision as int)
    ),
panel_calls_plans_promos as
    (
    select
    panel_calls_plans.*,
    case when postpago.promocion_vf = '' then null else postpago.promocion_vf end as promo_code_vf,
    case when cast(postpago.meses_fin_cp_vf as int) = 9999 then null else cast(postpago.meses_fin_cp_vf as int) end as months_to_end_promo_vf,
    case when postpago.promocion_tarifa = '' then null else postpago.promocion_tarifa end as promo_code_tarif,
    case when cast(postpago.meses_fin_cp_tarifa as int) = 9999 then null else cast(postpago.meses_fin_cp_tarifa as int) end as months_to_end_promo_tarif
    from 
    panel_calls_plans
    left join raw_es.vf_pos_ac_final postpago
    on postpago.x_id_red = panel_calls_plans.msisdn
    and postpago.x_num_ident = panel_calls_plans.ident
    and panel_calls_plans.billing_cycle_id = cast(postpago.year as int) * 10000 + cast(postpago.month as int) * 100 + cast(postpago.x_dia_emision as int)
    ),
panel_calls_plans_promos_socio as
    (
    select
    panel_calls_plans_promos.*,
    case postpago.codigo_postal
    when '' then null
    when ' ' then null
    else postpago.codigo_postal
    end as zip_code,
    case substring(codigo_postal, 1, 2)
    when ' ' then null
    when '' then null
    else substring(codigo_postal, 1, 2)
    end as region_code,
    case
    when postpago.x_sexo = 'Mujer' then 'Mujer'
    when postpago.x_sexo = 'Varón' then 'Varón'
    when postpago.x_sexo = 'Hombre' then 'Varón'
    when postpago.x_sexo = 'H' then 'Varón'
    when postpago.x_sexo = 'M' then 'Mujer'
    when postpago.x_sexo = 'Var�n' then 'Varón'
    when postpago.x_sexo = 'Varn' then 'Varón'
    when postpago.x_sexo = '' then null
    when postpago.x_sexo = '...' then null
    else null 
    end as gender,
    postpago.x_tipo_ident as type_ident,
    case postpago.x_nacionalidad
        when '' then null
        when '...' then null
        when 'Arabia Saud�' then 'Arabia Saudí'
        when 'Arabia Saud' then 'Arabia Saudí'
        when 'B�lgica' then 'Bélgica'
        when 'Blgica' then 'Bélgica'
        when 'Camern' then 'Camerún'
        when 'Camer�n' then 'Camerún'
        when 'Espaa' then 'España'
        when 'Espa�a' then 'España'
        when 'Fed.Rusia' then 'Fed.Rusa'
        when 'Gran Bretaa' then 'Gran Bretaña'
        when 'Gran Breta�a' then 'Gran Bretaña'
        when 'Guin Ecuatorial' then 'Guinea Ecuatorial'
        when 'Guinea Ecuatori' then 'Guinea Ecuatorial'
        when 'Irn' then 'Irán'
        when 'Ir�n' then 'Irán'
        when 'Is.Vrgenes Bri' then 'Islas Vírgenes Británicas'
        when 'Is.Vírg. Americ' then 'Islas Vírgenes Americanas'
        when 'Is.Vírgenes Bri' then 'Islas Vírgenes Británicas'
        when 'Is.V�rgenes Bri' then 'Islas Vírgenes Británicas'
        when 'Japn' then 'Japón'
        when 'Jap�n' then 'Japón'
        when 'Lbano' then 'Líbano'
        when 'L�bano' then 'Líbano'
        when 'Mjico' then 'Méjico'
        when 'M�jico' then 'Méjico'
        when 'Mnaco' then 'Mónaco'
        when 'M�naco' then 'Mónaco'
        when 'Nger' then 'Níger'
        when 'N�ger' then 'Níger'
        when 'Pakistn' then 'Pakistán'
        when 'Pakist�n' then 'Pakistán'
        when 'Pakistan' then 'Pakistán'
        when 'Panam' then 'Panamá'
        when 'Panam�' then 'Panamá'
        when 'Per' then 'Perú'
        when 'Per�' then 'Perú'
        when 'Reunin' then 'Reunión'
        when 'Reuni�n' then 'Reunión'
        when 'S.Tom, Princip' then 'S.Tomé y Príncipe'
        when 'S.Tomé, Princip' then 'S.Tomé y Príncipe'
        when 'S.Tom�, Princip' then 'S.Tomé y Príncipe'
        when 'Sudn' then 'Sudán'
        when 'Sud�n' then 'Sudán'
        when 'Sudfrica' then 'Sudáfrica'
        when 'Sud�frica' then 'Sudáfrica'
        when 'Turqua' then 'Turquía'
        when 'Turqu�a' then 'Turquía'
        when 'Tnez' then 'Túnez'
        when 'T�nez' then 'Túnez'
        when 'Antartica' then null
        when 'Antarctica' then null
        else postpago.x_nacionalidad
    end as nationality,
    case
    when year(current_timestamp()) - cast(x_fecha_nacimiento as int) > 100 then null
    when year(current_timestamp()) - cast(x_fecha_nacimiento as int) < 0 then null
    else year(current_timestamp()) - cast(x_fecha_nacimiento as int)
    end as age
    from 
    panel_calls_plans_promos
    left join raw_es.vf_pos_ac_final postpago
    on postpago.x_id_red = panel_calls_plans_promos.msisdn
    and postpago.x_num_ident = panel_calls_plans_promos.ident
    and panel_calls_plans_promos.billing_cycle_id = cast(postpago.year as int) * 10000 + cast(postpago.month as int) * 100 + cast(postpago.x_dia_emision as int)
    ),
panel_calls_plans_promos_socio_products as
    (
    select
    panel_calls_plans_promos_socio.*,
    cast(postpago.num_pospago as int) as n_lines_post,
    cast(postpago.num_prepago as int) as n_lines_pre,
    cast(postpago.num_total as int) as n_lines
    from 
    panel_calls_plans_promos_socio
    left join raw_es.vf_pos_ac_final postpago
    on postpago.x_id_red = panel_calls_plans_promos_socio.msisdn
    and postpago.x_num_ident = panel_calls_plans_promos_socio.ident
    and panel_calls_plans_promos_socio.billing_cycle_id = cast(postpago.year as int) * 10000 + cast(postpago.month as int) * 100 + cast(postpago.x_dia_emision as int)
    )
select *
from panel_calls_plans_promos_socio_products
where n_calls_billing_c_minus_1 is not null
and n_calls_billing_c_plus_1 is not null
and billing_cycle_id >= 20170101
and cast(billing_cycle_id as string) not like '%12';
