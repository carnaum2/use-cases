from churn_nrt.src.data.netscout_data import NetscoutData
from churn_nrt.src.data.tickets import Tickets
from churn_nrt.src.data.geneva_data import GenevaData
from churn_nrt.src.data.billing import Billing
from churn_nrt.src.data.spinners import Spinners
from churn_nrt.src.data.services_data import Service
from churn_nrt.src.data.ccc import CCC
from churn_nrt.src.data.calls_comps_data import CallsCompData
from churn_nrt.src.data.customer_base import CustomerBase
from churn_nrt.src.utils.date_functions import get_diff_days

from pyspark.sql.functions import avg as sql_avg, col


def get_segment_variables(spark, df_app_segment, closing_day):

    '''
      This function takes basic variables from different modules in order to make a profile of the different segments

      df_app_segment: dataframe of customers with the column app_segment (obtained from get_app_segmentation function: app_segmentation_days_since.py)
      closing_day: date for analysis

      The function computes a structure with the following fields:

          - msisdn
          - num_cliente
          - nif_cliente
          - different basic attributes selected: tgs_days_until_f_fin_bi, nb_rgus_nif...
          '''

    days = get_diff_days(closing_day[:6] + '01', closing_day) #number of days of the month (we calculate attributes for one month)

    print("[Info] Loading different modules and selecting variables of interest for closing_day={}".format(closing_day))

    customer = CustomerBase(spark).get_module(closing_day, save_others=True, force_gen=False, add_tgs=True) \
        .select('msisdn', 'tgs_days_until_f_fin_bi', 'nb_rgus_nif', 'nb_fixed_services_nif', 'nb_fbb_services_nif',
                'nb_mobile_services_nif', 'nb_tv_services_nif')

    geneva = GenevaData(spark, incremental_period=days).get_module(closing_day, save_others=True, force_gen=False) \
        .select('msisdn', 'gnv_total_calls', 'gnv_perc_calls_competitors', 'gnv_total_mou', 'gnv_num_distinct_comps')

    service = Service(spark).get_module(closing_day, save_others=True, force_gen=False) \
        .select('msisdn', 'PRICE_TARIFF', 'PRICE_DATA')

    ccc = CCC(spark).get_module(closing_day, save_others=True, force_gen=False) \
        .select('msisdn', 'num_calls_w4')

    calls_comp = CallsCompData(spark, window_length=days).get_module(closing_day, save_others=True, force_gen=False) \
        .select('msisdn', 'num_distinct_comps', 'sum_distinct_days_with_calls')

    spinners = Spinners(spark).get_module(closing_day, save_others=True, force_gen=False) \
        .select('nif_cliente', 'num_distinct_operators', 'total_acan', 'nif_port_freq_per_msisdn')

    billing = Billing(spark).get_module(closing_day, save_others=True, force_gen=False) \
        .select('NUM_CLIENTE', 'billing_mean', 'Bill_N1_Amount_To_Pay')

    tickets = Tickets(spark).get_module(closing_day, save_others=True, force_gen=False) \
        .select('NIF_CLIENTE', 'num_tickets_tipo_tramitacion_w4', 'num_tickets_tipo_averia_w4',
                'num_tickets_tipo_reclamacion_w4') \
        .withColumnRenamed('NIF_CLIENTE', 'nif_cliente')

    netscout = NetscoutData(spark, incremental_period=15).get_module(closing_day, save_others=True,
                                                                            force_gen=False, option='DIGITAL')

    print("[Info] Joining variables from different modules")

    df_join = df_app_segment.join(customer, on='msisdn', how='left')
    df_join = df_join.join(netscout, on='msisdn', how='left')
    df_join = df_join.join(geneva, on='msisdn', how='left')
    df_join = df_join.join(service, on='msisdn', how='left')
    df_join = df_join.join(ccc, on='msisdn', how='left')
    df_join = df_join.join(calls_comp, on='msisdn', how='left')
    df_join = df_join.join(spinners, on='nif_cliente', how='left')
    df_join = df_join.join(tickets, on='nif_cliente', how='left')
    df_join = df_join.join(billing, on='NUM_CLIENTE', how='left')

    df_join=df_join.drop_duplicates(subset=['msisdn'])

    return df_join


def get_segment_profile(df): #excluir los -1 y sustituir None por 0 antes de calcular las medias

    df = df.fillna(0)

    days_bi = df.filter(col('tgs_days_until_f_fin_bi')!=-1).groupBy("app_segment").agg(sql_avg('tgs_days_until_f_fin_bi').alias('avg_days_until_fin_bi'))

    days_bi.show()

    print "[Info] Average days_until_f_fin_bi above"

    nb_rgu = df.groupBy("app_segment").agg(sql_avg('nb_rgus_nif').alias('avg_nb_rgus_nif'))

    nb_rgu.show()

    print "[Info] Average number of products above"

    nb_fixed = df.groupBy("app_segment").agg(sql_avg('nb_fixed_services_nif').alias('avg_nb_fixed_services_nif'))

    nb_fixed.show()

    print "[Info] Average number of fixed products above"

    nb_fbb = df.groupBy("app_segment").agg(sql_avg('nb_fbb_services_nif').alias('avg_nb_fbb_services_nif'))

    nb_fbb.show()

    print "[Info] Average number of fbb products above"

    nb_mobile = df.groupBy("app_segment").agg(sql_avg('nb_mobile_services_nif').alias('avg_nb_mobile_services_nif'))

    nb_mobile.show()

    print "[Info] Average number of mobile products above"

    nb_tv = df.groupBy("app_segment").agg(sql_avg('nb_tv_services_nif').alias('avg_nb_tv_services_nif'))

    nb_tv.show()

    print "[Info] Average number of TV products above"

    data_social = df.groupBy("app_segment").agg(sql_avg('social_networks').alias('avg_social_networks'))

    data_social.show()

    print "[Info] Average data volume used in social networks above"

    data_google = df.groupBy("app_segment").agg(sql_avg("google_apps").alias("avg_google_apps"))

    data_google.show()

    print "[Info] Average data volume used in google apps above"

    data_message = df.groupBy("app_segment").agg(sql_avg("messaging").alias("avg_messaging"))

    data_message.show()

    print "[Info] Average data volume used in message apps above"

    data_internet = df.groupBy("app_segment").agg(sql_avg('internet').alias('avg_internet'))

    data_internet.show()

    print "[Info] Average data volume used in internet above"

    data_travel = df.groupBy("app_segment").agg(sql_avg('travel').alias('avg_travel'))

    data_travel.show()

    print "[Info] Average data volume used in travel above"

    data_music = df.groupBy("app_segment").agg(sql_avg('music').alias('avg_music'))

    data_music.show()

    print "[Info] Average data volume used in music above"

    data_e_commerce = df.groupBy("app_segment").agg(sql_avg('e_commerce').alias('avg_e_commerce'))

    data_e_commerce.show()

    print "[Info] Average data volume used in e-commerce above"

    data_technology = df.groupBy("app_segment").agg(sql_avg('technology').alias('avg_technology'))

    data_technology.show()

    print "[Info] Average data volume used in technology apps above"

    data_bank = df.groupBy("app_segment").agg(sql_avg('bank').alias('avg_bank'))

    data_bank.show()

    print "[Info] Average data volume used in bank apps above"

    data_news = df.groupBy("app_segment").agg(sql_avg('news').alias('avg_news'))

    data_news.show()

    print "[Info] Average data volume used in news above"

    data_streaming = df.groupBy("app_segment").agg(sql_avg('streaming').alias('avg_streaming'))

    data_streaming.show()

    print "[Info] Average data volume used in streaming above"

    data_sports = df.groupBy("app_segment").agg(sql_avg('sports').alias('avg_sports'))

    data_sports.show()

    print "[Info] Average data volume used in sports above"

    nb_calls = df.groupBy("app_segment").agg(sql_avg('gnv_total_calls').alias('avg_gnv_total_calls'))

    nb_calls.show()

    print "[Info] Average number of calls above"

    perc_calls_comp = df.filter(col('gnv_perc_calls_competitors')!=-1).groupBy("app_segment").agg(
        sql_avg('gnv_perc_calls_competitors').alias('avg_gnv_perc_calls_competitors'))

    perc_calls_comp.show()

    print "[Info] Average percentage calls to competitors above"

    total_mou = df.groupBy("app_segment").agg(sql_avg('gnv_total_mou').alias('gnv_total_mou'))

    total_mou.show()

    print "[Info] Average total minutes of calls above"

    num_distinct_comp_calls = df.groupBy("app_segment").agg(
        sql_avg('gnv_num_distinct_comps').alias('avg_gnv_num_distinct_comps'))

    num_distinct_comp_calls.show()

    print "[Info] Average number of distinct competitors calls above"

    price_tariff = df.groupBy("app_segment").agg(sql_avg('PRICE_TARIFF').alias('avg_PRICE_TARIFF'))

    price_tariff.show()

    print "[Info] Average tariff price above"

    price_data = df.groupBy("app_segment").agg(sql_avg('PRICE_DATA').alias('avg_PRICE_DATA'))

    price_data.show()

    print "[Info] Average data price above"

    nb_ccc_w4 = df.groupBy("app_segment").agg(sql_avg('num_calls_w4').alias('avg_num_calls_w4'))

    nb_ccc_w4.show()

    print "[Info] Average number of CCC in the last month above"

    num_distinct_comps = df.groupBy("app_segment").agg(sql_avg('num_distinct_comps').alias('avg_num_distinct_comps'))

    num_distinct_comps.show()

    print "[Info] Average number distinct competitors calls above"

    sum_distinct_days_with_calls = df.groupBy("app_segment").agg(
        sql_avg('sum_distinct_days_with_calls').alias('avg_sum_distinct_days_with_calls'))

    sum_distinct_days_with_calls.show()

    print "[Info] Average number of distinct days with calls to competitors above"

    num_distinct_operators = df.groupBy("app_segment").agg(
        sql_avg('num_distinct_operators').alias('avg_num_distinct_operators'))

    num_distinct_operators.show()

    print "[Info] Average num_distinct_operators spinners above"

    nif_port_freq_per_msisdn = df.groupBy("app_segment").agg(
        sql_avg('nif_port_freq_per_msisdn').alias('avg_nif_port_freq_per_msisdn'))

    nif_port_freq_per_msisdn.show()

    print "[Info] Average nif_port_freq_per_msisdn above"

    total_acan = df.groupBy("app_segment").agg(sql_avg('total_acan').alias('avg_total_acan'))

    total_acan.show()

    print "[Info] Average total_acan above"

    bill_n1_amount = df.groupBy("app_segment").agg(sql_avg('Bill_N1_Amount_To_Pay').alias('avg_Bill_N1_Amount_To_Pay'))

    bill_n1_amount.show()

    print "[Info] Average cost of last bill above"

    billing_mean = df.groupBy("app_segment").agg(sql_avg('billing_mean').alias('avg_billing_mean'))

    billing_mean.show()

    print "[Info] Average billing mean cost above"

    num_tickets_facturacion_w4 = df.groupBy("app_segment").agg(
        sql_avg('num_tickets_tipo_tramitacion_w4').alias('avg_num_tickets_tipo_tramitacion_w4'))

    num_tickets_facturacion_w4.show()

    print "[Info] Average number of billing problems in the last month above"

    num_tickets_averia_w4 = df.groupBy("app_segment").agg(
        sql_avg('num_tickets_tipo_averia_w4').alias('avg_num_tickets_tipo_averia_w4'))

    num_tickets_averia_w4.show()

    print "[Info] Average number of breakdowns in the last month above"

    num_tickets_reclamacion_w4 = df.groupBy("app_segment").agg(
        sql_avg('num_tickets_tipo_reclamacion_w4').alias('avg_num_tickets_tipo_reclamacion_w4'))

    num_tickets_reclamacion_w4.show()

    print "[Info] Average number of claims in the last month above"

    profile_df = days_bi.join(nb_rgu, ['app_segment'], 'inner').join(nb_fixed, ['app_segment'], 'inner') \
        .join(nb_fbb, ['app_segment'], 'inner') \
        .join(nb_mobile, ['app_segment'], 'inner').join(nb_tv, ['app_segment'], 'inner') \
        .join(data_social, ['app_segment'], 'inner').join(data_google, ['app_segment'], 'inner')\
        .join(data_internet, ['app_segment'], 'inner').join(data_message, ['app_segment'], 'inner') \
        .join(data_travel, ['app_segment'], 'inner').join(data_music, ['app_segment'], 'inner') \
        .join(data_e_commerce, ['app_segment'], 'inner').join(data_technology, ['app_segment'], 'inner') \
        .join(data_bank, ['app_segment'], 'inner').join(data_news, ['app_segment'], 'inner') \
        .join(data_streaming, ['app_segment'], 'inner').join(data_sports, ['app_segment'], 'inner') \
        .join(nb_calls, ['app_segment'], 'inner').join(perc_calls_comp, ['app_segment'], 'inner') \
        .join(total_mou, ['app_segment'], 'inner').join(num_distinct_comp_calls, ['app_segment'], 'inner') \
        .join(price_tariff, ['app_segment'], 'inner').join(price_data, ['app_segment'], 'inner') \
        .join(nb_ccc_w4, ['app_segment'], 'inner').join(num_distinct_comps, ['app_segment'], 'inner') \
        .join(sum_distinct_days_with_calls, ['app_segment'], 'inner').join(num_distinct_operators, ['app_segment'],
                                                                           'inner') \
        .join(nif_port_freq_per_msisdn, ['app_segment'], 'inner').join(total_acan, ['app_segment'], 'inner') \
        .join(bill_n1_amount, ['app_segment'], 'inner').join(billing_mean, ['app_segment'], 'inner') \
        .join(num_tickets_facturacion_w4, ['app_segment'], 'inner').join(num_tickets_averia_w4, ['app_segment'],
                                                                         'inner') \
        .join(num_tickets_reclamacion_w4, ['app_segment'], 'inner')

    return profile_df