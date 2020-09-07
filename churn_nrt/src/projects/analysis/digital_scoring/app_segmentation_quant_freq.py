from churn_nrt.src.data.myvf_data import MyVFdata

from churn_nrt.src.utils.date_functions import get_diff_days
from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter
from churn_nrt.src.data.customer_base import CustomerBase
from churn_nrt.src.data_utils.base_filters import keep_active_services
from pyspark.sql.functions import col, when


def get_app_segmentation(spark, start_dt, closing_day):

    '''
      This function make a segmentation based on quantity and frequency
      It considers that the customer has used the app at least one time before the period of analysis

      start_dt: starting date of the time window considered for the segmentation
      closing_day: ending date of the time window considered for the segmentation

      The function computes a structure with the following fields:

          - msisdn
          - num_cliente
          - nif_cliente
          ...

      '''

    days=get_diff_days(start_dt,closing_day)

    print("[Info] Days between closing_day={} start_dt={}: {} days".format(closing_day, start_dt,days))

    print("[Info] MyVFdata for closing_day={} days_range={}".format(closing_day, days))

    df_vyvf = MyVFdata(spark, platform='app', version=2).get_module(closing_day, save_others=True,
                                                                    force_gen=False, days_range=[days, 365])

    print("[Info] Customer base for start_dt={} filtered by non recent customers".format(start_dt))

    df_recent = get_non_recent_customers_filter(spark, start_dt, 90,
                                                level='msisdn')


    print("[Info] Customer base for closing_day={} filtered by rgu = mobile".format(closing_day))

    current_base_df = CustomerBase(spark).get_module(closing_day, save=True,
                                    save_others=True, add_tgs=False).filter(col("rgu") == "mobile")

    current_base_df=current_base_df.select('msisdn', 'nif_cliente','NUM_CLIENTE').distinct()


    print("[Info] Customer base for closing_day={}".format(closing_day) + " filtered by active services")

    current_base_df = keep_active_services(current_base_df)


    print("[Info] Inner join customer base for closing_day={} and start_dt={}".format(closing_day, start_dt))

    final_base = current_base_df.join(df_recent, ['msisdn'], 'inner').select('msisdn','nif_cliente','NUM_CLIENTE').distinct()

    print("[Info] Total number of customers={}".format(final_base.count()))

    print("[Info] Inner join final base and myvf app base")

    app_users=final_base.join(df_vyvf, ['msisdn'],'inner')
    #SAVED BOTH FILES IN: /data/udf/vf_es/churn/digital/final_base_20200801   #/data/udf/vf_es/churn/digital/app_20200801

    print("[Info] Total number of app users={}".format(app_users.count()))

    print("[Info] Total number of app users whose first access was before {} = {}".format(days, app_users.filter(col('myvf_1st_navig_last365')>days).count()))

    print("[Info] Apply app users filters: first access before {}".format(start_dt))

    app_users_final=app_users.filter(col('myvf_1st_navig_last365')>days)

    app_users_final2 = app_users_final.withColumn('quantity', col('myvf_nb_days_access_last' + str(days)) / days-1) \
        .withColumn('frequency', (col('myvf_1st_navig_last' + str(days)) -
                                  col('myvf_last_navig_last' + str(days))) / days)

    app_users_final2 = app_users_final2.select('msisdn', 'nif_cliente',
                                               'NUM_CLIENTE','quantity', 'frequency')

    print("[Info] Calculating segmentation")

    app_users_filtered=app_users_final2.filter((col('quantity')!=0)&(col('frequency')!=0))

    m1 = app_users_filtered.approxQuantile("frequency", [0.5], 0.25)[0]
    m2=app_users_filtered.approxQuantile("quantity", [0.5], 0.25)[0]

    app_users_segment = app_users_final2.withColumn('app_segment',
                                                    when((col('frequency') == 0) & (col('quantity') == 0), 'no_user') \
                                                    .otherwise(when((col('frequency') <= m1) & (col('quantity') <= m2),
                                                                    'light_user') \
                                                               .otherwise(
                                                        when((col('frequency') > m1) & (col('quantity') <= m2),
                                                             'occasional_user')
                                                        .otherwise(
                                                            when((col('frequency') <= m1) & (col('quantity') > m2),
                                                                 'short_period_user')
                                                            .otherwise(
                                                                when((col('frequency') > m1) & (col('quantity') > m2),
                                                                     'heavy_user'))))))

    print("[Info] App users segmentation:")

    app_users_segment.groupby('app_segment').count().orderBy('app_segment').show()

    print("[Info] Save app segmentation for closing_day={}".format(closing_day))
    app_users_segment = app_users_segment.select('msisdn', 'nif_cliente', 'NUM_CLIENTE', 'app_segment')

    app_users_segment.repartition(300).write.save(
        '/data/udf/vf_es/churn/digital/app_segments_freq_quant' + closing_day, format="parquet", mode="overwrite")

    return app_users_segment