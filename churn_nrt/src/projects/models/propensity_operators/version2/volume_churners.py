def set_paths():
    import os
    import sys

    USECASES_SRC = os.path.join(os.environ.get('BDA_USER_HOME', ''), "repositorios", "use-cases")
    if USECASES_SRC not in sys.path:
        sys.path.append(USECASES_SRC)



if __name__ == "__main__":

    set_paths()

    from churn_nrt.src.utils.spark_session import get_spark_session

    sc, spark, sql_context = get_spark_session(app_name="churners_funnel")

    from churn_nrt.src.data_utils.ids_utils import get_ids
    from pyspark.sql.functions import col, max as sql_max, sum as sql_sum

    print("[Info]: 20200131...")

    print("[Info]: Get ids with no filters...")

    df_ids_train = get_ids(spark, '20200131').where(col('serv_rgu') == 'mobile').dropDuplicates(['msisdn'])

    from churn_nrt.src.data.sopos_dxs import MobPort

    print("[Info]: Get portabilities...")

    port_df = MobPort(spark, churn_window=30) \
        .get_module('20200131', save=False, save_others=False, force_gen=True) \
        .select("msisdn", "label_mob") \
        .withColumnRenamed('label_mob', 'churn')

    print("[Info]: Join ids with portabilities...")

    pop_df = df_ids_train.join(port_df, ['msisdn'], 'left').na.fill({'churn': 0})

    pop_df= pop_df.repartition(400)
    pop_df.cache()
    pop_df.count()

    def get_funnel(spark, pop_df, date_, n_cycles=12, verbose=False):


        from churn_nrt.src.utils.date_functions import move_date_n_cycles, get_diff_days
        date_prev = move_date_n_cycles(date_, -n_cycles)
        diff_days = get_diff_days(date_prev, date_, format_date="%Y%m%d")
        pop_df.cache()
        print '[Info] Initial - Initial volume: ' + str(pop_df.count()) + ' - Initial number of msisdn: ' + str(
            pop_df.select('msisdn').distinct().count()) + ' - Initial number of NIFs: ' + str(
            pop_df.select('nif_cliente').distinct().count()) + ' - Initial number of churners: ' + str(
            pop_df.filter(col('churn') == 1).count())

        # if filter_recent:
        from churn_nrt.src.data_utils.base_filters import get_non_recent_customers_filter
        df_ids_rec = get_non_recent_customers_filter(spark, date_, diff_days, level='nif', verbose=False)
        filt_pop_df = pop_df.join(df_ids_rec.select('nif_cliente'), ['nif_cliente'], 'inner') \
            .select('msisdn', 'num_cliente', 'nif_cliente', 'churn')
        filt_pop_df.cache()
        print '[Info] Recent customers - Volume: ' + str(filt_pop_df.count()) + ' - Number of msisdn: ' + str(
            filt_pop_df.select('msisdn').distinct().count()) + ' - Number of NIFs: ' + str(
            filt_pop_df.select('nif_cliente').distinct().count()) + ' - Churners: ' + str(
            filt_pop_df.filter(col('churn') == 1).count())

        # if filter_disc:

        from churn_nrt.src.data_utils.base_filters import get_disconnection_process_filter
        df_ids_disc = get_disconnection_process_filter(spark, date_, diff_days, verbose=verbose, level="nif_cliente")
        filt_pop_df = filt_pop_df.join(df_ids_disc.select('nif_cliente'), ['nif_cliente'], 'inner')
        filt_pop_df.cache()
        print '[Info] DX customers - Volume: ' + str(filt_pop_df.count()) + ' - Number of msisdn: ' + str(
            filt_pop_df.select('msisdn').distinct().count()) + ' - Number of NIFs: ' + str(
            filt_pop_df.select('nif_cliente').distinct().count()) + ' - Churners: ' + str(
            filt_pop_df.filter(col('churn') == 1).count())

        # if filter_ord:

        df_ord_agg = pop_df \
            .select('nif_cliente', "Ord_sla_has_forbidden_orders_last90") \
            .groupBy('nif_cliente') \
            .agg(sql_max('Ord_sla_has_forbidden_orders_last90').alias('Ord_sla_has_forbidden_orders_last90'))


        filt_pop_df = filt_pop_df.join(df_ord_agg, ['nif_cliente'], 'inner').where(~(col("Ord_sla_has_forbidden_orders_last90") > 0))

        filt_pop_df = filt_pop_df.cache()

        print '[Info] Orders - Volume: ' + str(filt_pop_df.count()) + ' - Number of msisdn: ' + str(
            filt_pop_df.select('msisdn').distinct().count()) + ' - Number of NIFs: ' + str(
            filt_pop_df.select('nif_cliente').distinct().count()) + ' - Churners: ' + str(
            filt_pop_df.filter(col('churn') == 1).count())

        # if filter_cc:

        df_ccc_agg = pop_df \
                .select('nif_cliente', "CCC_CHURN_CANCELLATIONS_w8") \
                .groupBy('nif_cliente') \
                .agg(sql_sum('CCC_CHURN_CANCELLATIONS_w8').alias('CCC_CHURN_CANCELLATIONS_w8'))

        filt_pop_df = filt_pop_df.join(df_ccc_agg, ['nif_cliente'], 'inner').where(~(col("CCC_CHURN_CANCELLATIONS_w8") > 0))
        filt_pop_df = filt_pop_df.cache()

        print '[Info] Churn call - Volume: ' + str(filt_pop_df.count()) + ' - Number of msisdn: ' + str(
            filt_pop_df.select('msisdn').distinct().count()) + ' - Number of NIFs: ' + str(
            filt_pop_df.select('nif_cliente').distinct().count()) + ' - Churners: ' + str(
            filt_pop_df.filter(col('churn') == 1).count())

        return 1


    print("[Info]: Get funnel...")

    get_funnel(spark, pop_df, '20200131', n_cycles=12, verbose=False)

    print("[Info]: FINAL...")
