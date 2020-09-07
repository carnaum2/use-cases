


from pyspark.sql.functions import lower, upper, trim, collect_set, collect_list, asc, desc, col, lit, \
    array, upper, collect_list, desc, asc, \
    sum as sql_sum,  collect_set, col, lit, collect_list, desc, asc, \
    count as sql_count, desc, when, col, lit, upper, lower, concat, max as sql_max, min as sql_min, least, row_number, array

import time
import math


def get_saving_path(segment_name, horizon, discarded_cycles):
    if segment_name:
        return "/user/csanc109/projects/poc_segm/car_{}_h{}_d{}/".format(segment_name, horizon, discarded_cycles)
    else:
        return "/user/csanc109/projects/poc_segm/car_h{}_d{}/".format(horizon, discarded_cycles)

def save_car(df, closing_day, segment_name, horizon, discarded_cycles):

    path_to_save = get_saving_path(segment_name, horizon, discarded_cycles)

    print("About to save in path {}".format(path_to_save))
    df = df.withColumn("day", lit(int(closing_day[6:])))
    df = df.withColumn("month", lit(int(closing_day[4:6])))
    df = df.withColumn("year", lit(int(closing_day[:4])))

    print("Started saving - {} for closing_day={}".format(path_to_save, closing_day))
    (df.write.partitionBy('year', 'month', 'day').mode("append").format("parquet").save(path_to_save))

    print("Saved - {} for closing_day={} {}_h{}_d{}".format(path_to_save, closing_day, segment_name, horizon, discarded_cycles))


def set_segment(df, closing_day):

    from churn.utils.general_functions import amdocs_table_reader

    df_customer_aggregations = (amdocs_table_reader(spark, "customer_agg", closing_day, new=False)).na.fill(0)

    df_customer_aggregations = (df_customer_aggregations.withColumn("segment", when(((col("mobile_services_nc") >= 1) & (col("fbb_services_nc") == 0) & (col("tv_services_nc") == 0) &
                      (col("fixed_services_nc") == 0) & (col("bam_services_nc") == 0) & (col("bam_mobile_services_nc") == 0)), "onlymob")
                         .when((col("mobile_services_nc") > 0) & (col("fbb_services_nc") > 0), "mobileandfbb").otherwise("other")))

    print("df_customer_aggregations")
    df_customer_aggregations.select("segment").groupby("segment").agg(sql_count("segment").alias("count")).show()


    df = (df.join(df_customer_aggregations.select("num_cliente", "segment"), on=["num_cliente"], how="left"))

    df = df.fillna("unknown", subset=["segment"])

    print("finishing set_segment")
    df.select("segment").groupby("segment").agg(sql_count("segment").alias("count")).show()


    return df


def filter_car(df_tar):
    print("Filtering churn_cancellations_w8")
    df_tar = df_tar.filter(col('CHURN_CANCELLATIONS_w8') == 0)
    print("Filtering superoferta ON19")
    df_tar = df_tar.where(col("nb_superofertas") == 0)

    return df_tar

def apply_segment(spark, df_car, segment_name, closing_day):
    if segment_name:

        print("Filtering car for segment_name={}".format(segment_name))

        if segment_name == "orders_1":
            from churn.analysis.triggers.orders.run_segment_orders import get_mini_orders_module
            df_segment = get_mini_orders_module(spark, closing_day, exclude_clasif_list=None)
            df_segment = df_segment.where(col("nb_started_orders_last30")>0)
            print("segment {} nb nifs={}".format(segment_name, df_segment.count()))

            print("Before filter by segment={}: nb_msisdns={} nb_nifs={}".format(segment_name, df_car.count(), df_car.select("nif_cliente").distinct().count()))

            df_car = df_car.join(df_segment.select("NIF_CLIENTE"), on=["NIF_CLIENTE"], how="inner")
            print("After filter by segment={}: nb_msisdns={} nb_nifs={}".format(segment_name, df_car.count(),
                                                                  df_car.select("nif_cliente").distinct().count()))

        elif segment_name == "orders_calls_w8":
            from churn.analysis.triggers.orders.run_segment_orders import get_mini_orders_module, get_ccc_attrs_w8, get_segment_msisdn_anyday

            df_segment = get_mini_orders_module(spark, closing_day, exclude_clasif_list=None)
            df_base_msisdn = get_segment_msisdn_anyday(spark, closing_day)
            df_ccc_w8 = get_ccc_attrs_w8(spark, closing_day, df_base_msisdn)
            df_ccc_w8 = df_ccc_w8.select("nif_cliente", "num_calls_w8", "CHURN_CANCELLATIONS_w8")

            df_segment = df_segment.join(df_ccc_w8, on=["nif_cliente"], how="inner")
            df_segment = df_segment.fillna(0)

            df_segment = df_segment.where( (col("nb_started_orders_last30")>0) & (col("num_calls_w8")>0) & (col("CHURN_CANCELLATIONS_w8")==0))
            print("segment {} nb nifs={}".format(segment_name, df_segment.count()))

            print("Before filter by segment={}: nb_msisdns={} nb_nifs={}".format(segment_name, df_car.count(), df_car.select("nif_cliente").distinct().count()))

            df_car = df_car.join(df_segment.select("NIF_CLIENTE"), on=["NIF_CLIENTE"], how="inner")
            print("After filter by segment={}: nb_msisdns={} nb_nifs={}".format(segment_name, df_car.count(),
                                                                  df_car.select("nif_cliente").distinct().count()))

        elif segment_name == "billing_1":

            df_car = df_car.where( ((col("greatest_diff_bw_bills") > 40) & (col('num_calls_w8') > 3) & (col('billing_std') > (0.3 * col("billing_mean"))) & (col('BILLING_POSTPAID_w8') > 0)))

        else:
            print("No existe un segmento llamado {}".format(segment_name))
            import sys
            sys.exit()

        return df_car


def get_unlabeled_car(spark, closing_day):

    #df_car = spark.read.table("tests_es.jvmm_amdocs_ids_" + closing_day) # join del ids
    df_car = spark.read.table("tests_es.jvmm_amdocs_prepared_car_mobile_complete_" + closing_day) # prepared car

    from churn.datapreparation.app.generate_table_extra_feats_new import get_extra_feats_module_path, MODULE_JOIN
    extra_feats_path = get_extra_feats_module_path(closing_day, MODULE_JOIN)
    df_extra_feats  = spark.read.load(extra_feats_path).drop("NIF_CLIENTE", "num_cliente", "SRV_BASIC", "rgu", "campo1", "campo2")

    df_car = df_car.join(df_extra_feats, on=["msisdn"], how="left")

    df_car = set_segment(df_car, closing_day)

    # Keep only active services
    from churn.datapreparation.general.data_loader import get_active_services
    df_active_cd = get_active_services(spark, closing_day, new=False, customer_cols=None, service_cols=None)
    df_car = df_car.join(df_active_cd.select("msisdn"), on=["msisdn"], how="inner")

    df_car.select("segment").groupby("segment").agg(sql_count("segment").alias("count")).show()

    df_car = (df_car.withColumn('blindaje', when((col('tgs_days_until_fecha_fin_dto') >= 0) & (col('tgs_days_until_fecha_fin_dto') <= 60), 'soft')
                                           .when((col('tgs_days_until_fecha_fin_dto') > 60),'hard').otherwise("none")))

    return df_car



def get_labeled_car_segment(spark, closing_day, discarded_cycles, horizon, segment_name):

    from pykhaos.utils.hdfs_functions import check_hdfs_exists

    path_to_car = get_saving_path(segment_name, horizon, discarded_cycles) + "year={}/month={}/day={}/".format(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:]))

    if check_hdfs_exists(path_to_car):
        print("Found already car - '{}'".format(path_to_car))
        df_tar_segm = spark.read.parquet(path_to_car)

    else:

        path_to_car = get_saving_path(None, horizon, discarded_cycles) + "year={}/month={}/day={}/".format(int(closing_day[:4]), int(closing_day[4:6]), int(closing_day[6:]))

        if check_hdfs_exists(path_to_car):
            print("Found already car - '{}'".format(path_to_car))
            df_tar = spark.read.parquet(path_to_car)

        else:
            start_time = time.time()
            df_tar = get_labeled_car(spark, closing_day, discarded_cycles, horizon)
            print("Get labeled car elapsed time: {} mins".format((time.time() - start_time) / 60))

        churn_rate = 100.0 * df_tar.select("label").rdd.map(lambda r: r["label"]).mean()
        print("all", churn_rate)

        for ss in ["mobileandfbb", "onlymob", "other"]:
            print(ss, 100.0 * df_tar.where(col("segment")==ss).select("label").rdd.map(lambda r: r["label"]).mean())

        if not segment_name:
            print("No specified a segment name. Returned complete car")
            return

        df_tar_segm = apply_segment(spark, df_tar, segment_name, closing_day)

        churn_rate_segm = 100.0 * df_tar_segm.select("label").rdd.map(lambda r: r["label"]).mean()

        print("churn_rate={:.2f} churn_rate_seg={:.2f} LIFT={:.2f}".format(churn_rate, churn_rate_segm, churn_rate_segm / churn_rate))

        save_car(df_tar_segm, closing_day, segment_name, horizon, discarded_cycles)

    return df_tar_segm

def get_labeled_car(spark, closing_day, discarded_cycles, horizon):


    import time

    start_time = time.time()
    df_car = get_unlabeled_car(spark, closing_day)
    print("get_unlabeled_car {}".format(   (time.time()-start_time)/60.0))

    num_msisdns = df_car.count()

    from pykhaos.utils.date_functions import move_date_n_days, move_date_n_cycles

    start_date_disc = move_date_n_days(closing_day, n=1) if discarded_cycles > 0 else None
    start_date = move_date_n_cycles(closing_day, n=discarded_cycles) if discarded_cycles > 0 else move_date_n_days(closing_day, n=1)

    end_date_disc = move_date_n_days(start_date, n=-1) if discarded_cycles > 0 else None
    end_date = move_date_n_cycles(closing_day, n=horizon)

    # - - - - - - - - - - - - - - - - - - - - - -
    # DISCARD PORT OUTS IN DISCARDED RANGE
    # - - - - - - - - - - - - - - - - - - - - - -

    from churn.datapreparation.general.data_loader import get_port_requests_table

    # Remove msisdn's of port outs made during the discarded period
    if discarded_cycles > 0:
        print("Getting portouts within discarded period")
        df_sol_por_discard = (get_port_requests_table(spark, config_obj=None, start_date=start_date_disc, end_date=end_date_disc, ref_date=end_date, select_cols=None)
            .withColumnRenamed("msisdn_a", "msisdn_a_discarded")
            .withColumnRenamed("label", "label_discarded"))

        print("Num port outs in discarded period {}".format(df_sol_por_discard.count()))

        # df_sol_por => msisdn_a | label
        df_car = (df_car.join(df_sol_por_discard, on=df_car["msisdn"] == df_sol_por_discard["msisdn_a_discarded"], how='left')
                        .where(df_sol_por_discard['msisdn_a_discarded'].isNull())
                        .drop(*["msisdn_a_discarded", "label_discarded"]))
        num_msisdns_after = df_car.count()

        print("Number of msisdns: car={} | car after removing discarded port-outs={}".format(num_msisdns, num_msisdns_after))

    # - - - - - - - - - - - - - - - - - - - - - -
    # GET PORT OUTS IN TARGET RANGE
    # - - - - - - - - - - - - - - - - - - - - - -

    start_time = time.time()
    df_sol_port = (get_port_requests_table(spark, None, start_date, end_date, end_date, select_cols=None).withColumnRenamed("msisdn_a", "msisdn_a_port"))
    df_sol_port = df_sol_port.cache()

    print("sol_port = {}".format(df_sol_port.count(), df_sol_port.columns))

    # Join between CAR and Port-out requests using "msisdn_d"
    df_tar = (df_car.join(df_sol_port, on=df_car["msisdn"] == df_sol_port["msisdn_a_port"], how="left")
                    .drop(*["msisdn_d_port"]).na.fill({'label': 0.0})
                    .where(col("rgu").rlike("movil|mobile")))

    df_tar = df_tar.fillna(0, subset=["label"])

    print("add df_sol_port {}".format(   (time.time()-start_time)/60.0))


    df_tar = df_tar.cache()
    print("Summary: nb_msisdns={} nb_ports={} nb_nifs={}".format(df_tar.count(),
                                                                 df_tar.where(col("label") == 1).count(),
                                                                 df_tar.select("nif_cliente").distinct().count()))

    churn_rate = 100.0 * df_tar.select("label").rdd.map(lambda r: r["label"]).mean()
    print("churn_rate={:.2f}".format(churn_rate))

    save_car(df_tar, closing_day, "", horizon, discarded_cycles)

    return df_tar


if __name__ == "__main__":


    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
    else:
        root_dir = re.match("(.*)use-cases/churn(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))

    import argparse

    parser = argparse.ArgumentParser(
        description="Run churn_delivery  XXXXXXXX -c YYYYMMDD",
        epilog='Please report bugs and issues to Cristina <cristina.sanchez4@vodafone.com>')
    parser.add_argument('-c', '--closing_day', metavar='<YYYYMMDD>', type=str, required=True,
                        help='Closing day YYYYMMDD to compute lifts by type')

    parser.add_argument('-p', '--predict_closing_day', metavar='<YYYYMMDD>', type=str, required=False,
                        help='Predict closing day to generate trigger')

    parser.add_argument('-s', '--segment',metavar='<name of the segments', type=str, required=False,
                        help='Name of the segment')

    parser.add_argument('-g', '--generate', action='store_true', help='generate car. do not run analysis')

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # INPUT
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    args = parser.parse_args()
    print(args)

    closing_day_arg = args.closing_day.split(" ")[0]
    predict_closing_day = args.predict_closing_day.split(" ")[0] if args.predict_closing_day else None
    segment_name = args.segment
    horizon = 8
    discarded_cycles = int(0.25 * horizon)
    generate_car = args.generate

    from churn.utils.general_functions import init_spark

    spark = init_spark("run_poc_segmentation")

    if generate_car:

        print("Generating car. Do not run analysis")

        if "," in closing_day_arg:
            closing_day_list = closing_day_arg.split(",")
        else:
            closing_day_list = [closing_day_arg]

        from pykhaos.utils.date_functions import move_date_n_days, move_date_n_cycles

        start_time = time.time()

        for closing_day in closing_day_list:

            print("CLOSING DAY {}".format(closing_day))

            start_date_disc = move_date_n_days(closing_day, n=1) if discarded_cycles > 0 else None
            start_date = move_date_n_cycles(closing_day, n=discarded_cycles) if discarded_cycles > 0 else move_date_n_days(closing_day, n=1)

            end_date_disc = move_date_n_days(start_date, n=-1) if discarded_cycles > 0 else None
            end_date = move_date_n_cycles(closing_day, n=horizon)

            print("closing_day: {}".format(closing_day))
            print("predict_closing_day: {}".format(predict_closing_day))
            print("segment: {}".format(segment_name))
            print("Discarded period: {} to {}".format(start_date_disc, end_date_disc))
            print("Port period: {} to {}".format(start_date, end_date))

            start_time_labeled = time.time()
            df_car_segm = get_labeled_car_segment(spark, closing_day, discarded_cycles, horizon, segment_name)
            print("Get labeled car segment elapsed time: {} mins".format((time.time() - start_time_labeled) / 60))


    else:
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        start_time = time.time()
        closing_day_train = "20190414"
        closing_day_val = "20190614"
        analysis_segment = "billing_1"
        from churn.analysis.poc_segments.Metadata import FEATS

        feat_cols = FEATS
        noninf_feats = ['msisdn']
        label_col = 'label'

        COLS = feat_cols + noninf_feats + [label_col]

        df_tar_all = spark.read.load("/user/csanc109/projects/poc_segm/car_h8_d2/year={}/month={}/day={}/".format(int(closing_day_train[:4]), int(closing_day_train[4:6]), int(closing_day_train[6:])))
        df_tar_all = df_tar_all.drop(*[col_ for col_ in df_tar_all.columns if col_.startswith("Bill_")])
        churn_rate_all_base = 100.0 * df_tar_all.select("label").rdd.map(lambda r: r["label"]).mean()

        # df_tar_all.select("segment").groupby("segment").agg(sql_count("segment").alias("count")).show()
        print("step0", df_tar_all.count(), churn_rate_all_base)
        df_tar_conv = df_tar_all.where(col("segment") == "mobileandfbb")
        df_tar_conv = df_tar_conv.cache()
        print("After keeping only mobileandfbb", df_tar_all.count())

        # df_tar = filter_car(df_tar_all)
        # print("step2", df_tar.count())
        # billing car has already filtered super oferta and churn cancellations
        df_billing = spark.read.load(
            "/user/csanc109/projects/triggers/trigger_billing_car/year={}/month={}/day={}/".format(int(closing_day_train[:4]), int(closing_day_train[4:6]), int(closing_day_train[6:])))
        df_billing = df_billing.drop(*[col_ for col_ in df_billing.columns if col_.startswith("tgs_") or col_ == "label"])  # tgs columns are included in car

        df_tar = df_tar_conv.join(df_billing, on=["nif_cliente"], how="inner")

        df_tar = apply_segment(spark, df_tar, analysis_segment, closing_day_train)
        df_tar = df_tar.cache()


        print("step3", df_tar.count())
        df_tar = df_tar.select(*COLS)

        churn_rate_all = 100.0 * df_tar_conv.select("label").rdd.map(lambda r: r["label"]).mean()
        churn_rate_segm = 100.0 * df_tar.select("label").rdd.map(lambda r: r["label"]).mean()
        print("churn_rate_base={:.2f} churn_rate_seg={:.2f} LIFT={:.2f}".format(churn_rate_all_base, churn_rate_segm,
                                                                                churn_rate_segm / churn_rate_all_base))

        print("")
        print("- - - - - - ")
        print("")
        # ANOTHER MONTH FOR VALIDATION

        df_next_tar_all = spark.read.load("/user/csanc109/projects/poc_segm/car_h8_d2/year={}/month={}/day={}/".format(int(closing_day_val[:4]), int(closing_day_val[4:6]), int(closing_day_val[6:])))
        df_next_tar_all = df_next_tar_all.drop(*[col_ for col_ in df_next_tar_all.columns if col_.startswith("Bill_")])
        churn_rate_next_all_base = 100.0 * df_next_tar_all.select("label").rdd.map(lambda r: r["label"]).mean()
        print("step0", df_next_tar_all.count(), churn_rate_next_all_base)
        df_next_tar_conv = df_next_tar_all.where(col("segment") == "mobileandfbb")
        df_next_tar_conv = df_next_tar_conv.cache()

        print("After keeping only mobileandfbb", df_next_tar_conv.count())
        # print("step2", df_next_tar.count())
        # billing car has already filtered super oferta and churn cancellations
        df_billing_next = spark.read.load(
            "/user/csanc109/projects/triggers/trigger_billing_car/year={}/month={}/day={}/".format(int(closing_day_val[:4]), int(closing_day_val[4:6]), int(closing_day_val[6:])))
        df_billing_next = df_billing_next.drop(*[col_ for col_ in df_billing_next.columns if col_.startswith("tgs_") or col_ == "label"])  # tgs columns are included in car

        df_next_tar = df_next_tar_conv.join(df_billing, on=["nif_cliente"], how="inner")

        df_next_tar = apply_segment(spark, df_next_tar, analysis_segment, closing_day_val) if analysis_segment else df_next_tar_conv

        df_next_tar = df_next_tar.cache()
        print("step3", df_next_tar.count())
        df_next_tar = df_next_tar.select(*COLS)

        ### Filter inactive

        # from churn.datapreparation.general.data_loader import get_active_services
        # df_active_cd = get_active_services(spark, closing_day_train, new=False, customer_cols=None, service_cols=None)
        # df_active_cd_val = get_active_services(spark, closing_day_val, new=False, customer_cols=None, service_cols=None)

        # count_1 = df_tar.count()
        # df_tar = df_tar.join(df_active_cd.select("msisdn"), on=["msisdn"], how="inner")
        # count_2 = df_tar.count()

        # print("df_tar current_count={} inactive={}".format(count_2, count_1 - count_2))

        # count_1 = df_next_tar.count()
        # df_next_tar = df_next_tar.join(df_active_cd_val.select("msisdn"), on=["msisdn"], how="inner")
        # count_2 = df_next_tar.count()

        # print("df_next_tar current_count={} inactive={}".format(count_2, count_1 - count_2))

        churn_rate_next_conv = 100.0 * df_next_tar_conv.select("label").rdd.map(lambda r: r["label"]).mean()
        churn_rate_next_segm = 100.0 * df_next_tar.select("label").rdd.map(lambda r: r["label"]).mean()
        print("churn_rate_base={:.2f} churn_rate_seg={:.2f} LIFT={:.2f}".format(churn_rate_next_all_base, churn_rate_next_segm,
                                                                           churn_rate_next_segm / churn_rate_next_all_base))

        from churn.datapreparation.app.generate_table_extra_feats_new import impute_nulls

        df_tar = impute_nulls(df_tar, spark, metadata_version="1.1")
        df_next_tar = impute_nulls(df_next_tar, spark, metadata_version="1.1")

        print("Get labeled car segments elapsed time: {} mins".format((time.time() - start_time) / 60))


        # - - - - - - - - - - - - -

        from churn.analysis.poc_segments.poc_modeler import get_split, fit, smart_fit

        df_unbaltr, df_tr, df_tt = get_split(df_tar, train_split_ratio=0.7)

        print("Lets fit")
        start_fit = time.time()
        # model = fit(df_tr, feat_cols, maxDepth=10, maxBins=32, minInstancesPerNode=50, impurity="gini", featureSubsetStrategy="sqrt", subsamplingRate=0.7, numTrees=50,seed=1234)
        model = smart_fit("rf", df_tr)
        print("ended fit - {} min".format((time.time() - start_fit) / 60.0))

        model.stages[-1].extractParamMap()

        from churn.analysis.poc_segments.poc_modeler import get_feats_imp

        feat_imp_list = get_feats_imp(model, feat_cols, top=25)

        from churn.analysis.poc_segments.poc_modeler import get_score, get_metrics

        # Train evaluation
        df_tr_preds = get_score(model, df_tr)
        # trPredictionAndLabels = df_tr_preds.select(['model_score', 'label']).rdd.map(lambda r: (r['model_score'], float(r['label'])))

        # Test eval
        df_tt_preds = get_score(model, df_tt)

        # Evaluation TODO
        df_val_preds = get_score(model, df_next_tar).withColumnRenamed("model_score", "score_segm")

        # - - - - - - - - - - - - -

        nb_deciles = int(math.floor(df_val_preds.count() / 2500))

        # Validation
        get_metrics(df_tr_preds, title="train", nb_deciles=None)
        get_metrics(df_tt_preds, title="test", nb_deciles=None)
        lift_val = get_metrics(df_val_preds, title="validation", nb_deciles=nb_deciles, score_col="score_segm", refprevalence=churn_rate_next_all_base)

        from churn.datapreparation.general.model_outputs_manager import get_complete_scores_model_name_closing_day

        df_deliv = get_complete_scores_model_name_closing_day(spark, model_name="churn_preds_mobileandfbb", closing_day=closing_day_val).select("msisdn", "scoring")
        df_deliv = df_deliv.cache()

        # print("df_deliv= {}".format(df_deliv.count()))
        df_deliv = df_deliv.join(df_val_preds.select("msisdn", "label"), on=["msisdn"], how="right")
        # df_deliv = df_deliv.cache()
        # count_1 = df_deliv.count()
        # print("df_deliv= {}".format(count_1))
        # df_deliv_null = df_deliv.where(col("scoring").isNull())

        # df_deliv = df_deliv.where(col("scoring").isNotNull())
        # count_2 = df_deliv.count()
        # print("df_deliv removed {1} nulls. Current count {0}".format(count_2, count_1-count_2))

        get_metrics(df_deliv, title="delivery", nb_deciles=None, score_col="scoring", label_col="label")

        print("Complete process elapsed time: {} mins".format( (time.time() - start_time)/60))

