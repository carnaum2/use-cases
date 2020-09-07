
from churn.datapreparation.general.data_loader import get_unlabeled_car, get_port_requests_table, get_numclients_under_analysis
import os
from pyspark.sql.functions import col

from pykhaos.utils.date_functions import move_date_n_days, move_date_n_cycles

def set_label_to_msisdn_df(spark, config_obj):

    closing_day = config_obj.get_closing_day()[0] #FIXME change to support more than one closing

    cycles_horizon = config_obj.get_cycles_horyzon()
    discarded_cycles = config_obj.get_discarded_cycles()

    # df_target_num_clients cols:num_cliente
    start_date_disc = move_date_n_days(closing_day, n=1) if discarded_cycles > 0 else None
    start_date = move_date_n_cycles(closing_day, n=discarded_cycles) if discarded_cycles > 0 else move_date_n_days(
        closing_day, n=1)

    end_date_disc = move_date_n_days(start_date, n=-1) if discarded_cycles > 0 else None
    end_date = move_date_n_cycles(closing_day, n=cycles_horizon)

    level = config_obj.get_level()
    service_set = config_obj.get_service_set()

    # - - - - - - - - - - - - - - - - - - - - - -
    # UNLABELED CAR
    # - - - - - - - - - - - - - - - - - - - - - -
    # 1.Population(num_cliente)
    # cols = 'num_cliente'
    df_car = get_unlabeled_car(spark, config_obj)

    if not config_obj.get_labeled():
        print("return df_car since unlabeled car was requested")
        return df_car

    num_msisdns = df_car.count()

    # - - - - - - - - - - - - - - - - - - - - - -
    # DISCARD PORT OUTS IN DISCARDED RANGE
    # - - - - - - - - - - - - - - - - - - - - - -

    # Remove msisdn's of port outs made during the discarded period
    if discarded_cycles>0:

        df_sol_por_discard = (get_port_requests_table(spark, config_obj, closing_day, select_cols=None)
                            .withColumnRenamed("msisdn_a", "msisdn_a_discarded").withColumnRenamed("label", "label_discarded"))

        print("Num port outs in discarded period {}".format(df_sol_por_discard.count()))

        # df_sol_por => msisdn_a | label
        df_car = (df_car.join(df_sol_por_discard, on=df_car["msisdn_a"]==df_sol_por_discard["msisdn_a_discarded"],
                                    how='left').where(df_sol_por_discard['msisdn_a_discarded'].isNull())
                        .drop(*["msisdn_a_discarded", "label_discarded"])
                  )
        num_msisdns_after = df_car.count()
        print("[Info DataLoader] Number of msisdns: car={} | car after removing discarded port-outs={}".format(
            num_msisdns, num_msisdns_after))

    # - - - - - - - - - - - - - - - - - - - - - -
    # GET PORT OUTS IN TARGET RANGE
    # - - - - - - - - - - - - - - - - - - - - - -

    df_sol_port = (get_port_requests_table(spark, config_obj, start_date, end_date, closing_day, select_cols=None)
                                .withColumnRenamed("msisdn_a", "msisdn_a_port"))

    print("sol_port = {}".format(df_sol_port.count()))
    # df_sol_port cols:msisdn_d_port,label
    if level == "service":
        # Join between CAR and Port-out requests using "msisdn_d"
        df_tar = (df_car.join(df_sol_port, on=df_car["msisdn_a"]==df_sol_port["msisdn_a_port"], how="left")
            .withColumnRenamed("msisdn_a", "msisdn")
            .drop(*["msisdn_d_port"])
            .na.fill({'label': 0.0})
            .where(col("rgu").isin(service_set))
        )
        #print("df_tar", ",".join(df_tar.columns))
        nb_rows_tar = df_tar.count()
        print("[Info DataLoader] Number of msisdns: df_tar={} ".format(nb_rows_tar))
        if nb_rows_tar>0:
            print("Num port outs in labeled car {}".format(df_tar.select("label").rdd.map(lambda x: (1, x[0])).reduceByKey(
                lambda x, y: x + y).collect()[0][1]))
    else:
        print("level {} is not implemented".format(level))
        import sys
        sys.exit(1)

    return df_tar


def get_labeled_car(spark, config_obj, feats=None, target=None):

    # 1.Population(num_cliente)
    # cols = 'num_cliente'
    df_target_services = get_numclients_under_analysis(spark, config_obj, config_obj.get_closing_day()[0])#FIXME add support to more than one closing day

    print("[Info DataLoader] Number of target services before labeling:{} ".format(df_target_services.count()))

    # 2. Labeling
    # with the specified target (modeltarget) and the specified level (level).Services (service_set) in the
    # specified segment (target_num_cliente) are labeled

    df_label_car = set_label_to_msisdn_df(spark, config_obj)

    # TODO
    if feats!=None and target!=None:
        car_feats = feats + target
        df_label_car = df_label_car.select(car_feats)

    print("[Info Amdocs Car Preparation] Size of labelfeatcar: {}".format(df_label_car.count()))

    return df_label_car

def get_labeled_or_unlabeled_car(spark, config_obj, feats=None, target=None):

    labeled = config_obj.get_labeled()
    print("LABELED {},{}".format(labeled, type(labeled)))
    if labeled:
        print("Asked labeled car")
        df_labeled_car = get_labeled_car(spark, config_obj, feats=feats, target=target)
        return df_labeled_car
    elif labeled==False:
        print("Asked unlabeled car")
        df_unlabeled_car = get_unlabeled_car(spark, config_obj)
        return df_unlabeled_car
    else:
        print("Not valid value for labeled in yaml file")
        return None

def build_storage_dir_name(config_obj, add_hdfs_preffix=False):

    from churn.utils.constants import HDFS_DIR_DATA
    import os

    closing_day = config_obj.get_closing_day()
    segment_filter = config_obj.get_segment_filter()
    level = config_obj.get_level()
    model_target = config_obj.get_model_target()
    cycles_horizon =  config_obj.get_cycles_horyzon()
    discarded_cycles = config_obj.get_discarded_cycles()
    labeled = config_obj.get_labeled()
    service_set = config_obj.get_service_set()

    name_ = 'df_{}_{}_{}_{}_c{}_d{}'.format(model_target, segment_filter, level, "_".join(closing_day), cycles_horizon, discarded_cycles) if labeled else 'df_{}_{}_{}_{}'.format(model_target, segment_filter, level, closing_day)

    filename_df = os.path.join(HDFS_DIR_DATA, model_target if labeled else "unlabeled", segment_filter, name_)

    if add_hdfs_preffix:
        filename_df = "hdfs://" + filename_df

    print("Built filename: {}".format(filename_df))
    return filename_df




def save_results(df, config_obj):
    '''
    This function save the df and the yaml's (internal and user)
    :param df:
    :param path_filename:
    :return:
    '''

    storage_dir = build_storage_dir_name(config_obj, add_hdfs_preffix=False)
    from churn.utils.general_functions import save_df
    save_df(df, storage_dir)

    from churn.utils.constants import YAML_FILES_DIR
    from pykhaos.utils.hdfs_functions import create_directory
    # create directory in hdfs to store config files
    yaml_dir = os.path.join(storage_dir, YAML_FILES_DIR)
    create_directory(yaml_dir)
    from pykhaos.utils.hdfs_functions import move_local_file_to_hdfs
    user_config_filename = config_obj.get_user_config_filename()
    internal_config_filename = config_obj.get_internal_config_filename()
    move_local_file_to_hdfs(yaml_dir, user_config_filename)
    move_local_file_to_hdfs(yaml_dir, internal_config_filename)