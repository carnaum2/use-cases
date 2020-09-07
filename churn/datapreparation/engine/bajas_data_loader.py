

from pyspark.sql.functions import row_number, when, regexp_replace, col, desc, col, create_map, lit
from pyspark.sql.window import Window

import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()
from churn.utils.constants import *

from churn.datapreparation.general.data_loader import get_unlabeled_car
import sys
import os
from itertools import chain

#FIXME use constants
REASONS_MAPPING = {u'Cambio de situaci\ufffdn personal' : "SITUACION",
                   u'Precio-Necesidad de ahorro' : "PRECIO",
                   u'Problemas con el servicio': "SERVICIO/ATENCION",
                   u'No disponibilidad de Fibra' : "NO_FIBRA",
                   u'Mala atenci\ufffdn al cliente' : "SERVICIO/ATENCION",
                   u'Enga\ufffdo comercial' : "FRAUD",
                   u'Falta de contenidos de TV' : "CONTENIDOS",
                   u'Otros' : "OTROS",
                   u'Problemas con factura' : "BILLING"}



def set_label_to_msisdn_df(spark, config_obj):


    closing_day = config_obj.get_closing_day()
    months_delay = config_obj.get_months_delay()
    model_target = config_obj.get_model_target()


    # - - - - - - - - - - - - - - - - - - - - - -
    # UNLABELED CAR
    # - - - - - - - - - - - - - - - - - - - - - -
    # 1.Population(num_cliente)
    # cols = 'num_cliente'
    df_car = get_unlabeled_car(spark, config_obj)

    if not config_obj.get_labeled():
        print("return df_car since unlabeled car was requested")
        return df_car

    # - - - - - - - - - - - - - - - - - - - - - -
    # GET REASONS IN TARGET RANGE
    # - - - - - - - - - - - - - - - - - - - - - -
    df_reasons = load_reasons_encuestas(spark, config_obj).withColumn("NIF", "NIF_reasons")

    if model_target=="price":
        df_reasons = df_reasons.withColumn("label", when(col("reason").startswith("Precio"), 1).otherwise(0))
    else:
        print("Model target {} is not implemented".format(model_target))
        sys.exit()

    # - - - - - - - - - - - - - - - - - - - - - -
    # JOINED UNLABELED CAR
    # - - - - - - - - - - - - - - - - - - - - - -
    # Join between CAR and reasons using "NIF"
    df_tar = (df_reasons.join(df_car, on=df_reasons["NIF_reasons"]==df_car["NIF"], how="left") # only want NIFs with label
        .drop(*["NIF_reasons"]))

    print("[Info BajasDataLoader] Number of msisdns: df_tar={} ".format(df_tar.count()))

    return df_tar


# @deprecated
def get_reasons_doc(self):

    df = (self.get_spark().read.option("delimiter", ";").option("header", True).csv(REASONS_FILENAME)
          .where(col("Stack") == "Amdocs")
          .withColumnRenamed("Mes", "yearmonth")
          .withColumnRenamed("NET Motivo Principal", "reason"))

    # Create columns "year" and "month" in case we need to join to other dataframes
    MONTHS = [("Julio.*", "July"), ("Abril.*", "April"), ("Septiembre.*", "September"),
              ("Mayo.*", "May"), ("Junio.*", "June"), ("Agosto.*", "August")]
    df = (df.withColumn('year', col('yearmonth').substr(-2, 2)))  # get the two last characters
    df = df.withColumnRenamed('yearmonth', "month")
    for mm in MONTHS:
        df = df.withColumn('month', regexp_replace("month", mm[0], mm[1]))



def load_reasons_encuestas(spark, config_obj=None):
    if config_obj:
        closing_day = config_obj.get_closing_day()
        months_delay = config_obj.get_months_delay()

        from pykhaos.utils.date_functions import move_date_n_yearmonths
        start_yyyymm = move_date_n_yearmonths(closing_day[0], months_delay)  # yyyymm
        end_yyyymm = move_date_n_yearmonths(closing_day[-1], months_delay)  # yyyymm
        print("Reasons encuestas will be from {} to {}".format(start_yyyymm, end_yyyymm))
    else:
        print("Returning all information churn_reason...")
        start_yyyymm = end_yyyymm = None

    df_bajas_all = spark.read.load('/data/raw/vf_es/nps/churn_reason/1.0/parquet')
    df_bajas_all = df_bajas_all.drop_duplicates(subset=['NIF'])  # hay un NIF repetido

    df_bajas_all = df_bajas_all.where(col("Stack") == "Amdocs")

    if config_obj:
        df_bajas_all =  df_bajas_all.where( (col("year")>=start_yyyymm[:4]) & (col("month")>=start_yyyymm[4:6]) & (col("year")<=end_yyyymm[:4]) & (col("month")<=end_yyyymm[4:6]))

    df_bajas_all = (df_bajas_all.withColumnRenamed("Mes", "yearmonth").withColumnRenamed("NET_Motivo_Principal", "reason"))

    w_order = Window().partitionBy("nif").orderBy(desc("year"), desc("month"), desc("day"))

    # Filter the repeteated rows, keeping the last one
    df_bajas_all = (df_bajas_all.withColumn("rowNumorder", row_number().over(w_order)).where(col('rowNumorder') == 1)).drop('rowNumorder')



    mapping_expr = create_map([lit(x) for x in chain(*REASONS_MAPPING.items())])

    df_bajas_all = df_bajas_all.withColumn("REASON_ENCUESTA",
                                                     mapping_expr.getItem(col('reason')))


    return df_bajas_all

def get_labeled_car(spark, config_obj, feats=None, target=None):

    # # 1.Population(num_cliente)
    # # cols = 'num_cliente'
    # df_target_services = get_numclients_under_analysis(spark, config_obj)
    #
    # print("[Info DataLoader] Number of target services before labeling:{} ".format(df_target_services.count()))

    # Labeling
    # with the specified target (modeltarget) and the specified level (level).Services (service_set) in the
    # specified segment (target_num_cliente) are labeled

    df_label_car = set_label_to_msisdn_df(spark, config_obj)

    # TODO
    if feats != None and target != None:
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
    elif labeled == False:
        print("Asked unlabeled car")
        df_unlabeled_car = get_unlabeled_car(spark, config_obj)
        return df_unlabeled_car
    else:
        print("Not valid value for labeled in yaml file")
        return None

def build_storage_dir_name_obj(config_obj, add_hdfs_preffix=False):

    from churn.utils.constants import HDFS_DIR_DATA
    import os

    start_port = config_obj.get_start_port()
    end_port = config_obj.get_end_port()
    n_ccc = config_obj.get_ccc_range_duration()
    closing_day = config_obj.get_closing_day()[0]

    model_target = config_obj.get_model_target()
    labeled = config_obj.get_labeled()
    agg_by = config_obj.get_agg_by()

    name_ = 'df_{}_{}_c{}_n{}_{}_{}'.format(start_port, end_port, closing_day, abs(n_ccc), model_target, agg_by)

    print(name_)
    print(labeled)
    print(model_target)

    filename_df = os.path.join(HDFS_DIR_DATA, "encuestas", model_target if labeled else "unlabeled", name_)


    if add_hdfs_preffix:
        filename_df = "hdfs://" + filename_df

    print("Built filename: {}".format(filename_df))
    return filename_df



def save_results(df, config_obj, csv=True):
    '''
    This function save the df and the yaml's (internal and user)
    :param df:
    :param path_filename:
    :return:
    '''
    storage_dir = build_storage_dir_name_obj(config_obj, add_hdfs_preffix=False)
    from churn.utils.general_functions import save_df
    if df:
        save_df(df, storage_dir, csv)
        print("Saved df in '{}' format {}".format(storage_dir, "csv" if csv else "parquet"))
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
    return storage_dir


def get_complete_info_encuestas(spark):  # FIXME do not hardcode months
    '''
    Return the information of the churn reasons joint with columns "msisdn_a", "msisdn_d", "NIF_CLIENTE", "num_cliente"
    :param spark:
    :return:
    '''

    from churn.utils.general_functions import get_nif_msisdn

    df_bajas = load_reasons_encuestas(spark, config_obj=None)
    print("df_bajas={}".format(len(df_bajas)))

    df_old_0531 = get_nif_msisdn(spark, "20180531").withColumn("closing_day", lit("20180531"))
    df_old_0630 = get_nif_msisdn(spark, "20180630").withColumn("closing_day", lit("20180630"))
    df_old_0731 = get_nif_msisdn(spark, "20180731").withColumn("closing_day", lit("20180731"))
    df_old_0831 = get_nif_msisdn(spark, "20180831").withColumn("closing_day", lit("20180831"))
    df_old_0930 = get_nif_msisdn(spark, "20180930").withColumn("closing_day", lit("20180930"))
    df_old_1031 = get_nif_msisdn(spark, "20181031").withColumn("closing_day", lit("20181031"))

    cols = list(
        set(df_old_0531.columns) & set(df_old_0630.columns) & set(df_old_0731.columns) & set(df_old_0831.columns) & set(
            df_old_0930.columns) & set(df_old_1031.columns))

    def union_all(dfs):
        if len(dfs) > 1:
            return dfs[0].unionAll(union_all(dfs[1:]))
        else:
            return dfs[0]

    df_car_joined = union_all(
        [df_old_0531.select(cols), df_old_0630.select(cols), df_old_0731.select(cols), df_old_0831.select(cols),
         df_old_0930.select(cols), df_old_1031.select(cols)])
    w_order = Window().partitionBy("nif_cliente").orderBy(desc("closing_day"))
    # Filter the repeteated rows, keeping the last one
    df_car_joined = (
        df_car_joined.withColumn("rowNumorder", row_number().over(w_order)).where(col('rowNumorder') == 1)).drop(
        "rowNumorder")

    df_complete = df_bajas.join(df_car_joined, on=df_bajas["NIF"] == df_car_joined["NIF_CLIENTE"], how="left").drop(
        "NIF_CLIENTE", 'service_processed_at', 'service_file_id')
    print("df_complete={}".format(len(df_complete)))

    return df_complete