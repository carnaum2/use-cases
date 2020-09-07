# -*- coding: utf-8 -*-

########################################################################
"""
  This module contains functions to build de customer base: customer + services
"""
########################################################################

import sys
import pandas as pd
import itertools

from pyspark.sql.functions import (udf, col, lit, lower, concat, count, max, avg, desc, asc, row_number, lpad, trim, when,
                                   isnull, count as sql_count, collect_list, create_map)
from itertools import chain

# from churn_nrt.src.data.customers_data import get_customers
# from churn_nrt.src.data.services_data import get_services

from churn_nrt.src.data.customers_data import Customer
from churn_nrt.src.data.services_data import Service

from churn_nrt.src.utils.date_functions import is_cycle, get_previous_cycle, move_date_n_cycles
from churn_nrt.src.data_utils.DataTemplate import DataTemplate
from pyspark.sql.types import IntegerType

from churn_nrt.src.data.customers_data import CCAA_DICT


def add_ccaa(spark, df, closing_day=None):
    '''
    Add a column with the ccaa ("ccaa")
    :param spark:
    :param df:
    :param closing_day:
    :return:
    '''
    added_cp = False
    if "cp" not in df.columns:
        if not closing_day:
            print("[customer_base] add_ccaa | closing_day is mandatory when cp column is not present. Program will exit now")
            import sys
            sys.exit(1)
        df = add_postal_code(spark, df, closing_day)
        added_cp = True

    cp_to_ccaa = {cp: ccaa for ccaa, cp_list in CCAA_DICT.items() for cp in cp_list}

    mapping_expr = create_map([lit(x) for x in chain(*cp_to_ccaa.items())])

    df = df.withColumn('ccaa', mapping_expr[df['cp']]).fillna({"ccaa": "unknown"})

    if added_cp:
        df = df.drop("cp")

    return df


def add_postal_code(spark, df, closing_day):
    '''
    Add a postal code column ("cp") with the postal code of the customer.
    cp is between [1,52]. otherwise, the value is set to 'unknown'
    :param spark:
    :param df:
    :param closing_day:
    :return: the original dataframe plus an additional column 'cp' with the postal code (two chars)
    '''
    if not ("num_cliente" in df.columns or "NUM_CLIENTE" in df.columns) and not "msisdn" in df.columns:
        print("[customers_data] add_postal_code | This functions requires the num_cliente or msisdn columns")
        import sys
        sys.exit()

    UNKNOWN_TAG = "unknown"

    df_cust = Customer(spark).get_module(closing_day, add_columns=["codigo_postal"])
    df_cust = df_cust.withColumn("cp", col("codigo_postal").substr(0, 2))
    df_cust = df_cust.withColumn("cp", when(col("cp").rlike("^\d{2}$"), col("cp")).otherwise(UNKNOWN_TAG))
    df_cust = df_cust.withColumn("cp", when(~col("cp").isin("00"), col("cp")).otherwise(UNKNOWN_TAG))


    if not ("num_cliente" in df.columns or "NUM_CLIENTE" in df.columns) :
        df_base = CustomerBase(spark).get_module(closing_day).drop_duplicates(["msisdn"])
        df_join1 = df.join(df_base.select("msisdn", "num_cliente"), on=["msisdn"], how="left")
        df_join = df_join1.join(df_cust.select("num_cliente", "cp"), on=["num_cliente"], how="left")
        return df_join.drop("num_cliente")

    df_join = df.join(df_cust.select("num_cliente", "cp"), on=["num_cliente"], how="left")


    df_join = df_join.withColumn("cp", when(col("cp").cast(IntegerType())>52, UNKNOWN_TAG).when(col("cp").cast(IntegerType())<0, UNKNOWN_TAG).otherwise(col("cp")))

    return df_join



#PATH_UNLABELED = HDFS_CHURN_NRT + "customer_base/"

def __get_customer_base(spark, date_, save_others, add_columns_customer=None, force_gen=False):
    '''
    Given a date, return the customer base of active clientes
    The returned dataframe has the following columns: ['NUM_CLIENTE', 'NIF_CLIENTE', 'msisdn', 'rgu', 'msisdn_d']
    :param spark:
    :param date_:
    :return:
    '''

    print("[CustomerBase] __get_customer_base date_={} save_others={} add_columns_customer={}".format(date_, save_others, add_columns_customer))

    customers_df = Customer(spark).get_module(date_, save=save_others, add_columns=add_columns_customer, save_others=save_others, force_gen=force_gen)
    services_df = Service(spark).get_module( date_, save=save_others,  save_others=save_others, force_gen=force_gen)

    default_cols = ['NUM_CLIENTE', 'NIF_CLIENTE', 'msisdn', 'rgu', "cod_estado_general", "srv_basic", "TARIFF"]

    # Build segment_nif at this point
    select_cols = list(set(default_cols) | set(add_columns_customer)) if add_columns_customer else default_cols

    # INSIGHTS FILTER - uncomment when base comparison task is finished
    # base_df = (customers_df.join(services_df, 'NUM_CLIENTE', 'inner').filter((col('rgu') != 'prepaid') & (col("clase_cli_cod_clase_cliente").isin('NE ','RV ','DA ','BA ','RS')) & (col("cod_estado_general").isin( '01','03','07','09'))
    #         & (col("srv_basic").isin("MRPD1", "MRSUI", "MPPD2", "MRIOE", "MPSUI", "MVPS3", "MPIOE"))).select(select_cols))\
    #            .filter((~isnull(col('NIF_CLIENTE'))) & (~col('NIF_CLIENTE').isin('', ' ')) & (~isnull(col('msisdn'))) & (~col('msisdn').isin('', ' ')) & (~isnull(col('rgu'))) & (~col('rgu').isin('', ' ')) & (~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' ')))\
    base_df = customers_df\
    .join(services_df, 'NUM_CLIENTE', 'inner')\
    .filter((col('rgu') != 'prepaid') & (col("clase_cli_cod_clase_cliente") == "RS") & (col("cod_estado_general").isin("01", "03", "07", "09")) & (col("srv_basic").isin("MRSUI", "MPSUI") == False))\
    .select(select_cols)\
    .filter((~isnull(col('NIF_CLIENTE'))) & (~col('NIF_CLIENTE').isin('', ' ')) & (~isnull(col('msisdn'))) & (~col('msisdn').isin('', ' ')) & (~isnull(col('rgu'))) & (~col('rgu').isin('', ' ')) & (~isnull(col('NUM_CLIENTE'))) & (~col('NUM_CLIENTE').isin('', ' ')))\
    .distinct()

    return base_df


def get_customer_base_segment(spark, date_, save_others, add_columns_customer=None, force_gen=False, level="NIF_CLIENTE"):
    '''
    Same as get_customer_base but with an additional column 'segment_<by_level>'
     ['nif_cliente', 'NUM_CLIENTE', 'msisdn',  'rgu', 'msisdn_d', 'segment_<by_level>']
     by_level is "nif" if level="nif_cliente" or "nc" if level="num_cliente"
    :param spark:
    :param date_:
    :return:
    '''

    level = level.upper()

    if level not in ["NIF_CLIENTE", "NUM_CLIENTE"]:
        print("get_customer_base_segment | unknown level {}".format(level))
        import sys
        sys.exit()

    if level == "NIF_CLIENTE":
        suffix = "nif"
    else:
        suffix = "nc"

    print("[CustomerBase] Get get_customer_base_segment base segment | date {} save_others {} level={}".format(date_, save_others,level))
    VALUES_CUST_LEVEL_AGG = ['fbb', 'mobile', 'tv', 'prepaid', 'bam_mobile', 'fixed', 'bam']

    raw_base_df = __get_customer_base(spark, date_, save_others, add_columns_customer, force_gen=force_gen)
    #print("[CustomerBase] get_customer_base_segment | raw_base_df.columns={}".format(",".join(raw_base_df.columns)))

    data_CAR_CUST_tmp2 = (raw_base_df.groupby(level).pivot('rgu', VALUES_CUST_LEVEL_AGG).agg(count(lit(1))).na.fill(0))

    for c in VALUES_CUST_LEVEL_AGG:
        data_CAR_CUST_tmp2 = data_CAR_CUST_tmp2.withColumnRenamed(c, "nb_" + c + '_services_'+suffix)

    data_CAR_CUST_tmp2 = data_CAR_CUST_tmp2.join(raw_base_df.select(level).distinct(), [level], 'inner')

    # Pospaid segments by NIF
    # Postpaid mobile, and optionally with prepaid mobile, and no FBB
    mo_condition_nif = (data_CAR_CUST_tmp2['nb_mobile_services_'+suffix] > 0) & (data_CAR_CUST_tmp2['nb_fbb_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_prepaid_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_bam_mobile_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_fixed_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_bam_services_'+suffix] == 0)

    # Standalone FBB: FBB, and no postpaid mobile
    fbb_condition_nif = (data_CAR_CUST_tmp2['nb_mobile_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_fbb_services_'+suffix] > 0) & (data_CAR_CUST_tmp2['nb_prepaid_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_bam_mobile_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_fixed_services_'+suffix] >= 0) & (data_CAR_CUST_tmp2['nb_bam_services_'+suffix] == 0)

    # FBB, and optionally with pre or postpaid mobile
    co_condition_nif = (data_CAR_CUST_tmp2['nb_mobile_services_'+suffix] > 0) & (data_CAR_CUST_tmp2['nb_fbb_services_'+suffix] > 0) & (data_CAR_CUST_tmp2['nb_prepaid_services_'+suffix] >= 0) & (data_CAR_CUST_tmp2['nb_bam_mobile_services_'+suffix] >= 0) & (data_CAR_CUST_tmp2['nb_fixed_services_'+suffix] >= 0) & (data_CAR_CUST_tmp2['nb_bam_services_'+suffix] >= 0)

    # Only fixed
    fixed_condition_nif = (data_CAR_CUST_tmp2['nb_mobile_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_fbb_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_prepaid_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_bam_mobile_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_fixed_services_'+suffix] > 0) & (data_CAR_CUST_tmp2['nb_bam_services_'+suffix] == 0)

    # Pure prepaid: Prepaid, and no postpaid
    pre_condition_nif = (data_CAR_CUST_tmp2['nb_mobile_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_fbb_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_prepaid_services_'+suffix] > 0) & (data_CAR_CUST_tmp2['nb_bam_mobile_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_fixed_services_'+suffix] == 0) & (data_CAR_CUST_tmp2['nb_bam_services_'+suffix] == 0)

    # Others

    data_CAR_CUST_tmp2 = (data_CAR_CUST_tmp2.withColumn('seg_pospaid_'+suffix, when(mo_condition_nif,  'Mobile_only')
                                                                         .when(fbb_condition_nif, 'Standalone_FBB')
                                                                         .when(co_condition_nif,  'Convergent')
                                                                         .when(pre_condition_nif, 'Pure_prepaid')
                                                                        .when(fixed_condition_nif, 'Only_fixed')
                                                                        .otherwise('Other')))

    segment_base_df = raw_base_df \
        .join(data_CAR_CUST_tmp2.select(level, 'seg_pospaid_'+suffix, 'nb_fbb_services_'+suffix, 'nb_mobile_services_'+suffix, 'nb_tv_services_'+suffix, 'nb_prepaid_services_'+suffix, 'nb_bam_mobile_services_'+suffix, 'nb_fixed_services_'+suffix, 'nb_bam_services_'+suffix), [level.upper()], 'inner')

    segment_base_df = (segment_base_df
                     .withColumnRenamed("seg_pospaid_"+suffix, "segment_"+suffix)
                     .withColumnRenamed(level.upper(), level.lower())
                   )

    segment_base_df = segment_base_df.withColumn('nb_rgus_'+suffix, col('nb_fbb_services_'+suffix) + col('nb_mobile_services_'+suffix) + col('nb_bam_mobile_services_'+suffix) + col('nb_fixed_services_'+suffix) + col('nb_bam_services_'+suffix))


    return segment_base_df



class CustomerBase(DataTemplate):

    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "customer_base")

    def is_default_module(self, *args, **kwargs):
        print("[CustomerBase] is_default_module | args: {} | kwargs: {}".format(args, kwargs))

        # if None --> then it can be saved
        check =  (kwargs.get('level', "nif_cliente").lower() == "nif_cliente") and (kwargs.get('add_tgs', True) == True)
        if not check:
            print("[CustomerBase] is_default_module | Module {} cannot be saved since level is different than nif_cliente or add_tgs is True".format(self.MODULE_NAME))
        return check 

    def build_module(self, closing_day, save_others, add_columns_customer=None, force_gen=False, add_tgs=True, level="nif_cliente", **kwargs):

        '''
        Return a dataframe with following columns
        ['nif_cliente',
         'NUM_CLIENTE',
         'msisdn',
         'rgu',
         'cod_estado_general',
         'srv_basic',
         'TARIFF',
         'segment_nif',
         'nb_fbb_services_nif',
         'nb_mobile_services_nif',
         'nb_tv_services_nif',
         'nb_prepaid_services_nif',
         'nb_bam_mobile_services_nif',
         'nb_fixed_services_nif',
         'nb_bam_services_nif',
         'nb_rgus_nif',
         'rgus_list',
         'tgs_days_until_f_fin_bi',
         'tgs_has_discount']
        :param spark:
        :param closing_day:
        :param add_tgs to add tgs info. if add_tgs=False, then module is considered as no default and cannot be saved!
        :param level: nif_cliente to compute aggregates by nif or num_cliente to compute aggregates by num_cliente
        :return:
        '''

        #add_columns_customer =[]# ["birth_date", "fecha_naci", 'CLASE_CLI_COD_CLASE_CLIENTE', 'X_CLIENTE_PRUEBA', "TIPO_DOCUMENTO"]

        print("[CustomerBase] build_module | closing_day={} force_gen={} add_tgs={} level={}".format(closing_day, force_gen, add_tgs, level))

        df_base = get_customer_base_segment(self.SPARK, closing_day, save_others, add_columns_customer=add_columns_customer, force_gen=force_gen, level=level)
        # ['NIF_CLIENTE', 'NUM_CLIENTE', 'msisdn', 'rgu', 'seg_pospaid_nif'] if level="nif_cliente"
        # ['NIF_CLIENTE', 'NUM_CLIENTE', 'msisdn', 'rgu', 'seg_pospaid_nc'] if level="num_cliente"


        level = level.lower()

        df_base = (df_base.withColumnRenamed(level.upper(), level))
        df_base_level = df_base.groupBy(level).agg(*([collect_list("rgu").alias("rgus_list")]))
        df_base = df_base.drop_duplicates([level, "msisdn"])
        df_base = df_base.join(df_base_level, on=[level], how="left")
        df_base = df_base.drop_duplicates(["msisdn"])
        df_base = df_base.drop_duplicates(['msisdn', 'nif_cliente', 'num_cliente'])


        if add_tgs:
            # use closing_day if it is a cycle. Otherwise, get the previous one
            closing_day_tgs = closing_day if is_cycle(closing_day) else get_previous_cycle(closing_day)

            from churn_nrt.src.data.tgs import TGS
            df_tgs = TGS(self.SPARK).get_module(closing_day_tgs, save=save_others, save_others=save_others, force_gen=force_gen)

            # - - - - - - - - - - - - - - - -
            df_base = df_base.join(df_tgs.select("msisdn", "tgs_days_until_f_fin_bi", "tgs_has_discount"), on=["msisdn"], how="left")

        else:
            print("[CustomerBase] build_module |  Generating module without tgs columns")

        from churn_nrt.src.data_utils.Metadata import apply_metadata
        df_base = apply_metadata(self.get_metadata(level), df_base)
        df_base = df_base.filter((~isnull(col('msisdn'))) & (~isnull(col(level))) & (~col('msisdn').isin('', ' ')) & (~col(level).isin('', ' ')))

        return df_base

    def get_metadata(self, level="nif_cliente"):

        if level.lower() == "nif_cliente":
            suffix = "nif"
        else:
            suffix = "nc"

        na_dict = {'rgu': "unknown", 'cod_estado_general': -1, 'srv_basic': "unknown", 'segment_'+suffix: "unknown", 'nb_fbb_services_'+suffix: 0, 'nb_mobile_services_'+suffix: 0, 'nb_tv_services_'+suffix: 0,
                   'nb_prepaid_services_'+suffix: 0, 'nb_bam_mobile_services_'+suffix: 0, 'nb_fixed_services_'+suffix: 0, 'nb_bam_services_'+suffix: 0, 'nb_rgus_'+suffix: 0, 'rgus_list': None,
                   'tgs_days_until_f_fin_bi': -1, 'tgs_has_discount': 0, 'TARIFF' : 'unknown'}

        cat_feats = ["rgu", "srv_basic", "segment_"+suffix, 'TARIFF']
        array_feats = ["rgus_list"]

        na_vals = [str(x) for x in na_dict.values()]

        data = {'feature': na_dict.keys(), 'imp_value': na_vals}

        metadata_df = (self.SPARK.createDataFrame(pd.DataFrame(data))
                            .withColumn('source', lit('customer_base'))
                            .withColumn('type', lit('numeric'))
                            .withColumn('type', when(col('feature').isin(*cat_feats), 'categorical').when(col('feature').isin(*array_feats), 'array').otherwise(col('type')))
                            .withColumn('level', lit(level)))

        return metadata_df

class CustomerAdditional(DataTemplate):

    DAYS_BEFORE = 90

    def __init__(self, spark, days_before=90):

        if days_before <= 0:
            print("[CustomerAdditional] __init__ | n_days must be greater than 0!")
            import sys
            sys.exit()

        self.DAYS_BEFORE = days_before
        DataTemplate.__init__(self, spark, "customer_additional/{}".format(self.DAYS_BEFORE))

    def is_default_module(self, *args, **kwargs):
        print("[CustomerBase] is_default_module | args: {} | kwargs: {}".format(args, kwargs))

        # if None --> then it can be saved
        check =  (kwargs.get('level', "nif_cliente").lower() == "nif_cliente")
        if not check:
            print("[CustomerBase] is_default_module | Module {} cannot be saved since level is different than nif_cliente".format(self.MODULE_NAME))
        return check

    def build_module(self, closing_day, save_others, force_gen=False, level="nif_cliente", **kwargs):
        '''
        Ending "-nif" is replaced by "-nc" when level="num_cliente"
        ['NIF_CLIENTE',
         'fecha_naci',
         'age',
         'age_disc',
         'prev_nb_rgus_nif',
         'prev_nb_fbb_services_nif',
         'prev_nb_mobile_services_nif',
         'prev_nb_tv_services_nif',
         'prev_nb_prepaid_services_nif',
         'prev_nb_bam_mobile_services_nif',
         'prev_nb_fixed_services_nif',
         'prev_nb_bam_services_nif',
         'inc_nb_fixed_services_nif',
         'inc_nb_prepaid_services_nif',
         'inc_nb_fbb_services_nif',
         'inc_nb_bam_services_nif',
         'inc_nb_tv_services_nif',
         'inc_nb_bam_mobile_services_nif',
         'inc_nb_rgus_nif',
         'inc_nb_mobile_services_nif',
         'flag_segment_changed',
         'flag_new_customer',
         'flag_dx',
         ]
        :param closing_day:
        :param kwargs:
        :return:
        '''

        level = level.upper()

        if level == "NIF_CLIENTE":
            suffix = "nif"
        else:
            suffix = "nc"

        from churn_nrt.src.utils.date_functions import move_date_n_days

        prev_date = move_date_n_days(closing_day, n=-self.DAYS_BEFORE)

        sel_cols = [level, 'segment_'+suffix, 'nb_rgus_'+suffix, 'nb_fbb_services_'+suffix, 'nb_mobile_services_'+suffix, 'nb_tv_services_'+suffix, 'nb_prepaid_services_'+suffix,
                    'nb_bam_mobile_services_'+suffix, 'nb_fixed_services_'+suffix, 'nb_bam_services_'+suffix]

        add_columns_customer = ["birth_date", "fecha_naci", 'CLASE_CLI_COD_CLASE_CLIENTE', 'X_CLIENTE_PRUEBA', "TIPO_DOCUMENTO"]

        ref_base = get_customer_base_segment(self.SPARK, closing_day, save_others=save_others, add_columns_customer=add_columns_customer,
                                             force_gen=force_gen, level=level).select(*(sel_cols+['birth_date', 'fecha_naci'])).drop_duplicates([level])

        ref_base = (ref_base.withColumn("birth_date", when((col("birth_date").isNull() | (col("birth_date") == 1753)), col("fecha_naci")).otherwise(col("birth_date")).cast(IntegerType()))
                .withColumn("age", when(col("birth_date") != 1753, lit(int(closing_day[:4])) - col("birth_date")).otherwise(-1)))

        ref_base = ref_base.withColumn("age_disc", when(col("age").isNull(), "other")
                                     .when((col("age") > 0) & (col("age") < 20), "<20")
                                     .when((col("age") >= 20) & (col("age") < 25), "[20-25)")
                                     .when((col("age") >= 25) & (col("age") < 30), "[25-30)")
                                     .when((col("age") >= 30) & (col("age") < 35), "[30-35)")
                                     .when((col("age") >= 35) & (col("age") < 40), "[35-40)")
                                     .when((col("age") >= 40) & (col("age") < 45), "[40-45)")
                                     .when((col("age") >= 45) & (col("age") < 50), "[45-50)")
                                     .when((col("age") >= 50) & (col("age") < 55), "[50-55)")
                                     .when((col("age") >= 55) & (col("age") < 60), "[55-60)")
                                     .when((col("age") >= 60) & (col("age") < 65), "[60-65)")
                                     .when(col("age") >= 65, ">=65").otherwise("other")).drop("birth_date")



        if self.VERBOSE:
            print('[CustomerBase] build_module | Number of elements in ref_base is {} - Num distinct {} in ref_base is {}'.format( ref_base.count(), level,
                                                                                                                                    ref_base.select(level).distinct().count()))

        prev_base = get_customer_base_segment(self.SPARK, prev_date, save_others=save_others, level=level).select(sel_cols).drop_duplicates([level])

        if self.VERBOSE:
            print('[CustomerBase] build_module | Number of elements in prev_base is {} - Num distinct {} in prev_base is {}'.format(prev_base.count(), level,
                                                                                                                                    prev_base.select(level).distinct().count()))


        # Renaming prev_base columns

        ren_cols = list(set(prev_base.columns) - {level})

        for c in ren_cols:
            prev_base = prev_base.withColumnRenamed(c, 'prev_' + c)

        fill_na_map = {'prev_segment_'+suffix: 'no_prev_segment', 'prev_nb_rgus_'+suffix: 0, 'prev_nb_fbb_services_'+suffix: 0, 'prev_nb_mobile_services_'+suffix: 0, 'prev_nb_tv_services_'+suffix: 0,
                       'prev_nb_prepaid_services_'+suffix: 0, 'prev_nb_bam_mobile_services_'+suffix: 0, 'prev_nb_fixed_services_'+suffix: 0, 'prev_nb_bam_services_'+suffix: 0}

        ref_base = ref_base.join(prev_base, [level.lower()], 'left').na.fill(fill_na_map)

        # Adding attributes

        for c in list(set(sel_cols) - {level, 'segment_'+suffix}):
            ref_base = ref_base.withColumn('inc_' + c, col(c) - col('prev_' + c))

        ref_base = ref_base \
            .withColumn('flag_segment_changed', when(col('segment_'+suffix) != col('prev_segment_'+suffix), 1.0).otherwise(lit(0.0))) \
            .withColumn('flag_new_customer', when(col('prev_segment_'+suffix) == 'no_prev_segment', 1.0).otherwise(lit(0.0))) \
            .withColumn('flag_dx', when((col('inc_nb_fbb_services_'+suffix) < 0) | (col('inc_nb_mobile_services_'+suffix) < 0) | (col('inc_nb_tv_services_'+suffix) < 0), 1.0).otherwise(lit(0.0)))


        # segment_nif and nb_XXX_services_nif belong to CustomerBase module. We do not select them to avoid repetitions
        sel_cols = [col_ for col_ in ref_base.columns if col_.startswith("flag_") or col_.startswith("prev_nb") or
                                                         col_.startswith("inc_nb") or col_ in ['fecha_naci', 'age', 'age_disc', level]]


        return ref_base.select(sel_cols)

    def get_metadata(self, level="nif_cliente"):

        if level == "nif_cliente":
            suffix = "nif"
        else:
            suffix = "nc"

        na_dict = {'fecha_naci': 1753, 'age': -1, 'age_disc': 'other', 'prev_nb_rgus_'+suffix: 0, 'prev_nb_fbb_services_'+suffix: 0, 'prev_nb_mobile_services_'+suffix: 0, 'prev_nb_tv_services_'+suffix: 0,
                   'prev_nb_prepaid_services_'+suffix: 0, 'prev_nb_bam_mobile_services_'+suffix: 0, 'prev_nb_fixed_services_'+suffix: 0, 'prev_nb_bam_services_'+suffix: 0, 'inc_nb_fixed_services_'+suffix: 0,
                   'inc_nb_prepaid_services_'+suffix: 0, 'inc_nb_fbb_services_'+suffix: 0, 'inc_nb_bam_services_'+suffix: 0, 'inc_nb_tv_services_'+suffix: 0, 'inc_nb_bam_mobile_services_'+suffix: 0, 'inc_nb_rgus_'+suffix: 0,
                   'inc_nb_mobile_services_'+suffix: 0, 'flag_segment_changed': 0, 'flag_new_customer': 0, 'flag_dx': 0}

        cat_feats = ["age_disc"]

        na_vals = [str(x) for x in na_dict.values()]

        data = {'feature': na_dict.keys(), 'imp_value': na_vals}

        import pandas as pd

        metadata_df = (self.SPARK.createDataFrame(pd.DataFrame(data))
            .withColumn('source', lit('customer_additional'))
            .withColumn('type', lit('numeric'))
            .withColumn('type', when(col('feature').isin(*cat_feats), 'categorical').otherwise(col('type')))
            .withColumn('level', lit('msisdn')))

        return metadata_df