#Imports from churn_nrt

from churn_nrt.src.utils.date_functions import (
    get_last_day_of_month,
    move_date_n_yearmonths)

from churn_nrt.src.data.customer_base import CustomerBase
from churn_nrt.src.data.sopos_dxs import MobPort
from pyspark.sql import functions as F
from pyspark.sql.functions import when
from churn_nrt.src.data.navcomp_data import NavCompData

from churn_nrt.src.projects.models.propensity_operators.version2.input_vars import *


def get_ids_multiclass(date, ids_path, mode, test,spark):

    year = date[0:4]
    month = str(int(date[4:6]))
    day = str(int(date[6:8]))

    print("[Info]: Getting complete ids for the date given")

    ids_complete = spark.read.load(
        ids_path + "/year=" + year + "/month=" + month + "/day=" + day
    )  # read complete ids

    print("[Info]: Joining complete ids with customer base")

    cust_base = (
        CustomerBase(spark)
            .get_module(date, save=False, save_others=False, force_gen=True)
            .filter(F.col("rgu") == "mobile")
            .select("msisdn")
    )  # get mobile customer base for training

    ids_base = ids_complete.join(
        cust_base, on="msisdn", how="inner"
    )  # join ids with customer base

    print("[Info]: Filtering training set by active customers for three months"
          )

    previous_year_month = move_date_n_yearmonths(
        date[0:6], -3
    )  # 'YYYYMM' of three months ago

    # if the day of the current date is the last day of the month
    if (date[6] == "3") | (date[6:8] == "28") | (date[6:8] == "29"):

        previous_date = get_last_day_of_month(
            previous_year_month + "01"
        )  # returns last day of three months ago

    else:

        previous_date = (
                previous_year_month + date[6:8]
        )  # returns same day of three months ago

    active_customers_three_months = (
        CustomerBase(spark)
            .get_module(previous_date, save=False, save_others=False, force_gen=True)
            .filter(F.col("rgu") == "mobile")
            .select("msisdn")
            .distinct()
    )# get mobile clients who were in the base three months ago

    # join active customers three months ago with ids
    ids_active = ids_base.join(active_customers_three_months, on="msisdn", how="inner")

    print('[Info]: IDS colums: ',len(ids_active.columns))

    #print('[Info]: Join with navcomp attributes')

    #navcomp = NavCompData(spark, window_length=30).get_module(date, save=True)

    #ids_active = ids_active.join(navcomp, on='msisdn', how='left').fillna(0)

    #print('[Info]: IDS colums with navcomp: ', len(ids_active.columns))

    if ((mode == 'evaluation') | ((mode == 'production') & (test == 'False'))):

        print("[Info]: If we are getting train set or test set in mode evaluation, we need to label")

        print("[Info]: Getting portouts")

        portab_table = (  # MobPort new
            MobPort(spark, churn_window=30)
                .get_module(date, save=False, save_others=False, force_gen=True)
                .select("msisdn", "target_operator")
        )  # get portouts of the next 30 days

        portab_table = portab_table.withColumn('target_operator',
                                               when(F.col('target_operator') == 'movistar',
                                                    '2').otherwise(
                                                   when(F.col('target_operator') == 'orange',
                                                        '3').otherwise(
                                                       when(F.col('target_operator') == 'masmovil',
                                                             '1').otherwise('4'))))


        ids_labelled = ids_active.join(portab_table, on="msisdn", how="left").fillna(
            "none")  # label customer base: join with portouts and replace NA by 'none' (customers who do not churn)



        print("[Info]: Deleting customers who do not churn (target_operators==none)")

        ids_labelled = ids_labelled.filter(
            F.col("target_operator") != "none"
        )  # delete customers who do not churn for training the model

        print("[Info]: Portabilities grouped: ")

        ids_labelled.groupby('target_operator').count().show()

        
    if test == 'False':  # solo para los datos de train: select numeric

        print('[Info]: Get numerical variables of the ids...')

        numerical = get_num_vars(ids_active)

        print('[Info]: IDS numerical colums: ',len(numerical))

        corr_vars = drop_corr_vars_70()
        #corr_vars = drop_corr_vars_70_new()

        print('[Info]: Select variables for the model')

        var_numeric = [variable for variable in numerical if variable not in corr_vars]

        ids_final = ids_labelled.select(
            [
                c
                for c in ids_labelled.columns
                if c in ["msisdn"] + var_numeric + ['target_operator']
            ]
        ).withColumnRenamed('target_operator', "label")


    else:

        if mode == 'production':  # if we are in production and test, we do not have a column label to select

            print("[Info]: If mode is production and we are getting test set, we do not need to label")

            ids_final = ids_active


        else:

            ids_final = ids_labelled.select([c
                                             for c in ids_labelled.columns
                                             if c in ids_active.columns + ['target_operator']]
                                            ).withColumnRenamed('target_operator', "label")

    print('[Info]: Number of variables: ', len(ids_final.columns))

    return ids_final