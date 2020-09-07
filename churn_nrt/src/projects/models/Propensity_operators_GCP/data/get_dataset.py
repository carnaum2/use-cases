from my_propensity_operators.churn_nrt.src.utils.date_functions import (
    get_last_day_of_month,
    move_date_n_yearmonths,
)
from my_propensity_operators.churn_nrt.src.data.customer_base import CustomerBase
from my_propensity_operators.churn_nrt.src.data.sopos_dxs import MobPort
from pyspark.sql.functions import col, when
from my_propensity_operators.utils.input_vars import get_input_vars



def get_ids(operator, date, spark, ids_path, test, mode, confs, logger):

    """
    Get ids labelled (portauts of the next month) for the operator specified (different operators have different input variables).
    We filter by mobile customers and customers who have been active for three months.
    If mode=='production' and test='True', we do not label the ids. If we are in mode 'evaluation', we label train and test set

    :param operator: operator for which we want to run the model
    :type operator: string (i.e. 'masmovil')
    :param date: date that we will use for training or testing the model
    :type date: string (i.e. '20191231')
    :param spark: sparkSession
    :type spark: str
    :param ids_path: path where the ids is saved
    :type ids_path: string
    :param test: whether we are getting the test set or not
    :type test: string ('True' if we are getting the test set)
    :param mode: whether we are in mode evaluation or production
    :type mode: string ('production' if we are in mode production)
    :return: dataframe for the operator and the date given
    :rtype: :class:`pyspark.sql.DataFrame`

    """

    year = date[0:4]

    if date[4] == "0":

        month = date[5]

    else:

        month = date[4:6]

    day = date[6:8]

    logger.info("Getting complete ids for the date given")

    ids_complete = spark.read.load(
        ids_path + "/year=" + year + "/month=" + month + "/day=" + day
    )  # read complete ids

    logger.info("Joining complete ids with customer base")

    cust_base = (
        CustomerBase(spark,confs)
            .get_module(date)
            .filter(col("rgu") == "mobile")
            .select("msisdn")
    )  # get mobile customer base for training

    ids_base = ids_complete.join(
        cust_base, on="msisdn", how="inner"
    )  # join ids with customer base

    logger.info(
        "Filtering training set by active customers for three months"
    )  #

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
        CustomerBase(spark, confs)
            .get_module(previous_date)
            .filter(col("rgu") == "mobile")
            .select("msisdn")
            .distinct()
    )
    # get mobile clients who were in the base three months ago

    # join active customers three months ago with ids
    ids_active = ids_base.join(active_customers_three_months, on="msisdn", how="inner")

    logger.info("Selecting input variables for each operator")

    var_operator = get_input_vars(operator)

    logger.info("If mode is production, we do not need to label")

    if (mode == "production") & (
            test == "True"):  # if we are in production and test, we do not have a column label to select

        ids_final_operator = ids_active.select(
            [c for c in ids_active.columns if c in ["msisdn"] + var_operator]
        )

    else:

        logger.info("Since mode is evaluation, we need to label")

        logger.info("Getting portouts")

        portab_table = (
            MobPort(spark, confs, churn_window=30)
                .get_module(date)
                .select("msisdn", "target_operator")
        )  # get portouts of the next 30 days

        ids_labelled = ids_active.join(portab_table, on="msisdn", how="left").fillna(
            "none"
        )  # label customer base: join with portouts and replace NA by 'none' (customers who do not churn)

        logger.info("Distribution of portouts by operator")

        ids_labelled.groupby(
            "target_operator"
        ).count().show()  # show distribution of the target

        logger.info(
            "Deleting customers who do not churn (target_operators==none)"
        )

        ids_labelled = ids_labelled.filter(
            col("target_operator") != "none"
        )  # delete customers who do not churn for training the model

        logger.info("Adding one label for each operator")

        ids_labelled_final = (
            ids_labelled.withColumn(
                "masmovil",  # add a binary label for each operator
                when(ids_labelled["target_operator"] == "masmovil", 1).otherwise(0),
            )
                .withColumn(
                "movistar",
                when(ids_labelled["target_operator"] == "movistar", 1).otherwise(0),
            )
                .withColumn(
                "orange",
                when(ids_labelled["target_operator"] == "orange", 1).otherwise(0),
            )
                .withColumn(
                "others",
                when(ids_labelled["target_operator"] == "others", 1).otherwise(0),
            )
        )

        ids_final_operator = ids_labelled_final.select(
            [
                c
                for c in ids_labelled_final.columns
                if c in ["msisdn"] + var_operator + [operator]
            ]
        ).withColumnRenamed(operator, "label")

    return ids_final_operator