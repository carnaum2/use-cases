
from pykhaos.utils.date_functions import (get_last_day_of_month, get_next_month)
from pyspark.sql.functions import (udf,col,max as sql_max, when, isnull, concat, lpad, trim, lit, sum as sql_sum,
                                   length, upper, datediff, to_date, from_unixtime, unix_timestamp, substring, translate)
import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()



def get_contacts_and_responses(spark, campaign_start, campaign_end):

    logger.info("\t\tGetting contacts and responses from {} to {}".format(campaign_start, campaign_end))

    # Now, we read raw_es.campaign_msisdncontacthist
    # with yyyymm predicates and other stupid business filters
    # that I do not fully understand:
    contacts = (spark.read.table("raw_es.campaign_msisdncontacthist")
                .where(col("contactdatetime") >= campaign_start)
                .where(col("contactdatetime") < campaign_end)
                .where(col("campaigncode").isin(['AUTOMMES_PXXXP_MIG_PROPENSOS']))
                .where(~(col("canal").like("PO%")))
                .where(~(col("canal").like("NBA%")))
                .where(col("canal")=="TEL")
                .where(col("flag_Borrado") == 0)
                )

    # We read raw_es.campaign_msisdnresponsehist:
    responses = (spark.read.table("raw_es.campaign_msisdnresponsehist"))

    # We are going to join contacts DF with responses DF, and they
    # happen to have columns with same names (but not same data),
    # so we rename all columns in responses DF, adding responses_
    # at the beggining:
    responses_columns = [(_column,"responses_"+_column) for _column in responses.columns]

    for existing, new in responses_columns:
        responses = responses.withColumnRenamed(existing, new)

    # Beautiful join. I do not expect you to understand
    # it, because neither do I. I just translated some
    # Teradata Query that VF Spain's CVM department uses
    # to Spark DF syntax. It runs quite fast...
    df_contacts_and_responses = (contacts.join(responses,
                                           how="left_outer",
                                           on=(contacts["TREATMENTCODE"]==responses["responses_TREATMENTCODE"])
                                              & (contacts["MSISDN"]==responses["responses_MSISDN"])
                                              & (contacts["CampaignCode"]==responses["responses_CampaignCode"])
                                           )
                                      .groupBy("MSISDN",
                                               "CAMPAIGNCODE",
                                               "CREATIVIDAD",
                                               "CELLCODE",
                                               "CANAL",
                                               "contactdatetime",
                                               "responses_responsedatetime")
                                      .agg(sql_max("responses_responsedatetime"))
                                      .select(col("MSISDN"),
                                              col("CAMPAIGNCODE"),
                                              col("CREATIVIDAD"),
                                              col("CELLCODE"),
                                              col("CANAL"),
                                              col("contactdatetime").alias("DATEID"),
                                              when(isnull(col("max(responses_responsedatetime)")), "0")
                                                  .otherwise("1").alias("EsRespondedor")
                                             )
                             ).withColumnRenamed("msisdn","msisdn_contact")

    col_ = 'DATEID'
    from pykhaos.utils.date_functions import get_last_day_of_month
    closing_day = campaign_end.split(" ")[0].replace("-","")
    print("Computing campaign days using closing_day {}".format(closing_day))
    df_contacts_and_responses = (df_contacts_and_responses.withColumn("days_since_{}".format(col_), when(col("DATEID").isNotNull(), datediff(
                                                                     from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")),
                                                                     from_unixtime(unix_timestamp(translate(substring(col("DATEID"), 1, 10),
                                                                               "-", ""), "yyyyMMdd"))
                                                                 ).cast("double")).otherwise(-1)))
    from pyspark.sql.functions import desc
    df_contacts_and_responses = df_contacts_and_responses.sort(desc("days_since_{}".format(col_)))
    df_contacts_and_responses = df_contacts_and_responses.drop_duplicates(["msisdn_contact"])


    df_contacts_and_responses = df_contacts_and_responses.drop("DATEID")

    return df_contacts_and_responses