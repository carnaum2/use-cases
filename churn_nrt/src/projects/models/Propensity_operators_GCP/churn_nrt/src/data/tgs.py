from pyspark.sql.functions import desc




def get_tgs(spark, closing_day):

    df_tgs = spark.read.load("/data/udf/vf_es/churn/extra_feats_mod/tgs/year={}/month={}/day={}".format(int(closing_day[:4]),
                                                                             int(closing_day[4:6]),
                                                                             int(closing_day[6:])))

    df_tgs = df_tgs.sort("nif_cliente", desc("tgs_days_until_fecha_fin_dto")).drop_duplicates(["nif_cliente"])
    return df_tgs