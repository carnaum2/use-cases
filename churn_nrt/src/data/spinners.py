
from pyspark.sql.functions import (col,
                                   decode,
                                   when,
                                   lit,
                                   lower,
                                   concat,
                                   translate,
                                   count,
                                   sum as sql_sum,
                                   max as sql_max,
                                   min as sql_min,
                                   avg as sql_avg,
                                   greatest,
                                   least,
                                   isnull,
                                   isnan,
                                   struct,
                                   substring,
                                   size,
                                   length,
                                   year,
                                   month,
                                   dayofmonth,
                                   unix_timestamp,
                                   date_format,
                                   from_unixtime,
                                   datediff,
                                   to_date,
                                   desc,
                                   asc,
                                   countDistinct,
                                   row_number,
                                   regexp_replace,
                                   upper,
                                   trim,
                                   array,
                                   create_map,
                                   randn,
                                   variance,
                                   from_unixtime, unix_timestamp, udf, array, sort_array, decode, when)
from churn_nrt.src.data_utils.DataTemplate import DataTemplate
import itertools

import pandas as pd

class Spinners(DataTemplate):

    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "spinners")


    def build_module(self, closing_day, **kwargs):

        sopodf = (self.SPARK.read
            .table("raw_es.portabilitiesinout_sopo_solicitud_portabilidad") \
            .select("sopo_ds_cif", "sopo_ds_msisdn1", "sopo_ds_fecha_solicitud", "esta_co_estado", "sopo_co_solicitante") \
            .withColumn("formatted_date", from_unixtime(unix_timestamp(col("sopo_ds_fecha_solicitud"), "yyyy-MM-dd"))) \
            .filter(col("formatted_date") >= from_unixtime(unix_timestamp(lit("20020101"), "yyyyMMdd"))) \
            .withColumn("ref_date", from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd"))) \
            .filter(col("formatted_date") < col("ref_date")) \
            .withColumn("destino", lit("otros")) \
            .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%telefonica%")) | (lower(col("sopo_co_solicitante")).like("%movistar%")), "movistar").otherwise(col("destino"))) \
            .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%eplus%")) | (lower(col("sopo_co_solicitante")).like("%e-plus% ")),"simyo").otherwise(col("destino"))) \
            .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%amena%")), "orange").otherwise(col("destino"))) \
            .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%jazztel%")), "jazztel").otherwise(col("destino"))) \
            .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%xfera%")) | (lower(col("sopo_co_solicitante")).like("%yoigo%")), "yoigo").otherwise(col("destino"))) \
            .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%mas movil%")) | (lower(col("sopo_co_solicitante")).like("%masmovil%")) | (lower(col("sopo_co_solicitante")).like("%republica%")), "masmovil").otherwise(col("destino"))) \
            .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%pepephone%")), "pepephone").otherwise(col("destino"))) \
            .withColumn("destino", when((lower(col("sopo_co_solicitante")).like("%mundor%")) | (lower(col("sopo_co_solicitante")).like("%euskaltel%")),"r_euskal").otherwise(col("destino")))
            .withColumn("destino",when(((lower(col("sopo_co_solicitante")) == "")) | ((lower(col("sopo_co_solicitante")) == " ")), "unknown").otherwise(col("destino")))
            .select("sopo_ds_cif", "sopo_ds_msisdn1", "formatted_date", "ref_date", "esta_co_estado", "destino"))

        feats1df = (sopodf
                    .select("sopo_ds_cif", "sopo_ds_msisdn1", "formatted_date", "ref_date")
                    .distinct()
                    .withColumn("days_since_port", datediff(col("ref_date"), col("formatted_date")).cast("double"))
                    .groupBy("sopo_ds_cif")
                    .agg(count("*").alias("nif_port_number"),
                         sql_min("days_since_port").alias("nif_min_days_since_port"),
                         sql_max("days_since_port").alias("nif_max_days_since_port"),
                         sql_avg("days_since_port").alias("nif_avg_days_since_port"),
                         variance("days_since_port").alias("nif_var_days_since_port"),
                         countDistinct("sopo_ds_msisdn1").alias("nif_distinct_msisdn"))
                    .withColumn("nif_var_days_since_port", when(isnan(col("nif_var_days_since_port")), 0.0).otherwise(col("nif_var_days_since_port")))
                    .withColumn("nif_port_freq_per_day", col("nif_port_number").cast("double")/col("nif_max_days_since_port").cast("double"))
                    .withColumn("nif_port_freq_per_day", when(isnan(col("nif_port_freq_per_day")), 0.0).otherwise(col("nif_port_freq_per_day")))
                    .withColumn("nif_port_freq_per_msisdn", col("nif_port_number").cast("double") / col("nif_distinct_msisdn").cast("double"))
                    .withColumn("nif_port_freq_per_msisdn", when(isnan(col("nif_port_freq_per_msisdn")), 0.0).otherwise(col("nif_port_freq_per_msisdn")))
                    )


        # To compute the feats related to the state and the destinations of the port-out requests:
        # destinos: all the possible destinations
        # estados: all the possible states
        # destino_estado: all the possible pairs
        # values: pair to string resulting in a list with all the possible levels of the combination

        destinos = ["movistar", "simyo", "orange", "jazztel", "yoigo", "masmovil", "pepephone", "reuskal", "unknown", "otros"]

        estados = ["ACON", "ASOL", "PCAN", "ACAN", "AACE", "AENV", "APOR", "AREC"]

        import itertools

        values = [p[0] + "_" + p[1] for p in list(itertools.product(destinos, estados))]

        feats2df = (sopodf
                    .select("sopo_ds_cif", "sopo_ds_msisdn1", "formatted_date", "esta_co_estado", "destino")
                    .distinct()
                    .withColumn("to_pivot", concat(col("destino"), lit("_"), col("esta_co_estado")))
                    .groupBy("sopo_ds_cif")
                    .pivot("to_pivot", values)
                    .agg(count("*"))
                    .na.fill(0)
                    .withColumn("total_acan", reduce(lambda a, b: a + b, [col(x) for x in values if("ACAN" in x)]))
                    .withColumn("total_apor", reduce(lambda a, b: a + b, [col(x) for x in values if ("APOR" in x)]))
                    .withColumn("total_arec", reduce(lambda a, b: a + b, [col(x) for x in values if ("AREC" in x)]))
                    .withColumn("total_movistar", reduce(lambda a, b: a + b, [col(x) for x in values if ("movistar" in x)]))
                    .withColumn("total_simyo", reduce(lambda a, b: a + b, [col(x) for x in values if ("simyo" in x)]))
                    .withColumn("total_orange", reduce(lambda a, b: a + b, [col(x) for x in values if ("orange" in x)]))
                    .withColumn("total_jazztel", reduce(lambda a, b: a + b, [col(x) for x in values if ("jazztel" in x)]))
                    .withColumn("total_yoigo", reduce(lambda a, b: a + b, [col(x) for x in values if ("yoigo" in x)]))
                    .withColumn("total_masmovil", reduce(lambda a, b: a + b, [col(x) for x in values if ("masmovil" in x)]))
                    .withColumn("total_pepephone", reduce(lambda a, b: a + b, [col(x) for x in values if ("pepephone" in x)]))
                    .withColumn("total_reuskal", reduce(lambda a, b: a + b, [col(x) for x in values if ("reuskal" in x)]))
                    .withColumn("total_unknown", reduce(lambda a, b: a + b, [col(x) for x in values if ("unknown" in x)]))
                    .withColumn("total_otros", reduce(lambda a, b: a + b, [col(x) for x in values if ("otros" in x)]))
                    .withColumn("num_distinct_operators", reduce(lambda a, b: a + b, [(col("total_" + x) > 0).cast("double") for x in destinos]))
                    )

        nifsopodf = feats1df \
            .join(feats2df, ["sopo_ds_cif"], "inner") \
            .withColumnRenamed("sopo_ds_cif", "nif_cliente")

        return nifsopodf

    def get_metadata(self):

        na_map_count_feats = dict(
            [("nif_port_number", 0.0),
             ("nif_min_days_since_port", -1.0),
             ("nif_max_days_since_port", 100000.0),
             ("nif_avg_days_since_port", 100000.0),
             ("nif_var_days_since_port", -1.0),
             ("nif_distinct_msisdn", 0.0),
             ("nif_port_freq_per_day", 0.0),
             ("nif_port_freq_per_msisdn", 0.0)])

        destinos = ["movistar", "simyo", "orange", "jazztel", "yoigo", "masmovil", "pepephone", "reuskal", "unknown", "otros"]

        estados = ["ACON", "ASOL", "PCAN", "ACAN", "AACE", "AENV", "APOR", "AREC"]

        na_map_destino_estado_feats = dict([(p[0] + "_" + p[1], 0.0) for p in list(itertools.product(destinos, estados))])

        total_feats = ["total_acan", "total_apor", "total_arec", "total_movistar", "total_simyo", "total_orange", "total_jazztel", "total_yoigo",
                       "total_masmovil", "total_pepephone", "total_reuskal", "total_unknown", "total_otros", "num_distinct_operators"]

        na_map = dict([(f, 0.0) for f in total_feats])

        na_map.update(na_map_count_feats)
        na_map.update(na_map_destino_estado_feats)

        feats = na_map.keys()

        na_vals = [str(x) for x in na_map.values()]

        cat_feats = []

        data = {'feature': feats, 'imp_value': na_vals}

        metadata_df = (self.SPARK.createDataFrame(pd.DataFrame(data))
            .withColumn('source', lit('spinners'))
            .withColumn('type', lit('numeric'))
            .withColumn('type', when(col('feature').isin(cat_feats), 'categorical').otherwise(col('type')))
            .withColumn('level', lit('nif')))

        return metadata_df
