# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.sql import Window

from pyspark.sql.functions import (
    from_unixtime,
    unix_timestamp,
    collect_set, row_number, asc, desc, min as sql_min, concat, lpad, date_format, add_months, to_date, udf, col, array, sort_array, decode, when, lit, lower, translate, count, sum as sql_sum, max as sql_max, isnull, substring, size, length, desc)

from churn_nrt.src.utils.date_functions import move_date_n_days, move_date_n_cycles
from pyspark.sql import functions as F
import datetime as dt

from churn_nrt.src.data_utils.DataTemplate import DataTemplate

PATH_TGS_DIR = '/data/raw/vf_es/cvm/PREVEN_MATRIX/1.1/parquet/'

MOST_COMMON_DISCOUNTS = ["PQC50", "DPR50", "CN003", "DMF15", "DPR40", "DPR25", "DPC50", "PQC30", "DMR50", "CN002", ]

# On this date, the new "blindaje" computation started. To avoid mixing different "Blindaje" computation, the generation of this module
# is restricted to date later on "20190930"
FIRST_TGS_DATE = "20190930"

def get_first_date(spark):
    first_date = (spark.read.parquet('/data/raw/vf_es/cvm/PREVEN_MATRIX/1.1/parquet/')
        .withColumn('mydate', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
        .select(sql_min(col('mydate')).alias('first_date'))
        .rdd.first()['first_date'])
    return first_date



class TGS(DataTemplate):
    """
        The class has been implemented to treat address information
        :param _spark:
        :param _closing_day: String; last day of the period to be treated. Format 'YYYYMMDD'
    """


    def __init__(self, spark):
        DataTemplate.__init__(self, spark, "tgs")


    def build_module(self, closing_day, save_others, **kwargs):


        if closing_day < FIRST_TGS_DATE:
            print("[TGS] build_module | The computation of the new 'blindaje' starts on {}. Module for day {} could not be generated!".format(FIRST_TGS_DATE, closing_day))
            print("[TGS] build_module | The program will exit here to guarantee that empty modules are not generated")
            import sys
            sys.exit()

        # Use the previous YYYYMM
        d = datetime.strptime(closing_day, "%Y%m%d")
        newrefdate = d.replace(day=1) - timedelta(days=1)

        df_tgs = self.SPARK.read.parquet(PATH_TGS_DIR).where(
            concat(col("year"), lpad(col("month"), 2, "0"), lpad(col("day"), 2, "0")) <= datetime.strftime(newrefdate, '%Y%m%d'))

        w_blindaje = Window() \
            .partitionBy("nif") \
            .orderBy(desc("AGRUPMAX_F_FIN"), desc("year"), desc("month"), desc("day"))

        df_tgs_blindaje = (
            df_tgs.select("nif", "AGRUPMAX_CODIGO", "AGRUPMAX_F_INICIO", "AGRUPMAX_F_FIN", "AGRUPMAX_NMESES",
                          "FLAG_BLINDAJE", "TIPO_BLINDAJE", "year", "month", "day")
                .withColumn("rowNum", row_number().over(w_blindaje))
                .replace("", None)
                .where(col("rowNum") == 1)
                .drop("year", "month", "day", "rowNum")
        )

        w_descto = Window() \
            .partitionBy("nif") \
            .orderBy(desc("FECHA_FIN"), desc("year"), desc("month"), desc("day"))

        df_tgs_descto = (
            df_tgs.select("nif", "CLF_CODE", "FECHA_INICIO", "FECHA_FIN", "MESES_FIN_DTO", "year", "month", "day")
                .withColumn("rowNum", row_number().over(w_descto))
                .replace("", None)
                .where(col("rowNum") == 1)
                .drop("year", "month", "day", "rowNum")
        )

        df_tgs_clean = df_tgs_blindaje.join(df_tgs_descto, on="nif", how="inner")

        # date of 5 years from now:
        top_year = 5
        top_date_blindaje = datetime.strftime(d + relativedelta(years=top_year), '%Y%m%d')

        df_tgs = df_tgs_clean.withColumn(
            "AGRUPMAX_F_FIN",
            when(unix_timestamp("AGRUPMAX_F_FIN", "yyyyMMdd") > unix_timestamp(lit(top_date_blindaje), "yyyyMMdd"),
                 date_format(add_months(to_date("AGRUPMAX_F_INICIO", 'yyyyMMdd'), top_year * 12), "yyyyMMdd")
                 ).otherwise(col("AGRUPMAX_F_FIN")))

        for colmn in ['sum_ind_under_use', 'sum_ind_over_use']:
            df_tgs = df_tgs.withColumn(colmn, lit(None).cast(IntegerType()))
        for colmn in ['days_since_f_inicio_bi_exp', 'days_until_f_fin_bi_exp']:
            df_tgs = df_tgs.withColumn(colmn, lit(None).cast(DoubleType()))
        for colmn in ['stack', 'decil',
                      'clasificacion_uso', 'blindaje_bi', 'blinda_bi_n2', 'blinda_bi_n4',
                      'blindaje_bi_expirado', 'target_accionamiento', 'ind_riesgo_mm', 'sol_24m',
                      'ind_riesgo_max', 'blinda_bi_pos_n12', 'tg_marta', 'ind_riesgo_o2',
                      'ind_riesgo_mv']:
            df_tgs = df_tgs.withColumn(colmn, lit(None).cast(StringType()))

        df_tgs = (
            df_tgs.where(col("NIF").isNotNull())
                .withColumnRenamed("NIF", "NIF_CLIENTE")
                .withColumnRenamed("CLF_CODE", "DESCUENTO")
                .withColumn("FECHA_FIN",
                            when(~col("FECHA_FIN").isNull(), concat(col("FECHA_FIN"), lit("14"))).otherwise(
                                col("FECHA_FIN")))
                .withColumn("FECHA_INICIO",
                            when(~col("FECHA_INICIO").isNull(), concat(col("FECHA_INICIO"), lit("14"))).otherwise(
                                col("FECHA_INICIO")))
                .withColumnRenamed("MESES_FIN_DTO", "meses_fin_dto_ok")
        )

        from churn_nrt.src.utils.date_functions import compute_diff_days

        df_tgs = df_tgs.withColumn(
            "days_since_F_INICIO_BI",
            when(
                col("AGRUPMAX_F_INICIO").isNotNull(),
                compute_diff_days(
                    from_unixtime(unix_timestamp("AGRUPMAX_F_INICIO", "yyyyMMdd")),
                    from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")),
                    -10000,
                ),
            ).otherwise(-10000),
        )
        df_tgs = df_tgs.withColumn(
            "days_since_F_INICIO_DTO",
            when(
                col("FECHA_INICIO").isNotNull(),
                compute_diff_days(
                    from_unixtime(unix_timestamp("FECHA_INICIO", "yyyyMMdd")),
                    from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")),
                    -10000,
                ),
            ).otherwise(-10000),
        )
        df_tgs = df_tgs.withColumn(
            "days_until_F_FIN_BI",
            when(
                col("AGRUPMAX_F_FIN").isNotNull(),
                compute_diff_days(
                    from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")),
                    from_unixtime(unix_timestamp("AGRUPMAX_F_FIN", "yyyyMMdd")),
                    -10000
                ),
            ).otherwise(-10000),
        )
        df_tgs = df_tgs.withColumn(
            "days_until_FECHA_FIN_DTO",
            when(
                col("FECHA_FIN").isNotNull(),
                compute_diff_days(
                    from_unixtime(unix_timestamp(lit(closing_day), "yyyyMMdd")),
                    from_unixtime(unix_timestamp("FECHA_FIN", "yyyyMMdd")),
                    -10000,
                )
            ).otherwise(-10000),
        )

        df_tgs = df_tgs.withColumn(
            "HAS_DISCOUNT", when(col("DESCUENTO").isNotNull(), 1).otherwise(0)
        )

        df_tgs = df_tgs.withColumn(
            "discount_proc",
            when(col("descuento").isNull(), "NO_DISCOUNT")
                .when(
                col("descuento").isin(MOST_COMMON_DISCOUNTS), col("descuento")
            )
                .otherwise("OTHERS"),
        )

        df_tgs = df_tgs.drop("FECHA_FIN_DTO", "F_INICIO_BI", "F_FIN_BI", "F_INICIO_BI_EXP", "F_FIN_BI_EXP",
                             "TARGET_SEGUIMIENTO_BI", 'MESES_FIN_DTO', "MES_ACCIONAMIENTO", "agrupmax_codigo",
                             "agrupmax_nmeses", "AGRUPMAX_F_FIN", "agrupmax_f_inicio", "fecha_inicio", "fecha_fin")

        print("[TGS] build_module | TGs table for day {} contains {} rows and {} NIFs".format(closing_day, df_tgs.count(),
                                                                                                          df_tgs.select("NIF_CLIENTE").distinct().count()
                                                                                                          ))

        from churn_nrt.src.data.customers_data import Customer
        from churn_nrt.src.data.services_data import Service

        df_serv = Service(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others, force_gen=False)
        df_cust = Customer(self.SPARK).get_module(closing_day, save=save_others, save_others=save_others, force_gen=False)


        df_services = df_serv.join(df_cust.select('NUM_CLIENTE', 'NIF_CLIENTE'), ['NUM_CLIENTE'], 'inner') \
            .withColumnRenamed('NUM_CLIENTE', 'num_cliente') \
            .withColumnRenamed('NIF_CLIENTE', 'nif_cliente').select(["msisdn", "num_cliente", "nif_cliente"])

        df_tgs_2 = (
            df_tgs.join(
                df_services,
                on=["nif_cliente"],
                how="inner",
            )
                .where(
                (col("num_cliente").isNotNull())
            )
                .dropDuplicates()
        )

        df_newColumns = ['tgs_' + c.lower() if c not in ["msisdn", "num_cliente", "NIF_CLIENTE"] else c for c in df_tgs_2.columns]
        df_table = df_tgs_2.toDF(*df_newColumns)

        print("[TGS] build_module | TGs table for day {} contains: {} rows, {} msisdns, {} num_clients and {} NIFs".format(
                                                                                                          closing_day,
                                                                                                          df_table.count(),
                                                                                                          df_table.select("msisdn").distinct().count(),
                                                                                                          df_table.select("num_cliente").distinct().count(),
                                                                                                          df_table.select("NIF_CLIENTE").distinct().count()
                                                                                                          ))

        '''
        df_table = df_table \
            .withColumnRenamed("nif_cliente", "nif_cliente_tgs") \
            .withColumnRenamed("num_cliente", "num_cliente_tgs")
               
        if impute_nulls:
            null_map = self.set_module_metadata()
            df_table = Metadata().apply_metadata_dict(df_table, null_map)
        '''

        return df_table

    def get_metadata(self):
        fill_no_discount = ["tgs_descuento", "tgs_discount_proc", "tgs_tipo_blindaje"]
        fill_big_minus = ["tgs_days_until_fecha_fin_dto", "tgs_days_until_f_fin_bi",
                          "tgs_days_since_f_inicio_bi", "tgs_days_since_f_inicio_dto"]
        fill_minus_one = [
            "tgs_has_discount",
            "tgs_sum_ind_over_use",
            "tgs_sum_ind_under_use",
            "tgs_days_since_f_inicio_bi_exp",
            "tgs_days_until_f_fin_bi_exp",
        ]

        fill_unknown = [
            "tgs_stack",
            "tgs_clasificacion_uso",
            "tgs_blindaje_bi_expirado",
            "tgs_blindaje_bi",
            "tgs_blinda_bi_n4",
            "tgs_blinda_bi_n2",
            "tgs_decil",
            "tgs_target_accionamiento",
        ]

        fill_zero_str = [
            "tgs_meses_fin_dto_ok",
            "tgs_ind_riesgo_mm",
            "tgs_ind_riesgo_mv",
            "tgs_ind_riesgo_o2",
        ]

        fill_minus_str = [
            "tgs_tg_marta",
            "tgs_ind_riesgo_max",
            "tgs_sol_24m",
            "tgs_blinda_bi_pos_n12",
            "tgs_flag_blindaje"
        ]

        module = "tgs"
        na_map = dict(
            [
                (k, ("", "id", module))
                for k in [
                "msisdn", "num_cliente_tgs", "nif_cliente_tgs"
            ]
            ]
            + [(k, (-1, "numerical", module)) for k in fill_minus_one]
            + [(k, ("NO_DISCOUNT", "categorical", module)) for k in fill_no_discount]
            + [(k, ('unknown', "categorical", module)) for k in fill_unknown]
            + [(k, ("0", "categorical", module)) for k in fill_zero_str]
            + [(k, ("-1", "categorical", module)) for k in fill_minus_str]
            + [(k, (-10000, "numerical", module)) for k in fill_big_minus]
        )

        import pandas as pd

        metadata_df = self.SPARK.createDataFrame(pd.DataFrame(na_map))

        return metadata_df