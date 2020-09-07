
from pyspark.sql.functions import col, concat_ws, lit, upper, when, count as sql_count, countDistinct, desc, upper
import csv
import os



def get_buckets(spark,):
    from churn.utils.constants import CHURN_DELIVERIES_DIR
    with open(os.path.join(CHURN_DELIVERIES_DIR, "amdocs_informational_dataset", "param",
                           "Agrup_Buckets_unific.txt")) as f:

        #with open(os.path.join("/var/SP/data/home/csanc109/src/devel/amdocs_informational_dataset", 'param', 'Agrup_Buckets_unific.txt')) as f:
        # Identify characteristics of text file (header, delimiter, ...)
        dialect = csv.Sniffer().sniff(f.read(1024))
        f.seek(0)
        # Read text files based on discovered characteristics
        reader = csv.reader(f, dialect)
        data_raw = [r for r in reader]
        header = data_raw.pop(0) # remove header
        data = [r for r in data_raw if len(r) == len(header)]
    buckets = spark.createDataFrame(data, header)

    # Unify buckets that differ in whitespaces or casing
    for c in ['Bucket', 'Sub_Bucket']:
        buckets_l = buckets.select(c).distinct().rdd.map(lambda x: x[c]).collect()
        for i in range(len(buckets_l)-1):
            for j in range(i+1, len(buckets_l)):
                if buckets_l[i].strip().lower() == buckets_l[j].strip().lower():
                    dest = min(buckets_l[i].strip(), buckets_l[j].strip())
                    if buckets_l[i] != dest:
                        buckets = buckets.withColumn(c,  when(buckets[c] == buckets_l[i], dest).otherwise(buckets[c]) )
                    if buckets_l[j] != dest:
                        buckets = buckets.withColumn(c,  when(buckets[c] == buckets_l[j], dest).otherwise(buckets[c]) )

    buckets = buckets.distinct()
    buckets = buckets.fillna('NA', ['Bucket', 'Sub_Bucket'])

    buckets = buckets.withColumn('Bucket_Sub_Bucket', concat_ws('_', lit('Bucket_Sub_Bucket'), buckets.Bucket, buckets.Sub_Bucket))
    buckets = buckets.withColumn('Bucket', concat_ws('_', lit('Bucket'), buckets.Bucket))
    buckets = buckets.withColumn('Sub_Bucket', concat_ws('_', lit('Sub_Bucket'), buckets.Sub_Bucket))

    buckets = (buckets.withColumn('INT_Tipo', upper(col('INT_Tipo')))
                        .withColumn('INT_Subtipo', upper(col('INT_Subtipo')))
                        .withColumn('INT_Razon', upper(col('INT_Razon')))
                        .withColumn('INT_Resultado', upper(col('INT_Resultado'))))

    return buckets





def get_custom_buckets(spark):
    FILENAME = '/user/csanc109/projects/churn/data/ccc_model/buckets_csanc109.csv'
    df_buckets_custom = spark.read.option("delimiter", ";").option("header", True).csv(FILENAME)
    df_buckets_custom = df_buckets_custom.withColumn("INT_Razon", lit("NA"))\
                                         .withColumn("INT_Resultado",lit("NA"))\
                                         .withColumn("Bucket_Sub_Bucket", lit("NA"))\
                                         .withColumn("Stack", lit("NA"))
    df_buckets_custom = df_buckets_custom.drop("count")
    return df_buckets_custom




def fill_missing_buckets(spark, df_all):

    df_buckets = get_buckets(spark)

    df_tipo = df_buckets.where(col("Bucket")!="NA").groupby("INT_Tipo").agg(sql_count("*").alias("count"), countDistinct("Bucket").alias("distinct")).sort(desc("count"))
    df_tipo = df_tipo.select("INT_Tipo").where(col("distinct")==1).join(df_buckets.select(["INT_Tipo", "Bucket"]).distinct(), ["INT_Tipo"], "left")

    df_tiposubtipo = df_buckets.where(col("Bucket")!="NA").groupby("INT_Tipo", "INT_Subtipo").agg(sql_count("*").alias("count"), countDistinct("Bucket").alias("distinct")).sort(desc("count"))
    df_tiposubtipo = df_tiposubtipo.select("INT_Tipo", "INT_Subtipo").where(col("distinct")==1).join(df_buckets.select(["INT_Tipo", "INT_Subtipo", "Bucket"]).distinct(), ["INT_Tipo", "INT_Subtipo"], "left")

    df_tiposubtiporazon = df_buckets.where(col("Bucket")!="NA").groupby("INT_Tipo", "INT_Subtipo", "INT_Razon").agg(sql_count("*").alias("count"), countDistinct("Bucket").alias("distinct")).sort(desc("count"))
    df_tiposubtiporazon = df_tiposubtiporazon.select("INT_Tipo", "INT_Subtipo", "INT_Razon").where(col("distinct")==1).join(df_buckets.select(["INT_Tipo", "INT_Subtipo", "INT_Razon", "Bucket"]).distinct(), ["INT_Tipo", "INT_Subtipo", "INT_Razon"], "left")

    df_tiporesultado = df_buckets.where(col("Bucket")!="NA").groupby("INT_Tipo", "INT_Resultado").agg(sql_count("*").alias("count"), countDistinct("Bucket").alias("distinct")).sort(desc("count"))
    df_tiporesultado = df_tiporesultado.select("INT_Tipo", "INT_Resultado").where(col("distinct")==1).join(df_buckets.select(["INT_Tipo", "INT_Resultado", "Bucket"]).distinct(), ["INT_Tipo", "INT_Resultado"], "left")

    def union_all(dfs):
        if len(dfs) > 1:
            return dfs[0].unionAll(union_all(dfs[1:]))
        else:
            return dfs[0]

    df_tipo = df_tipo.withColumnRenamed("Bucket", "Bucket_tipo").withColumnRenamed("INT_Tipo", "INT_Tipo_tipo")

    df_join1= df_all.join(df_tipo.select("Bucket_tipo", "INT_Tipo_tipo"),
                      on=(df_all["INT_Tipo"]==df_tipo["INT_Tipo_tipo"]),
                      how="left")

    df_join1 = df_join1.withColumn("Bucket", when((col("Bucket")=="NA") | (col("Bucket").isNull()), col("Bucket_tipo")).otherwise(col("Bucket")))

    df_tiposubtipo = df_tiposubtipo.withColumnRenamed("Bucket", "Bucket_subtipo").withColumnRenamed("INT_Tipo", "INT_Tipo_subtipo").withColumnRenamed("INT_Subtipo", "INT_Subtipo_subtipo")

    df_join2= df_join1.join(df_tiposubtipo.select("Bucket_subtipo", "INT_Tipo_subtipo", "INT_Subtipo_subtipo"),
                      on=((df_join1["INT_Tipo"]==df_tiposubtipo["INT_Tipo_subtipo"])&(df_join1["INT_Subtipo"]==df_tiposubtipo["INT_Subtipo_subtipo"])),
                      how="left")
    df_join2 = df_join2.withColumn("Bucket", when((col("Bucket")=="NA") | (col("Bucket").isNull()), col("Bucket_subtipo")).otherwise(col("Bucket")))

    df_tiposubtiporazon = df_tiposubtiporazon.withColumnRenamed("Bucket", "Bucket_subtiporazon").withColumnRenamed("INT_Tipo", "INT_Tipo_subtiporazon").withColumnRenamed("INT_Subtipo", "INT_Subtipo_subtiporazon").withColumnRenamed("INT_Razon", "INT_Razon_subtiporazon")

    df_join3= df_join2.join(df_tiposubtiporazon.select("Bucket_subtiporazon", "INT_Tipo_subtiporazon", "INT_Subtipo_subtiporazon", "INT_Razon_subtiporazon"),
                      on=((df_join2["INT_Tipo"]==df_tiposubtiporazon["INT_Tipo_subtiporazon"])&(df_join2["INT_Subtipo"]==df_tiposubtiporazon["INT_Subtipo_subtiporazon"])&(df_join2["INT_Razon"]==df_tiposubtiporazon["INT_Razon_subtiporazon"])),
                      how="left")

    df_join3 = df_join3.withColumn("Bucket", when((col("Bucket")=="NA") | (col("Bucket").isNull()), col("Bucket_subtiporazon")).otherwise(col("Bucket")))

    df_tiporesultado = df_tiporesultado.withColumnRenamed("Bucket", "Bucket_tiporesultado").withColumnRenamed("INT_Tipo", "INT_Tipo_tiporesultado").withColumnRenamed("INT_Resultado", "INT_Resultado_tiporesultado")

    df_join4= df_join3.join(df_tiporesultado.select("Bucket_tiporesultado", "INT_Tipo_tiporesultado", "INT_Resultado_tiporesultado"),
                      on=((df_join3["INT_Tipo"]==df_tiporesultado["INT_Tipo_tiporesultado"])&(df_join3["INT_Resultado"]==df_tiporesultado["INT_Resultado_tiporesultado"])),
                      how="left")

    df_join4 = df_join4.withColumn("Bucket", when((col("Bucket")=="NA") | (col("Bucket").isNull()), col("Bucket_tiporesultado")).otherwise(col("Bucket")))

    df_buckets_custom = get_custom_buckets(spark)

    df_buckets_custom = df_buckets_custom.withColumnRenamed("Bucket", "Bucket_custom").withColumnRenamed("INT_Tipo", "INT_Tipo_custom").withColumnRenamed("INT_Subtipo", "INT_Subtipo_custom").withColumnRenamed("INT_Razon", "INT_Razon_custom")

    df_join5= df_join4.join(df_buckets_custom.select("Bucket_custom", "INT_Tipo_custom", "INT_Subtipo_custom", "INT_Razon_custom"),
                      on=((df_join4["INT_Tipo"]==df_buckets_custom["INT_Tipo_custom"])&(df_join4["INT_Subtipo"]==df_buckets_custom["INT_Subtipo_custom"])&(df_join4["INT_Razon"]==df_buckets_custom["INT_Razon_custom"])),
                      how="left")

    df_join5 = df_join5.withColumn("Bucket", when((col("Bucket")=="NA") | (col("Bucket").isNull()), col("Bucket_custom")).otherwise(col("Bucket")))

    return df_join5

