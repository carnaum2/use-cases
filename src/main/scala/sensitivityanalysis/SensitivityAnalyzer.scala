package sensitivityanalysis

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object SensitivityAnalyzer {

  def computeGradient(trSet: Dataset[Row], ttSet: Dataset[Row], sensitivity_feats: List[String], model: PipelineModel): Dataset[Row] = {

    // For each feat, we have the point x0. points x_a = x0 - d and x_b = x0 + d are generated, with d small to study the variation
    // of the function in its local environment

    // The model must have been trained from scaled feature

    // The gradient along feat x is computed as [model(x_b) - model(x_a)]/2d

    // mydf.describe().filter(col("summary") === "stddev").show()

    //    input_feats
    //      .foldLeft(x)((acc, f) => acc.withColumn(f, col(f).cast("double")))
    //      .rdd
    //      .map(r => r.getValuesMap[Double](input_feats.toSeq))
    //      .map(r => computeGradient(r, model))

    // x includes a columns with the feat value and another one with the std in the tr set; in addition, it includes the model output for the current point
    // the vector input_feats includes the feats for which the study is carried out

    // Identifying the input feats in the dataset: columns generated by transformations must not be in the dataframe used as input for the method transform

    val idx_cols = ttSet.columns.filter(_.contains("_idx"))

    val enc_cols = ttSet.columns.filter(_.contains("_enc"))

    val feats_rmv = (idx_cols ++ enc_cols ++ Array("features", "prediction", "rawPrediction", "probability")).toList

    val input_feats = ttSet.columns.diff(feats_rmv).toList

    // Getting the std for each feat

    val stdev_map =  trSet
      .describe()
      .select(sensitivity_feats.head, sensitivity_feats.tail:_*)
      .filter(col("summary") === "stddev")
      .rdd
      .map(r => r.getValuesMap[Double](sensitivity_feats.toSeq))
      .collect
      .apply(0)

    val getScore = udf((probability: org.apache.spark.ml.linalg.Vector) => probability.toArray(1))

    val sensitivity_analysis = sensitivity_feats
      .foldLeft((ttSet, input_feats))((acc, f) => {

        println("\n[Info Sensitivity] Processing feat: " + f + "\n")

        // select on acc inside transform makes new columns added during the process to be removed

        val data = acc._1

        val feat_list = acc._2

        val output_data = model
          .transform(
            data.select(feat_list.head, feat_list.tail:_*)
              .withColumn(f + "_old", col(f)).withColumn(f, col(f) + lit(stdev_map(f)))
          )
          .withColumn(f + "_model_sens", getScore(col("probability")).cast(DoubleType))
          .drop(f)
          .withColumn(f, col(f + "_old"))
          .drop(f + "_old")
          //.withColumn(f + "_gradient", (col(f + "_model_sens") - col("model_score")) / lit(stdev_map(f)))
          // It is not required to normalise the increment in the model output by using the std in that direction.
          // As the increment in the direction of the feat f is given by 1 std, this is the unit in the denominator to obtain the gradient
          .withColumn(f + "_gradient", col(f + "_model_sens") - col("model_score") )
          .drop("features", "prediction", "rawPrediction", "probability")

        val output_feat_list = feat_list ++ List(f + "_model_sens", f + "_gradient")

        (output_data, output_feat_list)
      })

    sensitivity_analysis._1

  }

  def computeGradient2(trSet: Dataset[Row], ttSet: Dataset[Row], sensitivity_feats: List[String], model: PipelineModel): Dataset[Row] = {

    // For each feat, we have the point x0. points x_a = x0 - d and x_b = x0 + d are generated, with d small to study the variation
    // of the function in its local environment

    // The model must have been trained from scaled feature

    // The gradient along feat x is computed as [model(x_b) - model(x_a)]/2d

    // mydf.describe().filter(col("summary") === "stddev").show()

    //    input_feats
    //      .foldLeft(x)((acc, f) => acc.withColumn(f, col(f).cast("double")))
    //      .rdd
    //      .map(r => r.getValuesMap[Double](input_feats.toSeq))
    //      .map(r => computeGradient(r, model))

    // x includes a columns with the feat value and another one with the std in the tr set; in addition, it includes the model output for the current point
    // the vector input_feats includes the feats for which the study is carried out

    // Identifying the input feats in the dataset: columns generated by transformations must not be in the dataframe used as input for the method transform

    val idx_cols = ttSet.columns.filter(_.contains("_idx"))

    val enc_cols = ttSet.columns.filter(_.contains("_enc"))

    val feats_rmv = (idx_cols ++ enc_cols ++ Array("features", "prediction", "rawPrediction", "probability")).toList

    val input_feats = ttSet.columns.diff(feats_rmv).toList

    val inputdf = ttSet
      .select(input_feats.head, input_feats.tail:_*)
      .cache()

    trSet.sparkSession.sparkContext.broadcast()

    // Getting the std for each feat

    val stdev_map =  trSet
      .describe()
      .select(sensitivity_feats.head, sensitivity_feats.tail:_*)
      .filter(col("summary") === "stddev")
      .rdd
      .map(r => r.getValuesMap[String](sensitivity_feats.toSeq))
      .collect
      .apply(0)
      .mapValues(_.toDouble)

    stdev_map.toArray.foreach(f => println("\n[Info Sensitivity] stdev_map - " + f._1 + ": " + f._2 + "\n"))

    val broadcasted_stdev_map = trSet.sparkSession.sparkContext.broadcast(stdev_map)

    val getScore = udf((probability: org.apache.spark.ml.linalg.Vector) => probability.toArray(1))

    val scoresdf = ttSet
      .select("msisdn", "model_score")
      .cache()

    println("\n[Info Sensitivity] Num rows in scoresdf: " + scoresdf.count() +" - Num cols in scoresdf: " + scoresdf.columns.length + "\n")

    // Adding columns dynamically by using foldLeft may result in OutOfMemory error. An alternative way based on the transformation of a RDD[Row] by using map shuld be explored.

    val sensitivity_analysis_df = sensitivity_feats
      .foldLeft(scoresdf)((acc, f) => {

        println("\n[Info Sensitivity] Processing feat: " + f + "\n")

        // Dataframe with new point x' for each customer obtained by moving one std unit
        // along the axis corresponding to the feature under analysis

        val mod_inputdf = inputdf
          .withColumn(f, col(f) + lit(broadcasted_stdev_map.value(f)))

        // Output of the model at the new point x'

        val output_data = model
          .transform(mod_inputdf)
          .withColumn(f + "_model_sens", getScore(col("probability")).cast(DoubleType))
          .select("msisdn", f + "_model_sens")
          .cache()

        // Adding the column "feature_model_sens" to the accumulator

        acc
          .join(output_data, Seq("msisdn"), "inner")
          .repartition(500)
          .cache()
      })

    val result_df = sensitivity_feats
      .foldLeft(sensitivity_analysis_df)(
        (acc, f) => acc
          .withColumn(f + "_gradient", col(f + "_model_sens") - col("model_score") )
      )

    println("\n[Info Sensitivity] Sensitivity analysis completed - Result df with " + result_df.count() + " rows and " + result_df.columns.length + " columns\n")

    result_df

  }

  def computeGradient3(trSet: Dataset[Row], ttSet: Dataset[Row], sensitivity_feats: List[String], model: PipelineModel): Dataset[Row] = {

    // For each feat, we have the point x0. points x_a = x0 - d and x_b = x0 + d are generated, with d small to study the variation
    // of the function in its local environment

    // The model must have been trained from scaled feature

    // The gradient along feat x is computed as [model(x_b) - model(x_a)]/2d

    // mydf.describe().filter(col("summary") === "stddev").show()

    // -----------------------------------------------

    val spark = trSet.sparkSession

    // Identifying the input feats in the dataset: columns generated by transformations must not be in the dataframe used as input for the method transform

    val idx_cols = ttSet.columns.filter(_.contains("_idx"))

    val enc_cols = ttSet.columns.filter(_.contains("_enc"))

    val feats_rmv = (idx_cols ++ enc_cols ++ Array("features", "prediction", "rawPrediction", "probability", "label")).toList

    val input_feats = (ttSet.columns.diff(feats_rmv).toList ++ List("msisdn", "model_score")).distinct

    val inputdf = ttSet
      .select(input_feats.head, input_feats.tail:_*)
      .repartition(200)
      .cache()

    // Getting the std for each feat

    val stdev_map =  trSet
      .describe()
      .select(sensitivity_feats.head, sensitivity_feats.tail:_*)
      .filter(col("summary") === "stddev")
      .rdd
      .map(r => r.getValuesMap[String](sensitivity_feats.toSeq))
      .collect
      .apply(0)
      .mapValues(_.toDouble).toSeq

    stdev_map.toArray.foreach(f => println("\n[Info Sensitivity] stdev_map - " + f._1 + ": " + f._2 + "\n"))

    import spark.sqlContext.implicits._

    val stdev_df = stdev_map.toDF("mod_feat", "delta")

    stdev_df.show

    val getScore = udf((probability: org.apache.spark.ml.linalg.Vector) => probability.toArray(1))

    // Adding columns dynamically by using foldLeft may result in OutOfMemory error. An alternative way based on the transformation of a RDD[Row] by using map shuld be explored.

    // Feat columns casted to Double before the analysis

    val schema = inputdf.schema

    val output_schema = schema.add(StructField("mod_feat", StringType, nullable = true))

    val output_rows = inputdf
      .rdd
      .flatMap(r => {

        // For each customer/row, the seq containing the row

        val input_seq = r.toSeq

        // A collection of rows is generated for each original row

        val output_maps = sensitivity_feats
          .map(f => {

            // New row is the original one with the name of the feat to be modified

            val mod_row = Row.fromSeq(input_seq :+ f)

            mod_row

          })

        output_maps
      })

    val input_sens_analysis_df = spark.createDataFrame(output_rows, output_schema).join(stdev_df, Seq("mod_feat"), "inner")

    // input_sens_analysis_df.show()

    val mod_sens_analysis_df = sensitivity_feats
      .foldLeft(input_sens_analysis_df)((acc, f) => acc.withColumn(f, when(col("mod_feat") === f, col(f) + col("delta")).otherwise(col(f))))

    val sens_df = model
      .transform(mod_sens_analysis_df)
      .withColumn("model_sens", getScore(col("probability")).cast(DoubleType))
      .withColumn("gradient", col("model_sens") - col("model_score"))
      .select("msisdn", "mod_feat", "gradient")
      .groupBy("msisdn")
      .pivot("mod_feat")
      .agg(sum("gradient"))

    println("\n[Info Sensitivity] Sensitivity analysis completed - Result df with " + sens_df.count() + " rows and " + sens_df.columns.length + " columns\n")

    sens_df

  }



}