package modeling.winback

import featureselection.FeatureSelection
import labeling.{ChurnDataLoader, GeneralDataLoader, WinbackDataLoader}
import metadata.Metadata
import modeling.modelinterpretation.ModelInterpreter
import modeling.pipelinegenerator.PipelineGenerator
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, udf}
import org.apache.spark.sql.types.DoubleType
import utils.Utils

object AmdocsWinbackModel {

  def main(args: Array[String]): Unit = {

    val port_month = args(0)//"201805"
    val segmentfilter = args(1) // onlymob
    val competitor = args(2) // port
    val featsel = args(3)
    val services = args(4)
    val numArgs = args.length
    val feats = args.slice(5, numArgs).toList // all, reduced

    ////////////////// 0. Spark session //////////////////
    val spark = SparkSession
      .builder()
      .appName("Retention model for Amdocs (testing)")
      .getOrCreate()

    val ui = spark.sparkContext.uiWebUrl.getOrElse("-")

    println("\n[Info Amdocs Winback] Spark ui: " + ui + "\n")

    val input_feats = GeneralDataLoader.getInputFeats(feats)

    val service_set = Utils.getServiceList(services)

    val target_col = "label"
    val labelcardf = WinbackDataLoader.getLabeledWinbackCar(spark, port_month, segmentfilter, service_set, competitor, input_feats, target_col)

    println("\n[Info Amdocs Winback] Size of the table with service features and label: " + labelcardf.count() + "\n")

    ////////////////// 2. Datasets //////////////////

    val Array(unbaltrdf, ttdf) = labelcardf.randomSplit(Array(0.7, 0.3))

    unbaltrdf.groupBy(target_col).agg(count("*").alias("num_samples_unbal_tr")).show

    ttdf.groupBy(target_col).agg(count("*").alias("num_samples_tt")).show

    val trdf = Utils.getBalancedSet(unbaltrdf, target_col)

    trdf.groupBy(target_col).agg(count("*").alias("num_samples_tr")).show

    val nonInfFeats = FeatureSelection
      .getNonInformativeFeatures(trdf, featsel, Metadata.getNoInputFeats.toArray)

    ////////////////// 3. Pipeline //////////////////

    // 3.1. Input feats

    val input_feats_proc = input_feats
      .diff(Metadata.getIdFeats()
        .union(Metadata.getCatFeats())
        .union(Metadata.getNoInputFeats())
        .union(nonInfFeats) :+ target_col)
      .union(Metadata.getCatFeats().diff(Metadata.getNoInputFeats()).map(_ + "_enc"))

    input_feats_proc.foreach(f => println("\n[Info Amdocs Winback] Input feat: " + f + "\n"))

    val pipeline = PipelineGenerator.getPipeline(input_feats_proc.toArray, "f", "rf")

    ////////////////// 4. Training //////////////////

    val model = pipeline
      .fit(trdf)

    val ordImpFeats = ModelInterpreter.getOrderedRelevantFeats(model, input_feats_proc.toArray, "f", "rf")

    ordImpFeats.foreach{case(f, i) => println("\n[Info Amdocs Winback] Feat: " + f + " - Imp: " + i + "\n")}

    println("\n[Info Amdocs Winback] Before computing the preds on the tests set\n")

    ////////////////// 5. Test //////////////////

    // getScore takes into account that the order of rawPrediction is not guaranteed

    val getScore = udf((probability: org.apache.spark.ml.linalg.Vector) => probability.toArray(1))

    val ttPredictions = model
      .transform(ttdf.repartition(1000))
      .withColumn("model_score", getScore(col("probability")).cast(DoubleType))

    ////////////////// 6. Evaluation //////////////////

    val predictionLabelsTtRDD = ttPredictions.select("model_score", target_col).rdd.map(r => (r.getDouble(0), r.getDouble(1)))
    val binMetricsTt = new BinaryClassificationMetrics(predictionLabelsTtRDD)

    val ttAuc = binMetricsTt.areaUnderROC()

    println("\n[Info Amdocs Winback] AUC (tt) = " + ttAuc + "\n")

    val ttLift = Utils.getLift(Utils.getDeciles2(ttPredictions, "model_score"))

    ttLift.foreach(l => println("\n[Info Amdocs Winback] Decile: " + l._1 + " - Lift: " + l._2 ))
  }

}
