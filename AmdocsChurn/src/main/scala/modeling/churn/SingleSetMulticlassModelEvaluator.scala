package modeling.churn

import assessment.MulticlassMetricsCalculator
import featureselection.FeatureSelection
import labeling.{ChurnDataLoader, GeneralDataLoader}
import metadata.Metadata
import modeling.modelinterpretation.ModelInterpreter
import modeling.pipelinegenerator.PipelineGenerator
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import utils.Utils

object SingleSetMulticlassModelEvaluator {

  def main(args: Array[String]): Unit = {

    val car_date = args(0)//"20180528"
    val horizon = args(1)//2
    val segmentfilter = args(2) // onlymob
    val modeltarget = args(3) // port
    val featsel = args(4)
    val level = args(5)
    val services = args(6)
    val numArgs = args.length
    val feats = args.slice(7, numArgs).toList // all, reduced

    ////////////////// 0. Spark session //////////////////
    val spark = SparkSession
      .builder()
      .appName("Amdocs multiclass churn model (testing)")
      .getOrCreate()

    val ui = spark.sparkContext.uiWebUrl.getOrElse("-")

    println("\n[Info Amdocs Car Preparation] Spark ui: " + ui + "\n")

    ////////////////// 3. Input feats //////////////////
    val input_feats = GeneralDataLoader.getInputFeats(feats)

    val service_set = Utils.getServiceList(services)

    ////////////////// 1. Labeled CAR //////////////////
    val target_col = "label"

    val labelcardf = ChurnDataLoader.getLabeledCar(spark, car_date, horizon.toInt, segmentfilter, "multiclass", service_set, input_feats, target_col, level)

    println("\n[Info Amdocs churn model] Size of the table with service features and label: " + labelcardf.count() + "\n")

    ////////////////// 2. Datasets //////////////////

    val Array(unbaltrdf, ttdf) = labelcardf.randomSplit(Array(0.7, 0.3))

    println("\n[Info Amdocs Car Preparation] Printing unbal tr set\n")

    unbaltrdf.groupBy(target_col).agg(count("*").alias("num_samples_unbal_tr")).show

    println("\n[Info Amdocs Car Preparation] Printing tt set\n")

    ttdf.groupBy(target_col).agg(count("*").alias("num_samples_tt")).show

    val trdf = Utils.getBalancedSet(unbaltrdf, target_col)

    println("\n[Info Amdocs Car Preparation] Printing tr set\n")

    trdf.groupBy(target_col).agg(count("*").alias("num_samples_tr")).show

    val nonInfFeats = FeatureSelection.getNonInformativeFeatures(trdf, featsel, Metadata.getNoInputFeats.toArray)

    ////////////////// 3. Pipeline //////////////////

    // 3.1. Input feats

    val input_feats_proc = input_feats
      .diff(Metadata.getIdFeats()
        .union(Metadata.getCatFeats())
        .union(Metadata.getNoInputFeats())
        .union(nonInfFeats) :+ target_col)
      .union(Metadata.getCatFeats().map(_ + "_enc"))

    input_feats_proc.foreach(f => println("\n[Info Amdocs churn model] Input feat: " + f + "\n"))

    val pipeline = PipelineGenerator.getPipeline(input_feats_proc.toArray, "f", "rf")

    ////////////////// 4. Training //////////////////

    val model = pipeline
      .fit(trdf)

    val ordImpFeats = ModelInterpreter.getOrderedRelevantFeats(model, input_feats_proc.toArray, "f", "rf")

    ordImpFeats.foreach{case(f, i) => println("\n[Info Amdocs churn model] Feat: " + f + " - Imp: " + i + "\n")}

    println("\n[Info Amdocs churn model] Before computing the preds on the tests set\n")

    ////////////////// 5. Test //////////////////

    val trPredictions = model
      .transform(trdf.repartition(1000))

    val ttPredictions = model
      .transform(ttdf.repartition(1000))

    // Multiclass metrics

    val predictionLabelsMultiTrRDD = trPredictions.select("prediction", "label").rdd.map(r => (r.getDouble(0), r.getDouble(1)))
    val multiMetricsTr = new MulticlassMetricsCalculator(predictionLabelsMultiTrRDD)

    val predictionLabelsMultiTtRDD = ttPredictions.select("prediction", "label").rdd.map(r => (r.getDouble(0), r.getDouble(1)))
    val multiMetricsTt = new MulticlassMetricsCalculator(predictionLabelsMultiTtRDD)


    // Confusion matrix
    println("\n[Info PospagoModel] Confusion matrix (tt set):")
    println(multiMetricsTt.getConfMatrix)

    println("\n[Info PospagoModel] Confusion matrix (tr set):")
    println(multiMetricsTr.getConfMatrix)

  }


}
