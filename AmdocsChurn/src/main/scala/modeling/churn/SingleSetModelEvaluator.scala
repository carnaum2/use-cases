package modeling.churn

import featureselection.FeatureSelection
import labeling.{ChurnDataLoader, GeneralDataLoader}
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

object SingleSetModelEvaluator {

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
      .appName("Amdocs churn model (testing)")
      .getOrCreate()

    val ui = spark.sparkContext.uiWebUrl.getOrElse("-")

    println("\n[Info Amdocs Car Preparation] Spark ui: " + ui + "\n")

    ////////////////// 3. Input feats //////////////////
    val input_feats = GeneralDataLoader.getInputFeats(feats)

    val service_set = Utils.getServiceList(services)

    ////////////////// 1. Labeled CAR //////////////////
    val target_col = "label"
    val labelcardf = ChurnDataLoader.getLabeledCar(spark, car_date, horizon.toInt, segmentfilter, modeltarget, service_set, input_feats, target_col, level)

    println("\n[Info Amdocs churn model] Size of the table with service features and label: " + labelcardf.count() + "\n")

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

    input_feats_proc.foreach(f => println("\n[Info Amdocs churn model] Input feat: " + f + "\n"))

    val pipeline = PipelineGenerator.getPipeline(input_feats_proc.toArray, "f", "rf")

    ////////////////// 4. Training //////////////////

    val model = pipeline
      .fit(trdf)

    val ordImpFeats = ModelInterpreter.getOrderedRelevantFeats(model, input_feats_proc.toArray, "f", "rf")

    ordImpFeats.foreach{case(f, i) => println("\n[Info Amdocs churn model] Feat: " + f + " - Imp: " + i + "\n")}

    println("\n[Info Amdocs churn model] Before computing the preds on the tests set\n")

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

    println("\n[Info Amdocs churn model] AUC (tt) = " + ttAuc + "\n")

    val ttLift = Utils.getLift(Utils.getDeciles2(ttPredictions, "model_score"))

    ttLift.foreach(l => println("\n[Info Amdocs churn model] Decile: " + l._1 + " - Lift: " + l._2 ))

  }

}
