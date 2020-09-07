package modeling.churn

import java.util.Calendar

import featureselection.FeatureSelection
import labeling.{ChurnDataLoader, GeneralDataLoader}
import metadata.Metadata
import modeling.modelinterpretation.ModelInterpreter
import modeling.pipelinegenerator.PipelineGenerator
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature.{OneHotEncoder, PCA, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types._
import utils.Utils

object AmdocsChurnModelPredictor {

  def main(args: Array[String]): Unit = {

    val initProcessPoint = Calendar.getInstance().getTimeInMillis

    val initProcessStr = Calendar.getInstance().getTime().toString

    // Setting the config of the experiment
    val predname = args(0)
    val horizon = args(1).toInt
    val trMonthIni = args(2)
    val trMonthEnd = args(3)
    val balance = args(4)
    val ttMonth = args(5)
    val classAlg = args(6)
    val featsel = args(7)
    val segmentfilter = args(8)
    val modeltarget = args(9)
    val pca = args(10)
    val level = args(11)
    val services = args(12)
    val numArgs = args.length
    val feats = args.slice(13, numArgs).toList
    val input_feats = GeneralDataLoader.getInputFeats(feats)
    val mode = "prediction"

    val target_col = "label"

    val service_set = Utils.getServiceList(services)

    // Spark session
    val spark = SparkSession
      .builder()
      .appName("Amdocs churn model - Prediction mode")
      .getOrCreate()

    println("\n[Info AmdocsChurnModel Predictor] Running AmdocsChurnModel Predictor with the following params:\n" +
      "\t - trMonthIni: " + trMonthIni + "\n" +
      "\t - trMonthEnd: " + trMonthEnd + "\n" +
      "\t - balance: " + balance + "\n" +
      "\t - ttMonth: " + ttMonth + "\n" +
      "\t - classAlg: " + classAlg + "\n" +
      "\t - featset: " + featsel + "\n" +
      "\t - segmentfilter: " + segmentfilter + "\n" +
      "\t - modeltarget: " + modeltarget + "\n" +
      "\t - level: " + level + "\n" +
      "\t - pca: " + pca + "\n" +

      "\t - subset: " + input_feats.mkString(" + ") + "\n")

    //////////////////////////////////////////////////////
    // Building the training set
    //////////////////////////////////////////////////////

    val trmonthseq = Utils
      .getMonthSeq(trMonthIni, trMonthEnd)

    val labDtTrMonths = trmonthseq
      .map(trMonth => {

        val labDtTr = ChurnDataLoader.getLabeledCar(spark, trMonth, horizon, segmentfilter, modeltarget, service_set, input_feats, target_col, level)

        println("\n[Info AmdocsChurnModel Predictor] Training data - Training set has been labelled for month " + trMonth + "\n")

        labDtTr
      })
      .reduce(_ union _)

    val balDtTr = if(balance.toLowerCase.compare("t")==0) Utils.getBalancedSet(labDtTrMonths, target_col) else labDtTrMonths

    println("\n[Info AmdocsChurnModel Predictor] Training data - A balanced training set has been obtained for months between " + trMonthIni + " and " + trMonthEnd + "\n")

    val nonInfFeats = FeatureSelection
      .getNonInformativeFeatures(balDtTr, featsel, Metadata.getNoInputFeats.toArray)

    println("\n[Info AmdocsChurnModel Predictor] Training data - Feature selection has been completed from the balanced training set for months between " + trMonthIni + " and " + trMonthEnd + "\n")

    val numTrainSamples = balDtTr.count().toDouble
    val trSamplesNeg = balDtTr.filter(col("label") === 0.0).count().toDouble
    val trSamplesPos = balDtTr.filter(col("label") === 1.0).count().toDouble

    println("\n[Info AmdocsChurnModel Predictor] Tr data size: " + numTrainSamples + "\n")
    println("\n[Info AmdocsChurnModel Predictor] Tr data - Num samples 0: " + trSamplesNeg + "\n")
    println("\n[Info AmdocsChurnModel Predictor] Tr data - Num samples 1: " + trSamplesPos + "\n")

    //////////////////////////////////////////////////////
    // Building the test set
    //////////////////////////////////////////////////////

    val labDtTt = ChurnDataLoader
      .getUnlabeledCar(spark, ttMonth, segmentfilter, modeltarget, service_set, input_feats)

    println("\n[Info AmdocsChurnModel Predictor] Test data - Test set has been labelled\n")

    val numTestSamples = labDtTt.count().toDouble

    println("\n[Info AmdocsChurnModel Predictor] Tt data size: " + numTestSamples + "\n")

    //////////////////////////////////////////////////////
    // Building the pipeline
    //////////////////////////////////////////////////////

    val featCols = balDtTr
      .columns
      .diff(Metadata.getIdFeats()
        .union(Metadata.getNoInputFeats)
        .union(Metadata.getCatFeats())
        .union(nonInfFeats) :+ target_col)
      .union(Metadata.getCatFeats().map(_ + "_enc"))

    featCols.foreach(f => println("\n[Info AmdocsChurnModel Predictor] Input feat: " + f + "\n"))

    val pipeline = PipelineGenerator.getPipeline(featCols, pca, classAlg)

    //////////////////////////////////////////////////////
    // Training
    //////////////////////////////////////////////////////

    val model = pipeline.fit(balDtTr)

    val ordImpFeats = ModelInterpreter.getOrderedRelevantFeats(model, featCols, pca, classAlg)

    ordImpFeats.foreach{case(f,v) => {println("\n[Info AmdocsChurnModel Predictor] Feat: " + f + " - Importance: " + v + "\n")}}

    val thirtyImpFeats = ordImpFeats.slice(0, 30).map(_._1)

    val thirtyImpVals = ordImpFeats.slice(0, 30).map(_._2)

    println("\n[Info AmdocsChurnModel Predictor] Before computing the preds on the tests set\n")

    //////////////////////////////////////////////////////
    // Test
    //////////////////////////////////////////////////////

    val getScore = udf((probability: org.apache.spark.ml.linalg.Vector) => probability.toArray(1))

    val trPredictions = model
      .transform(balDtTr.repartition(1000))
      .withColumn("model_score", getScore(col("probability")).cast(DoubleType))

    val ttPredictions = model
      .transform(labDtTt.repartition(1000))
      .withColumn("model_score", getScore(col("probability")).cast(DoubleType))

    println("\n[Info AmdocsChurnModel Predictor] After computing the preds on both training and test sets and before computing lift\n")

    // STORING THE TABLE WITH PREDS

    val feats2store = Metadata.getIdFeats() :+ "model_score"

    Utils
      .getDeciles2(ttPredictions.select(feats2store.head, feats2store.tail:_*), "model_score")
      .withColumn("partitioned_month", lit(ttMonth))
      .withColumn("segment", lit(segmentfilter))
      .withColumn("train", lit(trmonthseq.mkString(",")))
      .withColumn("horizon", lit(horizon))
      .withColumn("pred_name", lit(predname))
      //.withColumn("target", lit(modeltarget)) Column to be included in the stored preds
      .write
      .format("parquet")
      .mode("append")
      .saveAsTable("tests_es.jvmm_amdocs_churn_scores")

      println("\n[Info AmdocsChurnModel Predictor] Preds table stored\n")


  }

}
