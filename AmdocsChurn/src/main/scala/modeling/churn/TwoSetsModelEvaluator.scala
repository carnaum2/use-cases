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
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import utils.Utils

object TwoSetsModelEvaluator {

  def main(args: Array[String]): Unit = {

    val initProcessPoint = Calendar.getInstance().getTimeInMillis

    val initProcessStr = Calendar.getInstance().getTime().toString

    // Setting the config of the experiment
    val horizon = args(0).toInt
    val trMonthIni = args(1)
    val trMonthEnd = args(2)
    val balance = args(3)
    val ttMonth = args(4)
    val classAlg = args(5)
    val featsel = args(6)
    val segmentfilter = args(7)
    val modeltarget = args(8)
    val pca = args(9)
    val level = args(10)
    val services = args(11)
    val storepreds = args(12)
    val numArgs = args.length
    val feats = args.slice(13, numArgs).toList
    val input_feats = GeneralDataLoader.getInputFeats(feats)
    val mode = "test"

    // 2 20180331 20180331 T 20180430 rf zerovarandcorr onlymob port F service movil F all_reduced

    val target_col = "label"

    val service_set = Utils.getServiceList(services)

    // Spark session
    val spark = SparkSession
      .builder()
      .appName("Churn model - Test mode")
      .getOrCreate()

    println("\n[Info PospagoModel] Running PospagoModel with the following params:\n" +
      "\t - trMonthIni: " + trMonthIni + "\n" +
      "\t - trMonthEnd: " + trMonthEnd + "\n" +
      "\t - balance: " + balance + "\n" +
      "\t - ttMonth: " + ttMonth + "\n" +
      "\t - classAlg: " + classAlg + "\n" +
      "\t - featset: " + featsel + "\n" +
      "\t - segmentfilter: " + segmentfilter + "\n" +
      "\t - modeltarget: " + modeltarget + "\n" +
      "\t - pca: " + pca + "\n" +
      "\t - level: " + level + "\n" +
      "\t - services: " + services + "\n" +
      "\t - storepreds: " + storepreds + "\n" +
      "\t - subset: " + feats.mkString(" + ") + "\n")

    //////////////////////////////////////////////////////
    // Building the training set
    //////////////////////////////////////////////////////

    val trmonthseq = Utils
      .getMonthSeq(trMonthIni, trMonthEnd)

    val labDtTrMonths = trmonthseq
      .map(trMonth => {

        val labDtTr = ChurnDataLoader.getLabeledCar(spark, trMonth, horizon, segmentfilter, modeltarget, service_set, input_feats, target_col, level)

        println("\n[Info TwoSetsModelEvaluator] Training data - Training set has been labelled for month " + trMonth + "\n")

        labDtTr
      })
      .reduce(_ union _)

    val balDtTr = if(balance.toLowerCase.compare("t")==0) Utils.getBalancedSet(labDtTrMonths, target_col) else labDtTrMonths

    println("\n[Info TwoSetsModelEvaluator] Training data - A balanced training set has been obtained for months between " + trMonthIni + " and " + trMonthEnd + "\n")

    val nonInfFeats = FeatureSelection
      .getNonInformativeFeatures(balDtTr, featsel, Metadata.getNoInputFeats.toArray)

    println("\n[Info TwoSetsModelEvaluator] Training data - Feature selection has been completed from the balanced training set for months between " + trMonthIni + " and " + trMonthEnd + "\n")

    val numTrainSamples = balDtTr.count().toDouble
    val trSamplesNeg = balDtTr.filter(col("label") === 0.0).count().toDouble
    val trSamplesPos = balDtTr.filter(col("label") === 1.0).count().toDouble

    println("\n[Info TwoSetsModelEvaluator] Tr data size: " + numTrainSamples + "\n")
    println("\n[Info TwoSetsModelEvaluator] Tr data - Num samples 0: " + trSamplesNeg + "\n")
    println("\n[Info TwoSetsModelEvaluator] Tr data - Num samples 1: " + trSamplesPos + "\n")

    //////////////////////////////////////////////////////
    // Building the test set
    //////////////////////////////////////////////////////


    val labDtTt = ChurnDataLoader
      .getLabeledCar(spark, ttMonth, horizon, segmentfilter, modeltarget, service_set, input_feats, target_col, level)

    println("\n[Info TwoSetsModelEvaluator] Test data - Test set has been labelled\n")

    val numTestSamples = labDtTt.count().toDouble
    val ttSamplesNeg = labDtTt.filter(col("label") === 0.0).count().toDouble
    val ttSamplesPos = labDtTt.filter(col("label") === 1.0).count().toDouble

    println("\n[Info TwoSetsModelEvaluator] Tt data size: " + numTestSamples + "\n")
    println("\n[Info TwoSetsModelEvaluator] Tt data - Num samples 0: " + ttSamplesNeg + "\n")
    println("\n[Info TwoSetsModelEvaluator] Tt data - Num samples 1: " + ttSamplesPos + "\n")

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

    val inputDim = featCols.length

    val pipeline = PipelineGenerator.getPipeline(featCols, pca, classAlg)

    //////////////////////////////////////////////////////
    // Training
    //////////////////////////////////////////////////////

    val model = pipeline.fit(balDtTr)

    val ordImpFeats = ModelInterpreter.getOrderedRelevantFeats(model, featCols, pca, classAlg)

    ordImpFeats.foreach{case(f,v) => {println("\n[Info TwoSetsModelEvaluator] Feat: " + f + " - Importance: " + v + "\n")}}

    val thirtyImpFeats = ordImpFeats.slice(0, 30).map(_._1)

    val thirtyImpVals = ordImpFeats.slice(0, 30).map(_._2)

    println("\n[Info TwoSetsModelEvaluator] Before computing the preds on the tests set\n")

    //////////////////////////////////////////////////////
    // Test
    //////////////////////////////////////////////////////

    val getScore = udf((probability: org.apache.spark.ml.linalg.Vector) => probability.toArray(1))

    val trPredictions = model
      .transform(balDtTr.repartition(9000))
      .withColumn("model_score", getScore(col("probability")).cast(DoubleType))

    val ttPredictions = model
      .transform(labDtTt.repartition(1000))
      .withColumn("model_score", getScore(col("probability")).cast(DoubleType))

    println("\n[Info TwoSetsModelEvaluator] After computing the preds on both training and test sets and before computing lift\n")

    // STORING THE TABLE WITH FEATS AND PREDS

    if(storepreds.toLowerCase.compare("t")==0) {

      val feats2store = Metadata.getIdFeats() :+ "model_score"

      Utils
        .getDeciles2(ttPredictions.select(feats2store.head, feats2store.tail:_*), "model_score")
        .withColumn("partitioned_month", lit(ttMonth))
        .withColumn("segment", lit(segmentfilter))
        .withColumn("train", lit(trmonthseq.mkString(",")))
        .withColumn("horizon", lit(horizon))
        //.withColumn("target", lit(modeltarget)) Column to be included in the stored preds
        .write
        .format("parquet")
        .mode("append")
        .saveAsTable("tests_es.jvmm_amdocs_risk_scores")

      println("\n[Info TwoSetsModelEvaluator] Preds table stored\n")

    }

    //////////////////////////////////////////////////////
    // Evaluation
    //////////////////////////////////////////////////////

    val mean0 = ttPredictions.select("label", "model_score").rdd.map(r => (r.getAs[Double](0), r.getAs[Double](1))).filter(_._1 == 0.0).map(_._2).mean()

    val mean1 = ttPredictions.select("label", "model_score").rdd.map(r => (r.getAs[Double](0), r.getAs[Double](1))).filter(_._1 == 1.0).map(_._2).mean()

    val numWrong = ttPredictions.select("model_score").rdd.map(r => (r.getAs[Double](0))).filter(_ == -1.0).count().toDouble

    println("\n[Info TwoSetsModelEvaluator] Mean score for class 0: " + mean0 + "\n")

    println("\n[Info TwoSetsModelEvaluator] Mean score for class 1: " + mean1 + "\n")

    println("\n[Info TwoSetsModelEvaluator] Num wrong scores: " + numWrong + "\n")

    val trLift = Utils.getLift(Utils.getDeciles2(trPredictions, "model_score"))

    val ttLift = Utils.getLift(Utils.getDeciles2(ttPredictions, "model_score"))

    ttLift.foreach(l => println("\n[Info TwoSetsModelEvaluator] Decile: " + l._1 + " - Lift: " + l._2 ))

    println("\n[Info TwoSetsModelEvaluator] After computing the lift\n")

    val predictionLabelsTtRDD = ttPredictions.select("model_score", "label").rdd.map(r => (r.getDouble(0), r.getDouble(1)))
    val binMetricsTt = new BinaryClassificationMetrics(predictionLabelsTtRDD)
    val avgScoreTt = predictionLabelsTtRDD.map(_._1).mean()
    val sdScoreTt = predictionLabelsTtRDD.map(_._1).stdev()
    val skScoreTt = predictionLabelsTtRDD.map(x => Math.pow((x._1 - avgScoreTt)/sdScoreTt, 3.0)).mean()
    val kuScoreTt = predictionLabelsTtRDD.map(x => Math.pow((x._1 - avgScoreTt)/sdScoreTt, 4.0)).mean()
    val minScoreTt = predictionLabelsTtRDD.map(_._1).min()
    val maxScoreTt = predictionLabelsTtRDD.map(_._1).max()

    val predictionLabelsTrRDD = trPredictions.select("model_score", "label").rdd.map(r => (r.getDouble(0), r.getDouble(1)))
    val binMetricsTr = new BinaryClassificationMetrics(predictionLabelsTrRDD)
    val avgScoreTr = predictionLabelsTrRDD.map(_._1).mean()
    val sdScoreTr = predictionLabelsTrRDD.map(_._1).stdev()
    val skScoreTr = predictionLabelsTrRDD.map(x => Math.pow((x._1 - avgScoreTt)/sdScoreTt, 3.0)).mean()
    val kuScoreTr = predictionLabelsTrRDD.map(x => Math.pow((x._1 - avgScoreTt)/sdScoreTt, 4.0)).mean()
    val minScoreTr = predictionLabelsTrRDD.map(_._1).min()
    val maxScoreTr = predictionLabelsTrRDD.map(_._1).max()

    val ttAuc = binMetricsTt.areaUnderROC()
    println("\n[Info PospagoModel] AUC (tt) = " + ttAuc + "\n")

    val trAuc = binMetricsTr.areaUnderROC()
    println("\n[Info PospagoModel] AUC (tr) = " + trAuc + "\n")

    //val time = System.currentTimeMillis()

    //val modelName =  mode + "_mobilechurn_" + modeltarget + "_subset_" + input_feats.mkString("_") + "_tr" + trMonthIni + "_" + trMonthEnd + "_tt" + ttMonth + "_segment" + segmentfilter + "_featsel" + featsel + "_alg" + classAlg + "_time" + time.toString

    //println("\n[Info PospagoModel] Model:  " + modelName + "\n")

    //val endProcessPoint = Calendar.getInstance().getTimeInMillis

    //val duration = (endProcessPoint - initProcessPoint).toDouble/(1000*60)

    //val resSchema = StructType(StructField("date_millis", LongType, true) :: StructField("mode", StringType, true) :: StructField("model", StringType, true) :: StructField("duration", DoubleType, true) :: StructField("date_string", StringType, true) :: StructField("segment", StringType, true) :: StructField("target", StringType, true) :: StructField("tr_month_ini", StringType, true) :: StructField("tr_month_end", StringType, true) :: StructField("input", ArrayType(StringType), true) :: StructField("alg", StringType, true) :: StructField("params", StringType, true) :: StructField("roc_tr", DoubleType, true) :: StructField("roc_tt", DoubleType, true) :: StructField("lift_tr", ArrayType(DoubleType), true) :: StructField("lift_tt", ArrayType(DoubleType), true) :: StructField("tr_samples_neg", DoubleType, true) :: StructField("tr_samples_pos", DoubleType, true) :: StructField("tt_samples_neg", DoubleType, true) :: StructField("tt_samples_pos", DoubleType, true) :: StructField("avg_score_tr", DoubleType, true) :: StructField("sd_score_tr", DoubleType, true) :: StructField("skewness_score_tr", DoubleType, true) :: StructField("kurtosis_score_tr", DoubleType, true) :: StructField("min_score_tr", DoubleType, true) :: StructField("max_score_tr", DoubleType, true) :: StructField("avg_score_tt", DoubleType, true) :: StructField("sd_score_tt", DoubleType, true) :: StructField("skewness_score_tt", DoubleType, true) :: StructField("kurtosis_score_tt", DoubleType, true) :: StructField("min_score_tt", DoubleType, true) :: StructField("max_score_tt", DoubleType, true) :: StructField("input_dim", DoubleType, true) :: StructField("imp_feats", ArrayType(StringType), true) :: StructField("imp_val", ArrayType(DoubleType), true) :: Nil)

    //val dfresult = spark.createDataFrame(spark.sparkContext.parallelize(Array(List(initProcessPoint, mode, modelName, duration, initProcessStr, segmentfilter, modeltarget, trMonthIni, trMonthEnd, input_feats.toArray, classAlg, model.extractParamMap().toString(), trAuc, ttAuc, trLift.map(_._2), ttLift.map(_._2), trSamplesNeg, trSamplesPos, ttSamplesNeg, ttSamplesPos, avgScoreTr, sdScoreTr, skScoreTr, kuScoreTr, minScoreTr, maxScoreTr, avgScoreTt, sdScoreTt, skScoreTt, kuScoreTt, minScoreTt, maxScoreTt, inputDim, thirtyImpFeats, thirtyImpVals))).map(r => Row(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12), r(13), r(14), r(15), r(16), r(17), r(18), r(19), r(20), r(21), r(22), r(23), r(24), r(25), r(26), r(27), r(28), r(29), r(30), r(31), r(32), r(33), r(34))), resSchema)

    //dfresult.write.mode(SaveMode.Append).saveAsTable("tests_es.jvmm_churn_model_results")

    //dfresult.show()

  }

}
