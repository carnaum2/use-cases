package modeling.pipelinegenerator

import metadata.Metadata
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassifier, RandomForestClassifier}
import org.apache.spark.ml.feature.{OneHotEncoder, PCA, StringIndexer, VectorAssembler}

object PipelineGenerator {

  def getPipeline(featCols: Array[String], pca: String, classAlg: String): Pipeline = {

    val inputDim = featCols.length

    // 1. Stages to deal with categorical feats: indexers and encoders

    val indexAndEncod = if(Metadata.getCatFeats().isEmpty)
      Array()
    else {
      val indexers = Metadata
        .getCatFeats()
        .map(c => new StringIndexer()
          .setInputCol(c)
          .setOutputCol(s"${c}_idx")
          .setHandleInvalid("skip"))
        .toArray

      val encoders = Metadata
        .getCatFeats()
        .map(c => new OneHotEncoder()
          .setInputCol(s"${c}_idx")
          .setOutputCol(s"${c}_enc"))
        .toArray

      indexers ++ encoders

    }

    // 2. Assembler: merging columns into a single column "features"

    val assembler = new VectorAssembler()
      .setInputCols(featCols)
      .setOutputCol("features")

    // 3. PCA

    val numpca = (0.75*inputDim).round.toInt

    val pcaStage = new PCA()
      .setInputCol("features")
      .setOutputCol("pcafeatures")
      .setK(numpca)

    // 4. Classifier

    val inputcol = if(pca.toLowerCase.compare("t")==0) "pcafeatures" else "features"

    val classStage = classAlg match {

      case "rf" => new RandomForestClassifier()
        .setNumTrees(300)
        .setFeatureSubsetStrategy("sqrt")
        .setImpurity("gini")
        .setMaxDepth(10)
        .setMaxBins(32)
        .setLabelCol("label")
        .setFeaturesCol(inputcol)
        .setSubsamplingRate(0.7)
        .setMinInstancesPerNode(50)

      case "gbm" => new GBTClassifier()
        .setSeed(1234)
        .setLabelCol("label")
        .setFeaturesCol(inputcol)
        .setMaxBins(32)
        .setMaxDepth(10)
        .setStepSize(0.01)
        .setImpurity("gini")
        .setMaxIter(500)
        .setSubsamplingRate(0.7)

    }

    // 5. Merging the pieces to obtain the pipeline; the resulting pipeline will depend on the options specified by the user

    val pipeline = (Metadata.getCatFeats().isEmpty, pca.toLowerCase.compare("t")==0) match {

      case (true, true) => {

        println("\n[PipelineGenerator] Pipeline without categorical feats and PCA\n")

        new Pipeline()
          .setStages(Array(assembler, pcaStage) :+ classStage)

      }

      case (false, true) => {

        println("\n[PipelineGenerator] Pipeline with categorical feats and PCA\n")

        new Pipeline()
          .setStages(indexAndEncod ++ Array(assembler, pcaStage) :+ classStage)

      }

      case (true, false) => {

        println("\n[PipelineGenerator] Pipeline without categorical feats and PCA\n")

        new Pipeline()
          .setStages(Array(assembler) :+ classStage)

      }

      case (false, false) => {

        println("\n[PipelineGenerator] Pipeline with categorical feats but without PCA\n")

        new Pipeline()
          .setStages(indexAndEncod ++ Array(assembler) :+ classStage)

      }
    }

    pipeline

  }

}
