package modeling.modelinterpretation

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{GBTClassificationModel, RandomForestClassificationModel}
import utils.Utils

object ModelInterpreter {

  def getOrderedRelevantFeats(model: PipelineModel, featCols: Array[String], pca: String, classAlg: String): Array[(String, Double)] = {

    val ordImpFeats = if (pca.toLowerCase.compare("t")==0) {

      Array[(String, Double)](("PCA applied", 0.0))

    } else {

      classAlg match {

        case "rf" => {

          val impFeats = model.stages.last.asInstanceOf[RandomForestClassificationModel].featureImportances

          val ordImpFeats = Utils.getImportantFeaturesFromVector(featCols, impFeats)

          ordImpFeats

        }

        case "gbm" => {

          val impFeats = model.stages.last.asInstanceOf[GBTClassificationModel].featureImportances

          val ordImpFeats = Utils.getImportantFeaturesFromVector(featCols, impFeats)

          ordImpFeats

        }

        case _ => {

          Array[(String, Double)](("Not a tree-based classifier", 0.0))

        }

      }
    }

    ordImpFeats

  }

}
