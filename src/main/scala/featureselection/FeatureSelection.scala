package featureselection

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}


/**
  * Created by victor on 2/10/17.
  */
object FeatureSelection {

  def getNonInformativeFeatures(dt: Dataset[Row], mode: String, noinputFeats: Array[String]): Array[String] = {

    val remFeats = mode match {

      case "none" => Array[String]()

      case "zerovar" => removeConstantFeatures(dt, noinputFeats)

      case "correlation" => loadCorrelatedFeatures() //removeCorrelatedFeatures(dt, 0.8, 200, noinputFeats)

      case "zerovarandcorr" => removeConstantFeatures(dt, noinputFeats) ++ loadCorrelatedFeatures() // ++ removeCorrelatedFeatures(dt, 0.8, 200, noinputFeats)

      case _ => Array[String]()

    }

    remFeats.foreach(x => println("\n[Info FeatureSelection] Feat to be removed: " + x + "\n"))

    remFeats

  }

  def loadCorrelatedFeatures(): Array[String] = {

    Array("dias_desde_fx_srv_basic",
      "dias_desde_fx_data",
      "total_num_services",
      "GNV_hour_0_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_0_W_MasMegas_Data_Volume_MB",
      "GNV_hour_0_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_0_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_1_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_1_W_MasMegas_Data_Volume_MB",
      "GNV_hour_1_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_1_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_1_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_2_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_2_W_MasMegas_Data_Volume_MB",
      "GNV_hour_2_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_2_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_2_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_3_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_3_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_3_W_MasMegas_Data_Volume_MB",
      "GNV_hour_3_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_3_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_3_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_4_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_4_W_MasMegas_Data_Volume_MB",
      "GNV_hour_4_W_Music_Pass_Data_Volume_MB",
      "GNV_hour_4_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_4_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_4_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_5_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_5_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_5_W_MasMegas_Data_Volume_MB",
      "GNV_hour_5_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_5_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_5_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_5_WE_Music_Pass_Data_Volume_MB",
      "GNV_hour_6_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_6_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_6_W_MasMegas_Data_Volume_MB",
      "GNV_hour_6_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_6_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_7_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_7_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_7_W_MasMegas_Data_Volume_MB",
      "GNV_hour_7_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_7_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_7_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_8_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_8_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_8_W_MasMegas_Data_Volume_MB",
      "GNV_hour_8_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_8_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_8_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_9_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_9_W_MasMegas_Data_Volume_MB",
      "GNV_hour_9_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_9_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_9_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_10_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_10_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_10_W_MasMegas_Data_Volume_MB",
      "GNV_hour_10_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_10_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_10_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_11_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_11_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_11_W_MasMegas_Data_Volume_MB",
      "GNV_hour_11_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_11_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_11_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_12_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_12_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_12_W_MasMegas_Data_Volume_MB",
      "GNV_hour_12_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_12_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_12_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_13_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_13_W_MasMegas_Data_Volume_MB",
      "GNV_hour_13_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_13_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_13_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_14_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_14_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_14_W_MasMegas_Data_Volume_MB",
      "GNV_hour_14_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_14_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_14_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_14_WE_Music_Pass_Data_Volume_MB",
      "GNV_hour_15_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_15_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_15_W_MasMegas_Data_Volume_MB",
      "GNV_hour_15_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_15_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_15_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_16_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_16_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_16_W_MasMegas_Data_Volume_MB",
      "GNV_hour_16_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_16_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_16_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_17_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_17_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_17_W_MasMegas_Data_Volume_MB",
      "GNV_hour_17_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_17_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_17_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_18_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_18_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_18_W_MasMegas_Data_Volume_MB",
      "GNV_hour_18_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_18_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_18_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_19_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_19_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_19_W_MasMegas_Data_Volume_MB",
      "GNV_hour_19_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_19_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_19_WE_Video_Pass_Data_Volume_MB",
      "GNV_hour_19_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_19_WE_Music_Pass_Data_Volume_MB",
      "GNV_hour_20_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_20_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_20_W_MasMegas_Data_Volume_MB",
      "GNV_hour_20_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_20_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_20_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_21_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_21_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_21_W_MasMegas_Data_Volume_MB",
      "GNV_hour_21_W_Music_Pass_Data_Volume_MB",
      "GNV_hour_21_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_21_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_21_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_22_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_22_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_22_W_MasMegas_Data_Volume_MB",
      "GNV_hour_22_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_22_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_22_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_23_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_23_W_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_23_W_Video_Pass_Data_Volume_MB",
      "GNV_hour_23_W_MasMegas_Data_Volume_MB",
      "GNV_hour_23_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_23_WE_VideoHD_Pass_Data_Volume_MB",
      "GNV_hour_23_WE_MasMegas_Data_Volume_MB",
      "GNV_hour_23_WE_Music_Pass_Data_Volume_MB",
      "GNV_hour_0_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_0_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_0_W_MasMegas_Num_Of_Connections",
      "GNV_hour_0_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_0_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_0_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_0_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_0_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_0_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_1_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_1_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_1_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_1_W_MasMegas_Num_Of_Connections",
      "GNV_hour_1_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_1_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_1_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_1_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_1_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_1_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_2_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_2_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_2_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_2_W_MasMegas_Num_Of_Connections",
      "GNV_hour_2_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_2_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_2_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_2_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_2_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_2_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_3_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_3_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_3_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_3_W_MasMegas_Num_Of_Connections",
      "GNV_hour_3_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_3_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_3_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_3_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_3_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_3_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_4_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_4_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_4_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_4_W_MasMegas_Num_Of_Connections",
      "GNV_hour_4_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_4_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_4_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_4_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_4_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_4_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_5_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_5_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_5_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_5_W_MasMegas_Num_Of_Connections",
      "GNV_hour_5_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_5_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_5_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_5_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_5_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_5_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_6_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_6_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_6_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_6_W_MasMegas_Num_Of_Connections",
      "GNV_hour_6_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_6_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_6_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_6_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_6_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_6_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_7_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_7_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_7_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_7_W_MasMegas_Num_Of_Connections",
      "GNV_hour_7_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_7_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_7_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_7_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_7_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_7_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_8_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_8_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_8_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_8_W_MasMegas_Num_Of_Connections",
      "GNV_hour_8_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_8_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_8_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_8_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_8_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_8_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_9_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_9_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_9_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_9_W_MasMegas_Num_Of_Connections",
      "GNV_hour_9_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_9_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_9_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_9_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_9_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_9_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_10_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_10_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_10_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_10_W_MasMegas_Num_Of_Connections",
      "GNV_hour_10_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_10_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_10_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_10_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_10_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_10_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_11_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_11_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_11_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_11_W_MasMegas_Num_Of_Connections",
      "GNV_hour_11_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_11_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_11_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_11_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_11_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_11_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_12_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_12_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_12_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_12_W_MasMegas_Num_Of_Connections",
      "GNV_hour_12_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_12_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_12_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_12_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_12_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_12_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_13_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_13_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_13_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_13_W_MasMegas_Num_Of_Connections",
      "GNV_hour_13_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_13_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_13_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_13_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_13_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_13_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_14_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_14_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_14_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_14_W_MasMegas_Num_Of_Connections",
      "GNV_hour_14_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_14_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_14_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_14_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_14_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_14_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_15_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_15_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_15_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_15_W_MasMegas_Num_Of_Connections",
      "GNV_hour_15_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_15_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_15_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_15_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_15_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_15_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_16_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_16_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_16_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_16_W_MasMegas_Num_Of_Connections",
      "GNV_hour_16_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_16_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_16_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_16_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_16_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_16_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_17_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_17_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_17_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_17_W_MasMegas_Num_Of_Connections",
      "GNV_hour_17_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_17_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_17_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_17_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_17_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_17_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_18_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_18_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_18_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_18_W_MasMegas_Num_Of_Connections",
      "GNV_hour_18_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_18_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_18_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_18_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_18_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_18_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_19_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_19_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_19_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_19_W_MasMegas_Num_Of_Connections",
      "GNV_hour_19_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_19_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_19_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_19_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_19_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_19_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_20_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_20_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_20_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_20_W_MasMegas_Num_Of_Connections",
      "GNV_hour_20_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_20_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_20_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_20_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_20_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_20_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_21_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_21_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_21_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_21_W_MasMegas_Num_Of_Connections",
      "GNV_hour_21_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_21_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_21_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_21_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_21_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_21_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_22_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_22_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_22_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_22_W_MasMegas_Num_Of_Connections",
      "GNV_hour_22_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_22_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_22_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_22_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_22_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_22_WE_Music_Pass_Num_Of_Connections",
      "GNV_hour_23_W_Maps_Pass_Num_Of_Connections",
      "GNV_hour_23_W_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_23_W_Video_Pass_Num_Of_Connections",
      "GNV_hour_23_W_MasMegas_Num_Of_Connections",
      "GNV_hour_23_W_Music_Pass_Num_Of_Connections",
      "GNV_hour_23_WE_Maps_Pass_Num_Of_Connections",
      "GNV_hour_23_WE_VideoHD_Pass_Num_Of_Connections",
      "GNV_hour_23_WE_Video_Pass_Num_Of_Connections",
      "GNV_hour_23_WE_MasMegas_Num_Of_Connections",
      "GNV_hour_23_WE_Music_Pass_Num_Of_Connections",
      "total_data_volume_w_0",
      "total_data_volume_we_0",
      "total_data_volume_w_1",
      "total_data_volume_we_1",
      "total_data_volume_w_2",
      "total_data_volume_we_2",
      "total_data_volume_w_3",
      "total_data_volume_we_3",
      "total_data_volume_w_5",
      "total_data_volume_w_6",
      "total_data_volume_we_6",
      "total_data_volume_w_13",
      "total_data_volume_we_13",
      "total_data_volume_we_14",
      "total_data_volume_w_15",
      "total_data_volume_we_15",
      "total_data_volume_w_16",
      "total_data_volume_we_16",
      "total_data_volume_w_17",
      "total_data_volume_we_17",
      "total_data_volume_w_18",
      "total_data_volume_we_18",
      "total_data_volume_w_19",
      "total_data_volume_we_19",
      "total_data_volume_w_20",
      "total_data_volume_we_20",
      "total_data_volume_w_21",
      "total_data_volume_we_21",
      "total_data_volume",
      "total_connections",
      "data_per_connection_w",
      "data_per_connection",
      "max_data_volume_w",
      "Camp_NIFs_Delight_TEL_Target_0",
      "Camp_NIFs_Delight_TEL_Universal_0",
      "Camp_NIFs_Ignite_EMA_Target_0",
      "Camp_NIFs_Ignite_SMS_Control_0",
      "Camp_NIFs_Ignite_SMS_Target_0",
      "Camp_NIFs_Ignite_TEL_Universal_0",
      "Camp_NIFs_Legal_Informativa_SLS_Target_0",
      "Camp_NIFs_Retention_HH_SAT_Target_0",
      "Camp_NIFs_Retention_HH_TEL_Target_0",
      "Camp_NIFs_Retention_Voice_EMA_Control_0",
      "Camp_NIFs_Retention_Voice_EMA_Control_1",
      "Camp_NIFs_Retention_Voice_EMA_Target_0",
      "Camp_NIFs_Retention_Voice_EMA_Target_1",
      "Camp_NIFs_Retention_Voice_SAT_Control_0",
      "Camp_NIFs_Retention_Voice_SAT_Control_1",
      "Camp_NIFs_Retention_Voice_SAT_Target_0",
      "Camp_NIFs_Retention_Voice_SAT_Target_1",
      "Camp_NIFs_Retention_Voice_SAT_Universal_0",
      "Camp_NIFs_Retention_Voice_SAT_Universal_1",
      "Camp_NIFs_Retention_Voice_SMS_Control_0",
      "Camp_NIFs_Retention_Voice_SMS_Control_1",
      "Camp_NIFs_Retention_Voice_SMS_Target_0",
      "Camp_NIFs_Retention_Voice_SMS_Target_1",
      "Camp_NIFs_Terceros_TEL_Universal_0",
      "Camp_NIFs_Terceros_TER_Control_0",
      "Camp_NIFs_Terceros_TER_Control_1",
      "Camp_NIFs_Terceros_TER_Target_0",
      "Camp_NIFs_Terceros_TER_Target_1",
      "Camp_NIFs_Terceros_TER_Universal_0",
      "Camp_NIFs_Up_Cross_Sell_EMA_Control_0",
      "Camp_NIFs_Up_Cross_Sell_EMA_Control_1",
      "Camp_NIFs_Up_Cross_Sell_EMA_Target_0",
      "Camp_NIFs_Up_Cross_Sell_EMA_Target_1",
      "Camp_NIFs_Up_Cross_Sell_HH_EMA_Control_0",
      "Camp_NIFs_Up_Cross_Sell_HH_EMA_Control_1",
      "Camp_NIFs_Up_Cross_Sell_HH_EMA_Target_0",
      "Camp_NIFs_Up_Cross_Sell_HH_EMA_Target_1",
      "Camp_NIFs_Up_Cross_Sell_HH_MLT_Universal_0",
      "Camp_NIFs_Up_Cross_Sell_HH_NOT_Universal_0",
      "Camp_NIFs_Up_Cross_Sell_HH_SMS_Control_0",
      "Camp_NIFs_Up_Cross_Sell_HH_SMS_Control_1",
      "Camp_NIFs_Up_Cross_Sell_HH_SMS_Target_0",
      "Camp_NIFs_Up_Cross_Sell_HH_SMS_Target_1",
      "Camp_NIFs_Up_Cross_Sell_HH_SMS_Universal_0",
      "Camp_NIFs_Up_Cross_Sell_HH_TEL_Control_0",
      "Camp_NIFs_Up_Cross_Sell_HH_TEL_Control_1",
      "Camp_NIFs_Up_Cross_Sell_HH_TEL_Target_1",
      "Camp_NIFs_Up_Cross_Sell_HH_TEL_Universal_0",
      "Camp_NIFs_Up_Cross_Sell_HH_TEL_Universal_1",
      "Camp_NIFs_Up_Cross_Sell_MLT_Universal_0",
      "Camp_NIFs_Up_Cross_Sell_MMS_Control_0",
      "Camp_NIFs_Up_Cross_Sell_MMS_Control_1",
      "Camp_NIFs_Up_Cross_Sell_MMS_Target_0",
      "Camp_NIFs_Up_Cross_Sell_MMS_Target_1",
      "Camp_NIFs_Up_Cross_Sell_SMS_Control_0",
      "Camp_NIFs_Up_Cross_Sell_SMS_Control_1",
      "Camp_NIFs_Up_Cross_Sell_SMS_Target_1",
      "Camp_NIFs_Up_Cross_Sell_SMS_Universal_0",
      "Camp_NIFs_Up_Cross_Sell_TEL_Control_0",
      "Camp_NIFs_Up_Cross_Sell_TEL_Control_1",
      "Camp_NIFs_Up_Cross_Sell_TEL_Target_0",
      "Camp_NIFs_Up_Cross_Sell_TEL_Target_1",
      "Camp_NIFs_Up_Cross_Sell_TEL_Universal_0",
      "Camp_NIFs_Up_Cross_Sell_TEL_Universal_1",
      "Camp_NIFs_Up_Cross_Sell_TER_Target_0",
      "Camp_NIFs_Up_Cross_Sell_TER_Universal_0",
      "Camp_NIFs_Welcome_EMA_Target_0",
      "Camp_NIFs_Welcome_TEL_Target_0",
      "Camp_NIFs_Welcome_TEL_Universal_0",
      "Camp_SRV_Delight_NOT_Universal_0",
      "Camp_SRV_Delight_SMS_Control_0",
      "Camp_SRV_Delight_SMS_Universal_0",
      "Camp_SRV_Ignite_MMS_Target_0",
      "Camp_SRV_Ignite_NOT_Target_0",
      "Camp_SRV_Ignite_SMS_Target_0",
      "Camp_SRV_Ignite_SMS_Universal_0",
      "Camp_SRV_Legal_Informativa_EMA_Target_0",
      "Camp_SRV_Legal_Informativa_MLT_Universal_0",
      "Camp_SRV_Legal_Informativa_MMS_Target_0",
      "Camp_SRV_Legal_Informativa_NOT_Target_0",
      "Camp_SRV_Legal_Informativa_SMS_Target_0",
      "Camp_SRV_Retention_Voice_EMA_Control_0",
      "Camp_SRV_Retention_Voice_EMA_Control_1",
      "Camp_SRV_Retention_Voice_EMA_Target_0",
      "Camp_SRV_Retention_Voice_EMA_Target_1",
      "Camp_SRV_Retention_Voice_MLT_Universal_0",
      "Camp_SRV_Retention_Voice_NOT_Control_0",
      "Camp_SRV_Retention_Voice_NOT_Target_0",
      "Camp_SRV_Retention_Voice_NOT_Target_1",
      "Camp_SRV_Retention_Voice_NOT_Universal_0",
      "Camp_SRV_Retention_Voice_SAT_Control_0",
      "Camp_SRV_Retention_Voice_SAT_Control_1",
      "Camp_SRV_Retention_Voice_SAT_Target_0",
      "Camp_SRV_Retention_Voice_SAT_Target_1",
      "Camp_SRV_Retention_Voice_SAT_Universal_0",
      "Camp_SRV_Retention_Voice_SAT_Universal_1",
      "Camp_SRV_Retention_Voice_SLS_Control_0",
      "Camp_SRV_Retention_Voice_SLS_Control_1",
      "Camp_SRV_Retention_Voice_SLS_Target_0",
      "Camp_SRV_Retention_Voice_SLS_Target_1",
      "Camp_SRV_Retention_Voice_SLS_Universal_0",
      "Camp_SRV_Retention_Voice_SLS_Universal_1",
      "Camp_SRV_Retention_Voice_SMS_Control_0",
      "Camp_SRV_Retention_Voice_SMS_Control_1",
      "Camp_SRV_Retention_Voice_SMS_Target_1",
      "Camp_SRV_Retention_Voice_SMS_Universal_0",
      "Camp_SRV_Retention_Voice_SMS_Universal_1",
      "Camp_SRV_Retention_Voice_TEL_Control_1",
      "Camp_SRV_Retention_Voice_TEL_Target_1",
      "Camp_SRV_Retention_Voice_TEL_Universal_0",
      "Camp_SRV_Retention_Voice_TEL_Universal_1",
      "Camp_SRV_Up_Cross_Sell_EMA_Control_0",
      "Camp_SRV_Up_Cross_Sell_EMA_Control_1",
      "Camp_SRV_Up_Cross_Sell_EMA_Target_0",
      "Camp_SRV_Up_Cross_Sell_EMA_Target_1",
      "Camp_SRV_Up_Cross_Sell_EMA_Universal_0",
      "Camp_SRV_Up_Cross_Sell_HH_EMA_Control_0",
      "Camp_SRV_Up_Cross_Sell_HH_EMA_Control_1",
      "Camp_SRV_Up_Cross_Sell_HH_EMA_Target_0",
      "Camp_SRV_Up_Cross_Sell_HH_EMA_Target_1",
      "Camp_SRV_Up_Cross_Sell_HH_MLT_Universal_0",
      "Camp_SRV_Up_Cross_Sell_HH_MLT_Universal_1",
      "Camp_SRV_Up_Cross_Sell_HH_NOT_Control_0",
      "Camp_SRV_Up_Cross_Sell_HH_NOT_Control_1",
      "Camp_SRV_Up_Cross_Sell_HH_NOT_Target_0",
      "Camp_SRV_Up_Cross_Sell_HH_NOT_Target_1",
      "Camp_SRV_Up_Cross_Sell_HH_SMS_Control_0",
      "Camp_SRV_Up_Cross_Sell_HH_SMS_Control_1",
      "Camp_SRV_Up_Cross_Sell_HH_SMS_Target_0",
      "Camp_SRV_Up_Cross_Sell_HH_SMS_Target_1",
      "Camp_SRV_Up_Cross_Sell_HH_SMS_Universal_0",
      "Camp_SRV_Up_Cross_Sell_MLT_Universal_0",
      "Camp_SRV_Up_Cross_Sell_MMS_Control_0",
      "Camp_SRV_Up_Cross_Sell_MMS_Control_1",
      "Camp_SRV_Up_Cross_Sell_MMS_Target_0",
      "Camp_SRV_Up_Cross_Sell_MMS_Target_1",
      "Camp_SRV_Up_Cross_Sell_NOT_Control_0",
      "Camp_SRV_Up_Cross_Sell_NOT_Target_0",
      "Camp_SRV_Up_Cross_Sell_NOT_Target_1",
      "Camp_SRV_Up_Cross_Sell_NOT_Universal_0",
      "Camp_SRV_Up_Cross_Sell_SLS_Control_0",
      "Camp_SRV_Up_Cross_Sell_SLS_Control_1",
      "Camp_SRV_Up_Cross_Sell_SLS_Target_0",
      "Camp_SRV_Up_Cross_Sell_SLS_Target_1",
      "Camp_SRV_Up_Cross_Sell_SLS_Universal_0",
      "Camp_SRV_Up_Cross_Sell_SLS_Universal_1",
      "Camp_SRV_Up_Cross_Sell_SMS_Control_0",
      "Camp_SRV_Up_Cross_Sell_SMS_Control_1",
      "Camp_SRV_Up_Cross_Sell_SMS_Target_1",
      "Camp_SRV_Up_Cross_Sell_SMS_Universal_0",
      "Camp_SRV_Up_Cross_Sell_SMS_Universal_1",
      "Camp_SRV_Up_Cross_Sell_TEL_Control_0",
      "Camp_SRV_Up_Cross_Sell_TEL_Control_1",
      "Camp_SRV_Up_Cross_Sell_TEL_Target_0",
      "Camp_SRV_Up_Cross_Sell_TEL_Target_1",
      "Camp_SRV_Up_Cross_Sell_TEL_Universal_0",
      "Camp_SRV_Up_Cross_Sell_TEL_Universal_1",
      "Camp_SRV_Welcome_EMA_Target_0",
      "Camp_SRV_Welcome_MMS_Target_0",
      "Camp_SRV_Welcome_SMS_Target_0",
      "total_redem_Target",
      "pcg_redem_Target",
      "total_redem_Control",
      "total_redem_Universal",
      "total_redem_EMA",
      "total_redem_TEL",
      "total_camps_SLS",
      "total_redem_SLS",
      "total_camps_MLT",
      "total_redem_MLT",
      "total_camps_SAT",
      "total_redem_SAT",
      "total_camps_NOT",
      "total_redem_NOT",
      "total_redem_SMS",
      "pcg_redem_SMS",
      "total_camps_MMS",
      "total_redem_MMS",
      "total_redem_Retention_Voice",
      "total_redem_Up_Cross_Sell",
      "price_tariff",
      "GNV_hour_0_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_0_WE_Maps_Pass_Data_Volume_MB",
      "GNV_hour_1_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_1_WE_Video_Pass_Data_Volume_MB",
      "GNV_hour_2_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_4_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_8_WE_Music_Pass_Data_Volume_MB",
      "GNV_hour_9_W_Maps_Pass_Data_Volume_MB",
      "GNV_hour_16_WE_Music_Pass_Data_Volume_MB",
      "GNV_hour_0_W_RegularData_Num_Of_Connections",
      "GNV_hour_0_WE_RegularData_Num_Of_Connections",
      "GNV_hour_3_WE_MOU",
      "GNV_hour_7_W_Chat_Zero_Num_Of_Connections",
      "GNV_hour_8_W_Chat_Zero_Num_Of_Connections",
      "GNV_hour_9_W_Chat_Zero_Num_Of_Connections",
      "GNV_hour_12_W_Chat_Zero_Num_Of_Connections",
      "GNV_hour_13_W_Chat_Zero_Num_Of_Connections",
      "GNV_hour_14_W_Chat_Zero_Num_Of_Connections",
      "GNV_hour_15_WE_RegularData_Data_Volume_MB",
      "GNV_hour_16_W_Chat_Zero_Num_Of_Connections",
      "GNV_hour_17_W_Chat_Zero_Num_Of_Connections",
      "GNV_hour_18_W_Chat_Zero_Num_Of_Connections",
      "GNV_hour_18_W_RegularData_Num_Of_Connections",
      "GNV_hour_19_W_Chat_Zero_Num_Of_Connections",
      "GNV_hour_19_W_RegularData_Num_Of_Connections",
      "GNV_hour_20_W_Chat_Zero_Num_Of_Connections",
      "GNV_hour_20_W_RegularData_Num_Of_Connections",
      "GNV_hour_21_W_Chat_Zero_Num_Of_Connections",
      "GNV_hour_21_W_RegularData_Num_Of_Connections",
      "total_connections_w",
      "total_data_volume_we",
      "total_data_volume_w",
      "Camp_NIFs_Delight_SMS_Control_0",
      "Camp_NIFs_Delight_SMS_Target_0",
      "Camp_NIFs_Legal_Informativa_EMA_Target_0",
      "Camp_SRV_Delight_EMA_Target_0",
      "Camp_SRV_Delight_MLT_Universal_0",
      "Camp_SRV_Delight_NOT_Target_0",
      "Camp_SRV_Delight_TEL_Universal_0",
      "Camp_SRV_Retention_Voice_TEL_Control_0",
      "total_camps_TEL",
      "total_camps_Target",
      "total_camps_Up_Cross_Sell"
    )

  }

  def removeConstantFeatures(dt: Dataset[Row], noinputFeats: Array[String]): Array[String] = {

    println("\n[Info FeatureSelection] Removing feats with zero variance\n")

    val numericTypes = Array("DoubleType", "FloatType", "IntegerType", "LongType")

    val dtStruct = dt.dtypes

    val contFeats = dtStruct
      .filter(f => numericTypes.contains(f._2))
      .map(_._1)
      .diff(noinputFeats)

    contFeats.foreach(f => println("\n[Info FeatureSelection] Numerical feat: " + f + "\n"))


    val remContFeats = contFeats.foldLeft(Array[String]())((acc, f) => {

     println("\n[Info FeatureSelection] Computing variance of feat " + f + "\n")

      val sigma2 = dt
       .select(variance(col(f).cast("double")).cast("double"))
        .first
        .getDouble(0)
       //.withColumn(f, col(f).cast("double"))
       //.rdd
       //.map(_.getDouble(0))
       //.variance()

     println("\n[Info FeatureSelection] Variance of feat " + f + ": " + sigma2 + "\n")

     if(sigma2 > 0) acc else acc :+ f

   })

    val catFeats = dtStruct
      .map(_._1)
      .diff(contFeats)
      .diff(noinputFeats)

    val remCatFeats = catFeats.foldLeft(Array[String]())((acc, f) => {
      val numDistinct = dt.select(f).distinct().count()

      println("\n[Info FeatureSelection] Num distinct values for feat " + f + ": " + numDistinct + "\n")

      if(numDistinct > 1) acc else acc :+ f
    })

    val remFeats = remContFeats ++ remCatFeats

    remFeats.foreach(x => println("\n[Info FeatureSelection] Feat to remove due to zero variance: " + x + "\n"))

    remFeats

    /*
    val outDt = if(remFeats.isEmpty) dt
    else {
      remFeats.foreach(x => println("\n[Info FeatureSelection] Feat to removed due to zero variance: " + x + "\n"))
      val selFeats = dt.columns.diff(remFeats)
      dt.select(selFeats.head, selFeats.tail: _*)
    }

    outDt
    */

  }

  def removeCorrelatedFeatures(dt: Dataset[Row], th: Double, proportion: Double, noinputFeats: Array[String]): Array[String] = {

    println("\n[Info FeatureSelection] Removing highly correlated feats\n")

    val numericTypes = Array("DoubleType", "FloatType", "IntegerType", "LongType")

    val dtStruct = dt.dtypes

    val contFeats = dtStruct
      .filter(f => numericTypes.contains(f._2))
      .map(_._1)
      .diff(noinputFeats)

    println("\n[Info FeatureSelection] Number of numeric feats: " + contFeats.length + "\n")

    val pairs = contFeats
      .toSet
      .subsets(2)
      .toArray
      .map(_.toArray)

    val nPairs = pairs.length

    println("\n[Info FeatureSelection] Number of pairs: " + nPairs + "\n")

    val numSubsetPairs = math.round(proportion*nPairs).toInt

    val shufflePairs = scala.util.Random.shuffle((0 to nPairs)).toArray.slice(0, numSubsetPairs)

    val subsetPairs = shufflePairs.foldLeft(Array[Array[String]]())((acc, e) => acc :+ pairs(e))

    // val subsetPairs = shufflePairs.slice(0, numSubsetPairs)

    val remFeats = subsetPairs.foldLeft(Array[String]())((acc, p) => {

      val pairCorr = dt.select(p.head, p.tail: _*)
        .stat
        .corr(p(0), p(1))

      println("[Info FeatureSelection] Corr between " + p(0) + " and " + p(1) + " is " + pairCorr + "\n")

      (pairCorr >= th) match {

        case true => acc :+ p(1)

        case false => acc
      }

    })

    remFeats.foreach(x => println("\n[Info FeatureSelection] Feat to remove due to high corr: " + x + "\n"))

    remFeats

  }

  def removeCorrelatedFeatures(dt: Dataset[Row], th: Double, numSubsetPairs: Int, noinputFeats: Array[String]): Array[String] = {

    println("\n[Info FeatureSelection] Removing highly correlated feats\n")

    val numericTypes = Array("DoubleType", "FloatType", "IntegerType", "LongType")

    val dtStruct = dt.dtypes

    val contFeats = dtStruct
      .filter(f => numericTypes.contains(f._2))
      .map(_._1)
      .diff(noinputFeats)

    println("\n[Info FeatureSelection] Number of numeric feats: " + contFeats.length + "\n")

    val pairs = contFeats
      .toSet
      .subsets(2)
      .toArray
      .map(_.toArray)

    val nPairs = pairs.length

    println("\n[Info FeatureSelection] Number of pairs: " + nPairs + "\n")

    val shufflePairs = scala.util.Random
      .shuffle((0 to nPairs))
      .toArray
      .slice(0, numSubsetPairs)

    val subsetPairs = shufflePairs.foldLeft(Array[Array[String]]())((acc, e) => acc :+ pairs(e))

    // val subsetPairs = shufflePairs.slice(0, numSubsetPairs)

    val remFeats = subsetPairs.foldLeft(Array[String]())((acc, p) => {

      val pairCorr = dt.select(p.head, p.tail: _*)
        .stat
        .corr(p(0), p(1))

      println("[Info FeatureSelection] Corr between " + p(0) + " and " + p(1) + " is " + pairCorr + "\n")

      (pairCorr >= th) match {

        case true => acc :+ p(1)

        case false => acc
      }

    })

    remFeats.foreach(x => println("\n[Info FeatureSelection] Feat to remove due to high corr: " + x + "\n"))

    remFeats

  }

}
