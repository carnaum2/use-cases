#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from pyspark.sql.functions import (desc,
                                   asc,
                                   sum as sql_sum,
                                   avg as sql_avg,
                                   max as sql_max,
                                   isnull,
                                   when,
                                   col,
                                   isnan,
                                   count,
                                   row_number,
                                   lit,
                                   coalesce,
                                   concat,
                                   lpad,
                                   unix_timestamp,
                                   from_unixtime,
                                   greatest,
                                   udf,
                                   countDistinct,
                                   regexp_replace,
                                   split,
                                   expr,
                                   length)
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.sql.types import DoubleType


def set_paths():
    import sys, os, re

    pathname = os.path.dirname(sys.argv[0])  # abs path to file (not included)
    print("pathname", pathname)

    if pathname.startswith("/var/SP/data/bdpmdses/deliveries_churn/"):
        import re
        root_dir = re.match("^(.*)use-cases(.*)", pathname).group(1)
    else:
        root_dir = re.match("(.*)use-cases/churn(.*)", pathname).group(1)
    print("Detected '{}' as root dir".format(root_dir))

    if root_dir not in sys.path:
        sys.path.append(root_dir)
        print("Added '{}' to path".format(root_dir))

    mypath = os.path.join(root_dir, "use-cases")
    if mypath not in sys.path:
        sys.path.append(mypath)
        print("Added '{}' to path".format(mypath))

    sys.path.append('/var/SP/data/home/adesant3/temp/amdocs_inf_dataset/')

def get_noninf_features():
    """

        : return: set of features in the IDS that should not be used as input since they do not provide relevant information;
        instead, proper aggregates on these features are included as model inputs.

        """

    non_inf_feats = (["Serv_L2_days_since_Serv_fx_data",
                      "Cust_Agg_L2_total_num_services_nc",
                      "Cust_Agg_L2_total_num_services_nif",
                      "GNV_Data_hour_0_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_0_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_0_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_0_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_1_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_1_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_1_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_1_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_1_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_2_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_2_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_2_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_2_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_2_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_3_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_3_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_3_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_3_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_3_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_3_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_4_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_4_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_4_W_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_4_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_4_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_4_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_5_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_5_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_5_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_5_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_5_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_5_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_5_WE_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_6_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_6_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_6_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_6_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_6_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_7_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_7_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_7_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_7_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_7_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_7_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_8_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_8_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_8_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_8_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_8_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_8_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_9_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_9_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_9_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_9_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_9_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_10_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_10_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_10_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_10_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_10_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_10_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_11_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_11_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_11_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_11_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_11_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_11_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_12_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_12_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_12_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_12_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_12_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_12_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_13_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_13_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_13_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_13_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_13_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_14_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_14_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_14_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_14_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_14_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_14_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_14_WE_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_15_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_15_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_15_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_15_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_15_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_15_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_16_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_16_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_16_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_16_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_16_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_16_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_17_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_17_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_17_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_17_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_17_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_17_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_18_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_18_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_18_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_18_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_18_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_18_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_19_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_19_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_19_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_19_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_19_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_19_WE_Video_Pass_Data_Volume_MB",
                      "GNV_Data_hour_19_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_19_WE_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_20_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_20_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_20_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_20_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_20_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_20_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_21_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_21_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_21_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_21_W_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_21_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_21_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_21_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_22_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_22_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_22_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_22_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_22_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_22_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_23_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_23_W_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_23_W_Video_Pass_Data_Volume_MB",
                      "GNV_Data_hour_23_W_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_23_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_23_WE_VideoHD_Pass_Data_Volume_MB",
                      "GNV_Data_hour_23_WE_MasMegas_Data_Volume_MB",
                      "GNV_Data_hour_23_WE_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_0_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_0_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_0_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_0_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_0_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_0_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_0_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_0_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_0_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_1_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_1_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_1_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_2_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_2_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_2_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_3_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_3_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_3_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_4_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_4_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_4_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_5_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_5_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_5_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_6_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_6_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_6_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_7_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_7_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_7_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_8_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_8_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_8_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_9_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_9_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_9_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_10_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_10_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_10_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_11_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_11_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_11_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_12_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_12_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_12_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_13_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_13_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_13_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_14_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_14_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_14_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_15_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_15_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_15_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_16_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_16_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_16_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_17_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_17_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_17_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_18_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_18_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_18_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_19_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_19_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_19_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_20_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_20_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_20_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_21_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_21_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_21_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_22_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_22_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_22_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_W_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_W_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_W_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_W_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_23_W_Music_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_WE_Maps_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_WE_VideoHD_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_WE_Video_Pass_Num_Of_Connections",
                      "GNV_Data_hour_23_WE_MasMegas_Num_Of_Connections",
                      "GNV_Data_hour_23_WE_Music_Pass_Num_Of_Connections",
                      "GNV_Data_L2_total_data_volume_W_0",
                      "GNV_Data_L2_total_data_volume_WE_0",
                      "GNV_Data_L2_total_data_volume_W_1",
                      "GNV_Data_L2_total_data_volume_WE_1",
                      "GNV_Data_L2_total_data_volume_W_2",
                      "GNV_Data_L2_total_data_volume_WE_2",
                      "GNV_Data_L2_total_data_volume_W_3",
                      "GNV_Data_L2_total_data_volume_WE_3",
                      "GNV_Data_L2_total_data_volume_W_5",
                      "GNV_Data_L2_total_data_volume_W_6",
                      "GNV_Data_L2_total_data_volume_WE_6",
                      "GNV_Data_L2_total_data_volume_W_13",
                      "GNV_Data_L2_total_data_volume_WE_13",
                      "GNV_Data_L2_total_data_volume_WE_14",
                      "GNV_Data_L2_total_data_volume_W_15",
                      "GNV_Data_L2_total_data_volume_WE_15",
                      "GNV_Data_L2_total_data_volume_W_16",
                      "GNV_Data_L2_total_data_volume_WE_16",
                      "GNV_Data_L2_total_data_volume_W_17",
                      "GNV_Data_L2_total_data_volume_WE_17",
                      "GNV_Data_L2_total_data_volume_W_18",
                      "GNV_Data_L2_total_data_volume_WE_18",
                      "GNV_Data_L2_total_data_volume_W_19",
                      "GNV_Data_L2_total_data_volume_WE_19",
                      "GNV_Data_L2_total_data_volume_W_20",
                      "GNV_Data_L2_total_data_volume_WE_20",
                      "GNV_Data_L2_total_data_volume_W_21",
                      "GNV_Data_L2_total_data_volume_WE_21",
                      "GNV_Data_L2_total_data_volume",
                      "GNV_Data_L2_total_connections",
                      "GNV_Data_L2_data_per_connection_W",
                      "GNV_Data_L2_data_per_connection",
                      "GNV_Data_L2_max_data_volume_W",
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
                      "Camp_L2_srv_total_redem_Target",
                      "Camp_L2_srv_pcg_redem_Target",
                      "Camp_L2_srv_total_redem_Control",
                      "Camp_L2_srv_total_redem_Universal",
                      "Camp_L2_srv_total_redem_EMA",
                      "Camp_L2_srv_total_redem_TEL",
                      "Camp_L2_srv_total_camps_SAT",
                      "Camp_L2_srv_total_redem_SAT",
                      "Camp_L2_srv_total_redem_SMS",
                      "Camp_L2_srv_pcg_redem_SMS",
                      "Camp_L2_srv_total_camps_MMS",
                      "Camp_L2_srv_total_redem_MMS",
                      "Camp_L2_srv_total_redem_Retention_Voice",
                      "Camp_L2_srv_total_redem_Up_Cross_Sell",
                      "Camp_L2_nif_total_redem_Target",
                      "Camp_L2_nif_pcg_redem_Target",
                      "Camp_L2_nif_total_redem_Control",
                      "Camp_L2_nif_total_redem_Universal",
                      "Camp_L2_nif_total_redem_EMA",
                      "Camp_L2_nif_total_redem_TEL",
                      "Camp_L2_nif_total_camps_SAT",
                      "Camp_L2_nif_total_redem_SAT",
                      "Camp_L2_nif_total_redem_SMS",
                      "Camp_L2_nif_pcg_redem_SMS",
                      "Camp_L2_nif_total_camps_MMS",
                      "Camp_L2_nif_total_redem_MMS",
                      "Camp_L2_nif_total_redem_Retention_Voice",
                      "Camp_L2_nif_total_redem_Up_Cross_Sell",
                      "Serv_PRICE_TARIFF",
                      "GNV_Data_hour_0_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_0_WE_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_1_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_1_WE_Video_Pass_Data_Volume_MB",
                      "GNV_Data_hour_2_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_4_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_8_WE_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_9_W_Maps_Pass_Data_Volume_MB",
                      "GNV_Data_hour_16_WE_Music_Pass_Data_Volume_MB",
                      "GNV_Data_hour_0_W_RegularData_Num_Of_Connections",
                      "GNV_Data_hour_0_WE_RegularData_Num_Of_Connections",
                      "GNV_Voice_hour_23_WE_MOU",  ### solo un mou??
                      "GNV_Data_hour_7_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_8_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_9_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_12_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_13_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_14_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_15_WE_RegularData_Data_Volume_MB",
                      "GNV_Data_hour_16_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_17_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_18_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_18_W_RegularData_Num_Of_Connections",
                      "GNV_Data_hour_19_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_19_W_RegularData_Num_Of_Connections",
                      "GNV_Data_hour_20_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_20_W_RegularData_Num_Of_Connections",
                      "GNV_Data_hour_21_W_Chat_Zero_Num_Of_Connections",
                      "GNV_Data_hour_21_W_RegularData_Num_Of_Connections",
                      "GNV_Roam_Data_L2_total_connections_W",
                      "GNV_Roam_Data_L2_total_data_volume_WE",
                      "GNV_Roam_Data_L2_total_data_volume_W",
                      "Camp_NIFs_Delight_SMS_Control_0",
                      "Camp_NIFs_Delight_SMS_Target_0",
                      "Camp_NIFs_Legal_Informativa_EMA_Target_0",
                      "Camp_SRV_Delight_EMA_Target_0",
                      "Camp_SRV_Delight_MLT_Universal_0",
                      "Camp_SRV_Delight_NOT_Target_0",
                      "Camp_SRV_Delight_TEL_Universal_0",
                      "Camp_SRV_Retention_Voice_TEL_Control_0",
                      "Camp_L2_srv_total_camps_TEL",
                      "Camp_L2_srv_total_camps_Target",
                      "Camp_L2_srv_total_camps_Up_Cross_Sell",
                      "Camp_L2_nif_total_camps_TEL",
                      "Camp_L2_nif_total_camps_Target",
                      "Camp_L2_nif_total_camps_Up_Cross_Sell",
                      "Cust_Agg_flag_prepaid_nc",
                      "tgs_ind_riesgo_mm",
                      "tgs_ind_riesgo_mv",
                      "tgs_meses_fin_dto_ok",
                      "CCC_L2_bucket_1st_interaction",
                      "tgs_tg_marta",
                      "CCC_L2_bucket_latest_interaction",
                      "tgs_ind_riesgo_o2",
                      "tgs_ind_riesgo_max",
                      "tgs_sol_24m",
                      "CCC_L2_latest_interaction",
                      "tgs_blinda_bi_pos_n12",
                      "CCC_L2_first_interaction"
                      ])
    return non_inf_feats

def get_id_features():

    """
    :return: id columns in the IDS (must never be used as an input to the model)
    """
    return ["msisdn", "serv_rgu", "num_cliente", "CAMPO1", "CAMPO2", "CAMPO3", "NIF_CLIENTE", "IMSI", "Instancia_P",
            "nif_cliente_tgs", "num_cliente_tgs", "msisdn_d", "num_cliente_d", "nif_cliente_d"]


def get_no_input_feats():

    """
    :return: set of attributes that have been identified as invalid
    """



    return ["Cust_x_user_facebook", "Cust_x_user_twitter", "Cust_codigo_postal",
            "Cust_NOMBRE", "Cust_PRIM_APELLIDO", "Cust_SEG_APELLIDO", "Cust_DIR_LINEA1", "Cust_DIR_LINEA2",
            "Cust_NOM_COMPLETO", "Cust_DIR_FACTURA1", "Cust_DIR_FACTURA2", "Cust_DIR_FACTURA3", "Cust_DIR_FACTURA4",
            "Cust_CODIGO_POSTAL", "Cust_TRAT_FACT", "Cust_NOMBRE_CLI_FACT", "Cust_APELLIDO1_CLI_FACT",
            "Cust_APELLIDO2_CLI_FACT", "Cust_DIR_LINEA1", "Cust_NOM_COMPLETO", "Cust_DIR_FACTURA1", "Cust_DIR_FACTURA2",
            "Cust_DIR_FACTURA3", "Cust_DIR_FACTURA4", "Cust_CODIGO_POSTAL", "Cust_TRAT_FACT", "Cust_NOMBRE_CLI_FACT",
            "Cust_APELLIDO1_CLI_FACT", "Cust_APELLIDO2_CLI_FACT", "Cust_CTA_CORREO_CONTACTO", "Cust_CTA_CORREO",
            "Cust_FACTURA_CATALAN", "Cust_NIF_FACTURACION", "Cust_TIPO_DOCUMENTO", "Cust_X_PUBLICIDAD_EMAIL",
            "Cust_x_tipo_cuenta_corp", "Cust_FLG_LORTAD", "Serv_MOBILE_HOMEZONE", "CCC_L2_bucket_list",
            "CCC_L2_bucket_set", "Serv_NUM_SERIE_DECO_TV", "Order_N1_Description", "Order_N2_Description",
            "Order_N5_Description", "Order_N7_Description", "Order_N8_Description", "Order_N9_Description",
            "Penal_CUST_APPLIED_N1_cod_penal", "Penal_CUST_APPLIED_N1_desc_penal", "Penal_CUST_FINISHED_N1_cod_promo",
            "Penal_CUST_FINISHED_N1_desc_promo", "device_n2_imei", "device_n1_imei", "device_n3_imei", "device_n4_imei",
            "device_n5_imei", "Order_N1_Id", "Order_N2_Id", "Order_N3_Id", "Order_N4_Id", "Order_N5_Id", "Order_N6_Id",
            "Order_N7_Id", "Order_N8_Id", "Order_N9_Id", "Order_N10_Id"]

def get_base_sfid(spark, start_date, end_date, verbose=True):
    '''
    spark:
    start_date: starting date of the time window considered for the observation of purchasing orders
    end_date: ending date of the time window considered for the observation of purchasing orders

    The function computes a structure with the following fields:
    - num_cliente: customer id (several msisdn may be associated with the same num_cliente)
    - num_buy_orders: for a given num_cliente, number of purchasing orders observed between start_date and end_date
    - num_digital_buy_orders: for a given num_cliente, number of purchasing orders observed between start_date and end_date by means of a digital channel
    - pcg_digital_orders: percentage of digital purchasing orders in the interval between start_date and end_date
    '''

    # 1. Class order: this info is required to retain purchasing orders
    class_order_df = spark.read.table("raw_es.cvm_classesorder") \
        .select("OBJID", "X_CLASIFICACION", "year", "month", "day")

    from pyspark.sql import Window

    w_orderclass = Window().partitionBy("OBJID").orderBy(desc("year"), desc("month"), desc("day"))

    class_order_df = class_order_df \
        .filter(from_unixtime(unix_timestamp(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')),
                                             'yyyyMMdd')) <= from_unixtime(unix_timestamp(lit(end_date), 'yyyyMMdd'))) \
        .withColumn("rowNum", row_number().over(w_orderclass)) \
        .filter(col('rowNum') == 1)

    class_order_df.repartition(400)

    class_order_df.cache()

    # 2. CRM Order: all the orders

    window = Window.partitionBy("NUM_CLIENTE", "INSTANCIA_SRV").orderBy(asc("FECHA_WO_COMPLETA"), asc("HORA_CIERRE"))

    orders_crm_df = (spark
                     .read
                     .table("raw_es.customerprofilecar_ordercrmow")
                     .filter(
        (from_unixtime(unix_timestamp(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')),
                                      'yyyyMMdd')) >= from_unixtime(unix_timestamp(lit(start_date), 'yyyyMMdd')))
        &
        (from_unixtime(unix_timestamp(concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')),
                                      'yyyyMMdd')) <= from_unixtime(unix_timestamp(lit(end_date), 'yyyyMMdd')))
    )
                     .select("NUM_CLIENTE", "INSTANCIA_SRV", "NUM_VEND", "WDQ01", "WDQ02", "COD_SERVICIO",
                             "FECHA_WO_COMPLETA", "HORA_CIERRE", "CLASE_ORDEN", "ESTADO")
                     .withColumnRenamed("CLASE_ORDEN", "OBJID")
                     .filter(col("ESTADO") == "CP").withColumn("row_nb", row_number().over(window))
                     .filter((col("row_nb") == 1) & (col("WDQ01") == 0) & (col("WDQ02") == 1))
                     )

    # 3. Base: customer IDs found in the base on end_date

    from churn_nrt.src.data.customer_base import CustomerBase

    base_df = CustomerBase(spark) \
        .get_module(end_date, save=False, save_others=False, force_gen=True) \
        .select("NUM_CLIENTE") \
        .distinct()

    base_df.repartition(400)

    base_df.cache()

    if verbose:
        print "Number of customers in the base: " + str(base_df.count())

    # 4. Filtering the orders by using the base: only those orders from customers in the base on end_date are reatined

    filt_orders_crm_df = orders_crm_df.join(base_df, ['NUM_CLIENTE'], 'inner')

    filt_orders_crm_df.repartition(400)

    filt_orders_crm_df.cache()

    # 5. Join orders and class: Field X_CLASIFICACION is added to the orders (X_CLASIFICACION specifies the order type)

    order_crm_sfid = filt_orders_crm_df \
        .join(class_order_df, ['OBJID'], 'left') \
        .filter(col("X_CLASIFICACION").isin('Instalacion', 'Reconexion', 'Aumento')) \
        .distinct()

    order_crm_sfid.repartition(400)

    order_crm_sfid.cache()

    if verbose:
        print "[Info] Volume: " + str(order_crm_sfid.count()) + " - Num distinct OBJID: " + str(
            order_crm_sfid.select("OBJID").distinct().count()) + " - Num distinct NUM_CLIENTE: " + str(
            order_crm_sfid.select("NUM_CLIENTE").distinct().count())

    # 6. Labeling NUM_VEND as digital or not

    digital_sfid_df = spark.read.csv("/user/jmarcoso/data/lista_sfid_digital.csv", header=True, sep=";") \
        .filter(col("VERTICAL") == "DIGITAL").select("SFID").distinct().withColumn("flag_sfid_digital", lit(1.0))

    order_crm_sfid_df = order_crm_sfid \
        .withColumnRenamed("NUM_VEND", "SFID") \
        .join(digital_sfid_df, ["SFID"], "left").na.fill({"flag_sfid_digital": 0.0})

    order_crm_sfid_df.cache()

    # 7. Aggregating by num_cliente

    nc_order_crm_sfid_df = order_crm_sfid_df \
        .groupBy("NUM_CLIENTE") \
        .agg(count("*").alias("num_buy_orders"), sql_sum("flag_sfid_digital").alias("num_digital_buy_orders"))

    # 8. For each customer in the base, computing the number of purchasing orders and the number of purchasing orders by using a digital channel

    base_df = base_df \
        .join(nc_order_crm_sfid_df, ['num_cliente'], 'left') \
        .na.fill({'num_buy_orders': 0.0, 'num_digital_buy_orders': 0.0}) \
        .withColumn('pcg_digital_orders', when(col('num_buy_orders') > 0,
                                               col('num_digital_buy_orders').cast('double') / col(
                                                   'num_buy_orders').cast('double')).otherwise(0.0))
    return base_df


def get_kpis(spark, date_):

    # List of KPIs to track
    
    year_ = date_[0:4]
    month_ = date_[4:6]
    day_ = date_[6:8]

    sel_cols = ['nif_cliente',
                'CCC_num_calls_w4',
                'Cust_Agg_L2_fbb_fx_first_days_since_nif_agg_mean',
                'Cust_Agg_L2_fixed_fx_first_days_since_nif_agg_mean',
                'Cust_Agg_L2_mobile_fx_first_days_since_nif_agg_mean',
                'Cust_Agg_L2_tv_fx_first_days_since_nif_agg_mean',
                'Cust_Agg_L2_total_num_services_nif_agg_mean',
                'Cust_Agg_fbb_services_nif_agg_mean',
                'Cust_Agg_fixed_services_nif_agg_mean',
                'Cust_Agg_mobile_services_nif_agg_mean',
                'Cust_Agg_tv_services_nif_agg_mean',
                'GNV_Data_L2_total_data_volume_agg_mean',
                'Bill_N1_Amount_To_Pay_agg_mean',
                'tgs_days_until_f_fin_bi_agg_mean']

    kpis = ['nif_cliente',
            'num_services',
            'num_fbb_services',
            'num_fixed_services',
            'num_mobile_services',
            'num_tv_services',
            'tgs_days_until_f_fin_bi_agg_mean',
            'data_usage_mean',
            'arpu',
            'foc',
            'tenure',
            'tenure_fbb',
            'tenure_fixed',
            'tenure_mobile',
            'tenure_tv']

    from churn_nrt.src.data_utils.ids_utils import get_ids_nif

    ids_nif_df = get_ids_nif(spark, date_)\
        .select(sel_cols)\
        .withColumn('foc', col('CCC_num_calls_w4').cast("double") / lit(28.0)) \
        .withColumn('tenure', greatest('Cust_Agg_L2_fbb_fx_first_days_since_nif_agg_mean', 'Cust_Agg_L2_fixed_fx_first_days_since_nif_agg_mean', 'Cust_Agg_L2_mobile_fx_first_days_since_nif_agg_mean', 'Cust_Agg_L2_tv_fx_first_days_since_nif_agg_mean')) \
        .withColumnRenamed('Cust_Agg_L2_fbb_fx_first_days_since_nif_agg_mean', 'tenure_fbb') \
        .withColumnRenamed('Cust_Agg_L2_fixed_fx_first_days_since_nif_agg_mean', 'tenure_fixed') \
        .withColumnRenamed('Cust_Agg_L2_mobile_fx_first_days_since_nif_agg_mean', 'tenure_mobile') \
        .withColumnRenamed('Cust_Agg_L2_tv_fx_first_days_since_nif_agg_mean', 'tenure_tv') \
        .withColumnRenamed('Cust_Agg_L2_total_num_services_nif_agg_mean', 'num_services') \
        .withColumnRenamed('Cust_Agg_fbb_services_nif_agg_mean', 'num_fbb_services') \
        .withColumnRenamed('Cust_Agg_fixed_services_nif_agg_mean', 'num_fixed_services') \
        .withColumnRenamed('Cust_Agg_mobile_services_nif_agg_mean', 'num_mobile_services') \
        .withColumnRenamed('Cust_Agg_tv_services_nif_agg_mean', 'num_tv_services') \
        .withColumnRenamed("Bill_N1_Amount_To_Pay_agg_mean", "arpu") \
        .withColumnRenamed("GNV_Data_L2_total_data_volume_agg_mean", "data_usage_mean") \
        .select(kpis)

    ids_nif_df.printSchema()


    print "[Info] Size of ids_nif_aggs_df: " + str(ids_nif_df.count()) + " - Num distinct NIFs in ids_nif_aggs_df: " + str(ids_nif_df.select('nif_cliente').distinct().count())

    # Adding churn30 label

    from churn_nrt.src.data.sopos_dxs import Target

    target_nif_df = Target(spark, churn_window=30, level='nif')\
        .get_module(date_, save=False, save_others=False, force_gen=True)\
        .withColumnRenamed("label", "churn30")

    kpis_df = ids_nif_df\
        .join(target_nif_df, on=["nif_cliente"], how="inner")

    return kpis_df

def get_base_segmentation(spark, start_date, end_date, app_level = 'msisdn'):
    '''
    :param spark:
    :return: df with the structure msisdn, hero
    '''

    # Getting fields of the date

    year_ = end_date[0:4]
    month_ = end_date[4:6]
    day_ = end_date[6:8]

    # 0. Getting the base of msisdn services (that will be used as a reference)

    from churn_nrt.src.data.customer_base import CustomerBase

    mob_base_df = CustomerBase(spark) \
        .get_module(end_date, save=False, save_others=False, force_gen=True) \
        .filter(col("rgu") == "mobile") \
        .select("msisdn", "num_cliente", "nif_cliente") \
        .distinct()

    mob_base_df.repartition(400)

    mob_base_df.cache()

    # 1. Getting the base (num_cliente) with columns related to recent purchasing orders (sfid) and adding them to the reference base

    sfid_df = get_base_sfid(spark, start_date, end_date, verbose=True)

    if(app_level != 'msisdn'):
        mob_base_df = mob_base_df\
            .join(sfid_df, ['num_cliente'], 'inner')\
            .select('msisdn', app_level, 'num_buy_orders', 'num_digital_buy_orders', 'pcg_digital_orders')\
            .distinct()
    else:
        mob_base_df = mob_base_df \
            .join(sfid_df, ['num_cliente'], 'inner') \
            .select(app_level, 'num_buy_orders', 'num_digital_buy_orders', 'pcg_digital_orders') \
            .distinct()


    # 2. Adding Adobe-based columns_ number of recent visits

    app_df = (spark
              .read
              .table("raw_es.customerprofilecar_adobe_sections")
              .withColumn('formatted_date', concat(col('year'), lpad(col('month'), 2, '0'), lpad(col('day'), 2, '0')))
              .filter(
        (from_unixtime(unix_timestamp(col('formatted_date'), 'yyyyMMdd')) >= from_unixtime(
            unix_timestamp(lit(start_date), 'yyyyMMdd')))
        &
        (from_unixtime(unix_timestamp(col('formatted_date'), 'yyyyMMdd')) <= from_unixtime(
            unix_timestamp(lit(end_date), 'yyyyMMdd')))
    ).withColumn("msisdn", expr("substring(msisdn, 3, length(msisdn)-2)"))
              .join(mob_base_df, ["msisdn"], 'inner')
              .groupBy(app_level).agg(count("*").alias('nb_app_access'), countDistinct('formatted_date').alias('nb_days_access'))
              .distinct())

    mob_base_df = mob_base_df.join(app_df, [app_level], 'left').na.fill({'nb_app_access': 0.0, 'nb_days_access': 0.0})

    mob_base_df.cache()

    print "[Info] Size of mob_base_df: " + str(mob_base_df.count())

    # 9. Adding number of incoming cols (from IDS as CCC module may fail)

    call_df = (spark
               .read
               .load(
        '/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/year=' + year_ + '/month=' + month_ + '/day=' + day_)
               .select(app_level, 'CCC_num_calls_w4').distinct()
               .withColumnRenamed('CCC_num_calls_w4', 'num_calls'))

    mob_base_df = mob_base_df.join(call_df, [app_level], 'left').na.fill({'num_calls': 0.0})

    mob_base_df.cache()

    print "[Info] Size of mob_base_df: " + str(mob_base_df.count())

    # 3. Applying the conditions to define the poles: digital vs traditional


    mob_base_df = mob_base_df \
        .withColumn("hero", lit(-1)) \
        .withColumn("hero", when((col('num_digital_buy_orders') > 0) | (col('nb_app_access') > 0), 1).otherwise(col("hero"))) \
        .withColumn("hero", when(((col('num_buy_orders') > 0) & (col('num_digital_buy_orders') == 0)) | ((col('nb_app_access') == 0) & (col('num_calls') > 0)), 0).otherwise(col("hero")))


    '''
    mob_base_df = mob_base_df \
        .withColumn("hero", lit(-1)) \
        .withColumn("hero", when((col('num_digital_buy_orders') > 0) | ((col('nb_days_access') > 0) & (col('num_calls') == 0)), 1).otherwise(col("hero"))) \
        .withColumn("hero", when(((col('num_buy_orders') > 0) & (col('num_digital_buy_orders') == 0)) | ((col('nb_days_access') == 0) & (col('num_calls') > 0)), 0).otherwise(col("hero")))
    '''

    mob_base_df = mob_base_df.select(app_level, "hero")

    mob_base_df.cache()

    print "[Info] Size of mob_base_df: " + str(mob_base_df.count())

    # 4. Counting the number of services in each segment

    mob_base_df \
        .groupBy("hero") \
        .agg(countDistinct(app_level).alias("num_services")) \
        .show()

    print "[Info] Number of services in each segment above"

    return mob_base_df


def get_segment_profile(spark, base_df, end_date, app_level = 'msisdn'):

    # Getting the KPIs at NIF level
    kpis_df = get_kpis(spark, end_date)

    # List of KPIs computed by the function

    # 'num_services',
    # 'num_fbb_services',
    # 'num_fixed_services',
    # 'num_mobile_services',
    # 'num_tv_services',
    # 'tgs_days_until_f_fin_bi_agg_mean',
    # 'data_usage_mean',
    # 'data_usage_max',
    # 'arpu',
    # 'tnps',
    # 'foc',
    # 'tenure',
    # 'tenure_fbb',
    # 'tenure_fixed',
    # 'tenure_mobile',
    # 'tenure_tv'

    # kpis_df are computed at NIF level; join is based on app_level, but it must be nif_cliente

    base_df = base_df.select(app_level, 'hero').distinct().join(kpis_df, [app_level], 'inner')

    # 1. Tenure

    # 1.1. Global tenure

    tenure_df = base_df.groupBy("hero").agg(sql_avg('tenure').alias("avg_tenure_days"))

    #tenure_df.show()

    #print "[Info] Average tenure above"

    # 1.2. FBB tenure

    tenure_fbb_df = base_df.filter(col("tenure_fbb") > 0).groupBy("hero").agg(sql_avg('tenure_fbb').alias("avg_tenure_fbb_days"))

    #tenure_fbb_df.show()

    #print "[Info] Average tenure_fbb above"

    # 1.3. Fixed tenure

    tenure_fixed_df = base_df.filter(col("tenure_fixed") > 0).groupBy("hero").agg(sql_avg('tenure_fixed').alias("avg_tenure_fixed_days"))

    #tenure_fixed_df.show()

    #print "[Info] Average tenure_fixed above"

    # 1.4. Mobile tenure

    tenure_mobile_df = base_df.filter(col("tenure_mobile") > 0).groupBy("hero").agg(sql_avg('tenure_mobile').alias("avg_tenure_mobile_days"))

    #tenure_mobile_df.show()

    #print "[Info] Average tenure_mobile above"

    # 1.5. TV tenure

    tenure_tv_df = base_df.filter(col("tenure_tv") > 0).groupBy("hero").agg(sql_avg('tenure_tv').alias("avg_tenure_tv_days"))

    #tenure_tv_df.show()

    #print "[Info] Average tenure_tv above"

    # 2. FOC

    foc_df = base_df.groupBy("hero").agg(sql_avg('foc').alias('avg_foc'))

    #foc_df.show()

    #print "[Info] Average foc above"

    # 3. tNPS

    #tnps_df = base_df.groupBy("hero").agg(sql_avg('tnps').alias("avg_tnps_mean_vdn"))

    #tnps_df.show()

    #print "[Info] Average tnps above"

    # 4. ARPU

    arpu_df = base_df.filter(col('arpu') > 0).groupBy("hero").agg(sql_avg('arpu').alias("avg_arpu"))

    #arpu_df.show()

    #print "[Info] Average arpu above"

    # 5. Churn

    churn_df = base_df.groupBy("hero").agg(sql_avg('churn30').alias('churn_30_days'))

    #churn_df.show()

    #print "[Info] Churn rate (30 days) above"

    # 6. Data usage

    # 6.1. Mean usage

    data_usage_mean_df = base_df.groupBy("hero").agg(sql_avg('data_usage_mean').alias('avg_data_usage'))

    #data_usage_mean_df.show()

    #print "[Info] Mean data usage above"

    # 6.2. Max usage

    #data_usage_max_df = base_df.groupBy("hero").agg(sql_avg('data_usage_max').alias('max_data_usage'))

    #data_usage_max_df.show()

    #print "[Info] Max data usage above"

    # 7. TGs

    bound_df = base_df.withColumn("blindaje", when(col('tgs_days_until_f_fin_bi_agg_mean') >= 60, 1.0).otherwise(0.0)).groupBy("hero").agg(sql_avg('blindaje').alias('pcg_bound'))

    #bound_df.show()

    #print "[Info] Bound pcg above"

    # 8. Num services

    num_services_df = base_df.groupBy("hero").agg(sql_avg('num_services').alias('avg_num_services'))

    #num_services_df.show()

    #print "[Info] Num services above"

    # 8.1. Num FBB services

    num_fbb_services_df = base_df.groupBy("hero").agg(sql_avg('num_fbb_services').alias('avg_num_fbb_services'))

    #num_fbb_services_df.show()

    #print "[Info] Num FBB services above"

    # 8.2. Num fixed services

    num_fixed_services_df = base_df.groupBy("hero").agg(sql_avg('num_fixed_services').alias('avg_num_fixed_services'))

    #num_fixed_services_df.show()

    #print "[Info] Num fixed services above"

    # 8.3. Num mobile services

    num_mobile_services_df = base_df.groupBy("hero").agg(sql_avg('num_mobile_services').alias('avg_num_mobile_services'))

    #num_mobile_services_df.show()

    #print "[Info] Num mobile services above"

    # 8.4. Num tv services

    num_tv_services_df = base_df.groupBy("hero").agg(sql_avg('num_tv_services').alias('avg_num_tv_services'))

    #num_tv_services_df.show()

    #print "[Info] Num TV services above"

    profile_df = tenure_df\
        .join(tenure_fbb_df, ['hero'], 'inner') \
        .join(tenure_fixed_df, ['hero'], 'inner') \
        .join(tenure_mobile_df, ['hero'], 'inner') \
        .join(tenure_tv_df, ['hero'], 'inner') \
        .join(foc_df, ['hero'], 'inner')\
        .join(arpu_df, ['hero'], 'inner')\
        .join(churn_df, ['hero'], 'inner')\
        .join(data_usage_mean_df, ['hero'], 'inner') \
        .join(bound_df, ['hero'], 'inner')\
        .join(num_services_df, ['hero'], 'inner') \
        .join(num_fbb_services_df, ['hero'], 'inner') \
        .join(num_fixed_services_df, ['hero'], 'inner') \
        .join(num_mobile_services_df, ['hero'], 'inner') \
        .join(num_tv_services_df, ['hero'], 'inner')

    profile_df.show()

    print "[Info] Profiling showed above"

    return profile_df

def get_labeled_set(spark, segment_df, date_, mode='basic'):

    year_ = date_[0:4]
    month_ = date_[4:6]
    day_ = date_[6:8]

    # Those feats used for the definition of the segments must not be used as input attribute

    from churn_nrt.src.data_utils.ids_utils import get_ids_nif

    ids_nif_df = get_ids_nif(spark, date_)

    ids_nif_cols = ids_nif_df.columns

    ccc_cols = [c for c in ids_nif_cols if (c.startswith("CCC_"))]

    sel_cols = list(set(ids_nif_cols) - set(ccc_cols))

    segment_df = segment_df \
        .filter((col("hero") == 1) | (col("hero") == 0)) \
        .select("nif_cliente", "hero")\
        .join(ids_nif_df.select(sel_cols), ["nif_cliente"], 'inner') \
        .repartition(400)

    '''
    
    rem_feats = ['CCC_num_calls_w4']
    
    noninput_feats = ['msisdn', 'NUM_CLIENTE', 'NIF_CLIENTE', 'Serv_RGU', 'Cust_COD_ESTADO_GENERAL']

    ids_completo = (spark
                    .read
                    .load('/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/year=' + year_ + '/month=' + month_ + '/day=' + day_)
                    .filter(col("serv_rgu") == "mobile"))

    segment_df = segment_df \
        .filter((col("hero") == 1) | (col("hero") == 0)) \
        .join(ids_completo, ["msisdn", "num_cliente", "nif_cliente"], 'inner') \
        .repartition(400)

    # Removing ID feats

    #sel_feats = list(set(sel_feats) - set(noninput_feats)) + ['hero', 'msisdn']

    #segment_df = segment_df.select(sel_feats)
    
    '''

    return segment_df

def get_unlabeled_set(spark, date_, mode='basic'):

    year_ = date_[0:4]
    month_ = date_[4:6]
    day_ = date_[6:8]

    # Those feats used for the definition of the segments must not be used as input attribute

    from churn_nrt.src.data_utils.ids_utils import get_ids_nif

    ids_nif_df = get_ids_nif(spark, date_)

    ids_nif_cols = ids_nif_df.columns

    ccc_cols = [c for c in ids_nif_cols if (c.startswith("CCC_"))]

    sel_cols = list(set(ids_nif_cols) - set(ccc_cols))

    ids_nif_df = ids_nif_df.select(sel_cols)

    '''

    ids_completo = (spark
                    .read
                    .load('/data/udf/vf_es/amdocs_inf_dataset/amdocs_ids_service_level/year=' + year_ + '/month=' + month_ + '/day=' + day_)
                    .filter(col("serv_rgu") == "mobile"))
    
    '''

    return ids_nif_df


def get_model(model_name, label_="label"):
    return {
        'rf': RandomForestClassifier(featuresCol='features', numTrees=500, maxDepth=10, labelCol=label_, seed=1234,
                                     maxBins=32, minInstancesPerNode=50, impurity='gini', featureSubsetStrategy='sqrt',
                                     subsamplingRate=0.7),
        'gbt': GBTClassifier(featuresCol='features', labelCol=label_, maxDepth=5, maxBins=32, minInstancesPerNode=10,
                             minInfoGain=0.0, lossType='logistic', maxIter=200, stepSize=0.1, seed=None,
                             subsamplingRate=0.7),
    }[model_name]


def getOrderedRelevantFeats(model, featCols, pca):
    if (pca.lower() == "t"):
        return {"PCA applied": 0.0}

    else:
        impFeats = model.stages[-1].featureImportances

        feat_and_imp = zip(featCols, impFeats.toArray())
        return sorted(feat_and_imp, key=lambda tup: tup[1], reverse=True)


def get_feats_imp(model, feat_cols, top=None):
    feat_importance = getOrderedRelevantFeats(model, feat_cols, 'f')

    ii = 1
    for fimp in feat_importance:
        print " [Info] [{:>3}] {} : {}".format(ii, fimp[0], fimp[1])
        if not top or (top != None and ii < top):
            ii = ii + 1
        else:
            break

    return feat_importance


if __name__ == "__main__":
    set_paths()

    # start_date = sys.argv[1]
    end_date_tr = sys.argv[1]
    end_date_tt = sys.argv[2]
    model_ = sys.argv[3]

    from churn_nrt.src.utils.date_functions import move_date_n_days

    start_date_tr = move_date_n_days(end_date_tr, -30)

    from churn_nrt.src.utils.spark_session import get_spark_session

    sc, spark, sql_context = get_spark_session("sfid_identification")

    # 1. Applying the definitions: segmentation of the base

    segment_base_df = get_base_segmentation(spark, start_date_tr, end_date_tr, 'nif_cliente')

    segment_base_df.show()

    # 2. Profiling the segments

    profile_df = get_segment_profile(spark, segment_base_df, end_date_tr, "nif_cliente")

    # 3. Modelling

    # 3.1. Training set

    tr_df = get_labeled_set(spark, segment_base_df, end_date_tr)

    # 3.2. Feature selection (list-set to remove repeated columns)

    all_cols = tr_df.columns

    sel_cols = list(set([c for c in all_cols if(sum([t in c for t in get_noninf_features()]) == 0)]))

    tr_df = tr_df.select(sel_cols)

    # Only numeric columns

    all_cols = tr_df.dtypes

    sel_cols = list(set([c[0] for c in all_cols if (c[1] in ["double", "int", "float", "long", "bigint"])]))

    tr_df = tr_df.select(sel_cols)

    # 3.3. removing columns with null or nan values

    from churn_nrt.src.utils.pyspark_utils import count_nans

    null_cols = count_nans(tr_df).keys()

    sel_cols = list(set(sel_cols) - set(null_cols))

    tr_df = tr_df.select(sel_cols)

    # 3.2. No metadata required as all the attributes are numeric

    numeric_columns = list(set(tr_df.columns) - set(['nif_cliente', 'hero']))

    # 3.3. Building the stages of the pipeline

    stages = []

    assembler_inputs = numeric_columns
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    stages += [assembler]

    print("[Info] Starting fit....")

    mymodel = get_model(model_, "hero")
    stages += [mymodel]
    pipeline = Pipeline(stages=stages)
    pipeline_model = pipeline.fit(tr_df)

    feat_importance_list = get_feats_imp(pipeline_model, assembler_inputs)

    # 4. Scoring on test set (unlabeled)

    getScore = udf(lambda prob: float(prob[1]), DoubleType())

    tt_df = get_unlabeled_set(spark, end_date_tt)

    tt_preds_df = pipeline_model\
        .transform(tt_df)\
        .withColumn("model_score", getScore(col("probability")).cast(DoubleType()))

    # 5. Model outputs

    model_output_cols = ["model_name", \
                         "executed_at", \
                         "model_executed_at", \
                         "predict_closing_date", \
                         "msisdn", \
                         "client_id", \
                         "nif", \
                         "model_output", \
                         "scoring", \
                         "prediction", \
                         "extra_info", \
                         "year", \
                         "month", \
                         "day", \
                         "time"]

    import datetime as dt

    executed_at = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    from churn_nrt.src.utils.date_functions import get_next_dow

    partition_date = get_next_dow(3).strftime('%Y%m%d')  # get day of next wednesday

    partition_year = int(partition_date[0:4])

    partition_month = int(partition_date[4:6])

    partition_day = int(partition_date[6:8])

    df_model_scores = (tt_preds_df
                       .withColumn("model_name", lit("digital_hero").cast("string"))
                       .withColumn("executed_at",
                                   from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast("string"))
                       .withColumn("model_executed_at", col("executed_at").cast("string"))
                       .withColumn("client_id", lit(""))
                       .withColumn("msisdn", lit(""))
                       .withColumn("nif", col("nif_cliente").cast("string"))
                       .withColumn("scoring", col("model_score").cast("float"))
                       .withColumn("model_output", lit(""))
                       .withColumn("prediction", lit("").cast("string"))
                       .withColumn("extra_info", lit(""))
                       .withColumn("predict_closing_date", lit(end_date_tt))
                       .withColumn("year", lit(partition_year).cast("integer"))
                       .withColumn("month", lit(partition_month).cast("integer"))
                       .withColumn("day", lit(partition_day).cast("integer"))
                       .withColumn("time", regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer"))
                       .select(*model_output_cols))

    df_model_scores \
        .write \
        .partitionBy('model_name', 'year', 'month', 'day') \
        .mode("append") \
        .format("parquet") \
        .save("/user/jmarcoso/jmarcoso_model_scores/")

    '''
    
    # 3. Modelling

    # 3.1. Training set

    tr_df = get_labeled_set(spark, segment_base_df, end_date_tr)

    # Getting the metadata to identify numerical and categorical features

    from src.main.python.utils.general_functions import get_all_metadata

    non_inf_features = get_noninf_features() + get_no_input_feats()

    final_map, categ_map, numeric_map, date_map = get_all_metadata(end_date_tr)

    print "[Info] Metadata has been read"

    categorical_columns = list(set(categ_map.keys()) - set(non_inf_features))

    gnv_roam_columns = [c for c in numeric_map.keys() if ('GNV_Roam' in c)]

    address_columns = [c for c in numeric_map.keys() if ('address_' in c)]

    numeric_columns = list(set(numeric_map.keys()) - set(non_inf_features).union(set(gnv_roam_columns)).union(set(address_columns)))

    # 3.3. Building the stages of the pipeline

    stages = []

    # for categorical_col in categorical_columns:
    # string_indexer = StringIndexer(inputCol=categorical_col, outputCol=categorical_col + '_index', handleInvalid='keep')
    # encoder = OneHotEncoderEstimator(inputCols=[string_indexer.getOutputCol()], outputCols=[categorical_col + "_class_vec"])
    # stages += [string_indexer, encoder]

    # Only numerical features are used as input
    # (for each categorical attribute, StringIndexer + OneHotEncoder is required, taking too much time;
    # only specific categorical attributes should be used as input)

    assembler_inputs = numeric_columns  # + [c + "_class_vec" for c in categorical_columns]
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    stages += [assembler]

    print("[Info] Starting fit....")

    mymodel = get_model(model_, "hero")
    stages += [mymodel]
    pipeline = Pipeline(stages=stages)
    pipeline_model = pipeline.fit(tr_df)

    feat_importance_list = get_feats_imp(pipeline_model, assembler_inputs)

    # 4. Scoring on test set (unlabeled)

    getScore = udf(lambda prob: float(prob[1]), DoubleType())

    tt_df = get_unlabeled_set(spark, end_date_tt)

    tt_preds_df = pipeline_model.transform(tt_df).withColumn("model_score", getScore(col("probability")).cast(DoubleType()))

    ##########################################################################################
    # 5. Model outputs
    ##########################################################################################

    model_output_cols = ["model_name", \
                         "executed_at", \
                         "model_executed_at", \
                         "predict_closing_date", \
                         "msisdn", \
                         "client_id", \
                         "nif", \
                         "model_output", \
                         "scoring", \
                         "prediction", \
                         "extra_info", \
                         "year", \
                         "month", \
                         "day", \
                         "time"]

    import datetime as dt

    executed_at = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    from churn_nrt.src.utils.date_functions import get_next_dow

    partition_date = get_next_dow(3).strftime('%Y%m%d')  # get day of next wednesday

    partition_year = int(partition_date[0:4])

    partition_month = int(partition_date[4:6])

    partition_day = int(partition_date[6:8])

    df_model_scores = (tt_preds_df
                       .withColumn("model_name", lit("digital_hero").cast("string"))
                       .withColumn("executed_at",
                                   from_unixtime(unix_timestamp(lit(executed_at), "yyyyMMdd_HHmmss")).cast("string"))
                       .withColumn("model_executed_at", col("executed_at").cast("string"))
                       .withColumn("client_id", lit(""))
                       .withColumn("msisdn", col("msisdn").cast("string"))
                       .withColumn("nif", lit(""))
                       .withColumn("scoring", col("model_score").cast("float"))
                       .withColumn("model_output", lit(""))
                       .withColumn("prediction", lit("").cast("string"))
                       .withColumn("extra_info", lit(""))
                       .withColumn("predict_closing_date", lit(end_date_tt))
                       .withColumn("year", lit(partition_year).cast("integer"))
                       .withColumn("month", lit(partition_month).cast("integer"))
                       .withColumn("day", lit(partition_day).cast("integer"))
                       .withColumn("time", regexp_replace(split(col("executed_at"), " ")[1], ":", "").cast("integer"))
                       .select(*model_output_cols))

    df_model_scores \
        .write \
        .partitionBy('model_name', 'year', 'month', 'day') \
        .mode("append") \
        .format("parquet") \
        .save("/user/jmarcoso/jmarcoso_model_scores/")
    
    '''
