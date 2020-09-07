from pyspark.sql.functions import (udf, col, when, concat, lpad)

from pyspark.sql.types import DoubleType, StringType, IntegerType, ArrayType, FloatType

import datetime


reduceCampaignCode = udf(lambda campaignCodeArray: '_'.join(campaignCodeArray.split('_')[2:]), StringType())

# Calculate the current month and year so that campaigns from two months can be read
currentMonth = datetime.datetime.now().month
currentYear = datetime.datetime.now().year


class CampaignHist():
    def __init__(self):
        pass

    def get_most_common_campaign(self, num_month_backwards):
        ordered_campaign_hist_codes = \
            (self.campaign_hist
             .where(col('year') == currentYear)
             .where(col('month') == (currentMonth - num_month_backwards))
             .groupBy(col('CampaignCode'))
             .count()
             .orderBy(col('count').desc())
             .select(col('CampaignCode'))
             .collect()
             )
        print([hist_codes.CampaignCode for hist_codes in ordered_campaign_hist_codes[0:10]])
        return ordered_campaign_hist_codes[0].CampaignCode

    def get_dataframe(self):
        return self.campaign_hist

    def get_sample(self, spark, campaign_code, num_samples):
        return (spark.createDataFrame(self.campaign_hist
                                      .where(col('CampaignCode') == str(campaign_code))
                                      .take(num_samples)))


class NIFContactHist(CampaignHist):
    def __init__(self, spark):

        self.campaign_hist = \
        (spark.read.load("/data/raw/vf_es/campaign/NifContactHist/1.0/parquet/")
         # filter most common fields
         .where(col('campaigncode').like('%PXXX%'))
         .where(~col('canal').like('%NBA%'))
         .where(~col('canal').like('%PO%'))
         .where(~col('canal').like('%PER%'))
         .where(~col('canal').like('%PMG%'))
         .where(~col('canal').like('%PROMPTOS%'))
         .where(~col('canal').like('%Tienda%'))
         .where(col('flag_borrado') == 0)
         # If CampaignCode is AUTOMMES_PXXXC_BTS_XSELL_FUT
         # with this function, only the string BTS_XSELL_FUT is kept
         .withColumn('CampaignType',
                     (reduceCampaignCode(col('CampaignCode')))
                     )
         .withColumn('Grupo',
                     when(col('cellcode').startswith('CU'), 'Universal')
                     .when(col('cellcode').startswith('CC'), 'Control')
                     .otherwise('Target'))
         .withColumn('YearMonth',
                     concat(col('year'),col('month'))
                     )
         .select(col('year'), col('month'), col('day'), col('cif_nif'),
                 col('CampaignCode'), col('CampaignType'), col('Grupo'),
                 col('creatividad'), col('treatmentcode'), col('canal'),
                 col('contactdatetime'), col('YearMonth')
                 )
         )


class NIFResponseHist(CampaignHist):
    def __init__(self, spark):

        self.campaign_hist = \
            ((spark.read.load("/data/raw/vf_es/campaign/NifResponseHist/1.0/parquet/"))
             .where(col('campaigncode').like('%PXXX%'))
             .where(col('flag_borrado') == 0)
             # If CampaignCode is AUTOMMES_PXXXC_BTS_XSELL_FUT
             # with this function, only the string BTS_XSELL_FUT is kept
             .withColumn('CampaignType',
                         (reduceCampaignCode(col('CampaignCode')))
                         )
             .withColumn('Grupo',
                         when(col('cellcode').startswith('CU'), 'Universal')
                         .when(col('cellcode').startswith('CC'), 'Control')
                         .otherwise('Target'))
             .withColumn('YearMonth',
                         concat(col('year'), lpad(col('month'), 2, '0'))
                         )
             .select(col('year'), col('month'), col('day'), col('cif_nif'),
                     col('CampaignCode'), col('CampaignType'), col('Grupo'),
                     col('creatividad'), col('treatmentcode'), col('canal'),
                     col('responsedatetime'), col('YearMonth')
                     )
             )


# # UDFs
# def create_label_set(label_set):
#     # First, prioritize positive responses
#     if 'Target_Positive' in label_set:
#         return 'Target_Positive'
#     elif 'Control_Positive' in label_set:
#         return 'Control_Positive'
#     # Then, negative responses
#     elif 'Target_Negative' in label_set:
#         return 'Target_Negative'
#     elif 'Control_Negative' in label_set:
#         return 'Control_Negative'
#     else:
#         return 'Ignore'
#
#
# reduceLabelSet = udf(create_label_set, StringType())
