from Metadata import *
from feat_functions import *

def getBalancedSet2(spark, df, key, num, n_muestras = 150000):
    from pyspark.sql.functions import col
    from pyspark.sql.functions import rand
    import numpy as np
    schema = df.schema
    class_1 = df.filter(col(key) == 1.0)
    class_2 = df.filter(col(key) == 0.0)
    c = class_1.agg({"censor":"sum"}).collect()[0]
    c1_1 = c["sum(censor)"]
    c1_2 = n_muestras
    c1 = np.min([c1_1,c1_2])
    c2 = class_2.count()
    if c1 == c2:
        return df
    m = np.min([c1,c2])
    print(m)
    sampleRatio =float(m)/(num)
    print("SampleRatio",sampleRatio)
    if c1<c2:
        print(int(c1))
        sampled = class_2.take(int(c1))
        sampled_1 = class_1.take(int(c1))
        
        sampled = spark.createDataFrame(sampled,schema)
        sampled_1 = spark.createDataFrame(sampled_1,schema)
        
        balanced = sampled.union(sampled_1)
    else:
        print(int(c2))
        sampled = class_1.take(int(c2))
        sampled_2 = class_2.take(int(c2))
        
        sampled = spark.createDataFrame(sampled,schema)
        sampled_2 = spark.createDataFrame(sampled_2,schema)
        
        balanced = sampled.union(sampled_2)
        
    balanced_f = balanced.orderBy(rand())
    return balanced_f

def getInputFeats(df, mode, cat):
    rem1 = ['nif_cliente', "fecha_migracion",
    "bam_fx_first",
    "bam-movil_fx_first",
    "fbb_fx_first",
    "fixed_fx_first",
    "movil_fx_first",
    "prepaid_fx_first",
    "tv_fx_first",
    "fx_srv_basic",
    "fx_tariff",
    "fx_voice_tariff",
    "fx_data",
    "fx_dto_lev1",
    "fx_dto_lev2",
    "fx_data_additional",
    "fx_roam_zona_2", 'ref_date', 'msisdn_a_port','censor']
    
    delete_cols, cte_cat = getNonInformativeFeatures(df, mode, getNoInputFeats())
    
    rem_feats = rem1 + getIdFeats() + getNoInputFeats() + getCatFeats()  + delete_cols + ["label"]
    cat_cols = Diff(getCatFeats(), cte_cat)
    feats_cat = []
    print(cat)
    if (cat):
        print(cat_cols)
        for c in cat_cols:
            feats_cat.append(c + "_enc")   
    featCols = Diff(df.columns, rem_feats) + feats_cat
    return featCols, cat_cols

def getNonInformativeFeatures(dt, mode, noinputFeats):
    if not mode:
        return(([],[]))
    elif mode == "none":
        return(([], []))
    elif mode == "zerovar":
        return(get_constant_columns(dt))
    elif mode == "correlation":
        return(loadCorrelatedFeatures())
    elif mode == "zerovarandcorr":
        return(get_constant_columns(dt)+loadCorrelatedFeatures())

def getNumerical(df):
    numeric = ["bigint","double","int", "long"]
    columnList = []
    for n in numeric:
        columnList.append([item[0] for item in df.dtypes if item[1].startswith(n)])
    return(flatten(columnList))

def get_constant_columns(df):
    cte_cat = []
    df.cache()
    cols = getNumerical(df)
    cte_num = get_constant_numerical(df, cols)
    cat_cols = getCatFeats()
    for c in cat_cols:
        first_val = df.select(c).first()[0]
        if (df.filter(col(c) != first_val).count() == 0):
            cte_cat.append(c)
            print(c)
    df.unpersist()
    delete_cols = cte_num + cte_cat
    return delete_cols, cte_cat
           
def get_constant_numerical(df, feats):
    
   # Create a function with two arguments, listDiv and n (number of divisions):
    def divLista(listDiv, n):
       # For item i in a range that is a length of listDiv,
        for i in range(0, len(listDiv), n):
           # Create an index range for listDiv of n items:
            yield listDiv[i:i + n]

    numerical_feats_group = list(divLista(feats, 200))

    cte_vars_total = []
    for nn, feats in enumerate(numerical_feats_group):
        print "[Info FbbChurn] " + str(nn + 1) + "/" + str(len(numerical_feats_group)) + " Features group that is being processed at this moment"
        from pyspark.sql.functions import variance
        vars_list = [variance(f).alias(f) for f in feats]
        f_vars = df.select(vars_list).rdd.collect()[0].asDict()
        cte_vars = [f for f in list(f_vars.keys()) if f_vars[f] == 0]

        cte_vars_total.extend(cte_vars)
       #print(len(cte_vars_total))
    return cte_vars_total

def getPipelineNC(featCols, categoricals, pca, cat):
    from pyspark.ml.feature import  OneHotEncoder, PCA, StandardScaler, StringIndexer, VectorAssembler #, VectorIndexer
    from pyspark.ml import Pipeline
    inputDim = len(featCols)
############# ENCODERS E INDEXERS #############
    if not (cat):
        indexAndEncod = []
    else:
        indexAndEncod = []
        print(categoricals)
        for c in categoricals:
            out = c + "_idx"
            out_enc = c + "_enc"
            indexAndEncod.append(StringIndexer(inputCol=c, outputCol=out))
            indexAndEncod.append(OneHotEncoder(inputCol=out,outputCol=out_enc))
############# ASSEMBLER #############
    assembler = VectorAssembler(inputCols=featCols, outputCol="features")
############# PCA #############
    numpca = int(0.75*inputDim)
    pcaStage = PCA(inputCol="features", outputCol="pcafeatures")
    inputCol = "pcafeatures" if pca else "features"
############# PIPELINE #############
    if cat and pca:
        pipeline = Pipeline(stages=[assembler, pcaStage])
        print("[PipelineGenerator] Pipeline without categorical feats and PCA")  	 #HECHO
    elif not cat and pca:
        pipeStages = indexAndEncod
        pipeStages.append(assembler)
        pipeStages.append(pcaStage)
        pipeline = Pipeline(stages=pipeStages)
        print("[PipelineGenerator] Pipeline with categorical feats and PCA") 	 	 
    elif not cat and not pca:
        pipeline = assembler
        print("[PipelineGenerator] Pipeline without categorical feats and PCA")  	 #HECHO
    elif cat and not pca:
        pipeStages = indexAndEncod
        pipeStages.append(assembler)
        pipeline = Pipeline(stages=pipeStages)
        print("[PipelineGenerator] Pipeline with categorical feats but without PCA") 
    return(pipeline)


def getIds(spark, ClosingDay):
    import datetime
    ClosingDay_date=datetime.date(int(ClosingDay[:4]),int(ClosingDay[4:6]),int(ClosingDay[6:8 ]))
    hdfs_partition_path = 'year=' + str(int(ClosingDay[:4])) + '/month=' + str(int(ClosingDay[4:6])) + '/day=' + str(int(ClosingDay[6:8]))

    # BASIC PATH:
    #- Old Version (without data preparation):
    hdfs_write_path_common = '/data/udf/vf_es/amdocs_ids/'
    #- New Version (with data preparation):
    #hdfs_write_path_common = '/data/udf/vf_es/amdocs_inf_dataset/'

    path_customer = hdfs_write_path_common +'customer/'+hdfs_partition_path
    path_service = hdfs_write_path_common +'service/'+hdfs_partition_path
    path_customer_agg = hdfs_write_path_common +'customer_agg/'+hdfs_partition_path
    path_voiceusage = hdfs_write_path_common +'usage_geneva_voice/'+hdfs_partition_path
    path_datausage = hdfs_write_path_common +'usage_geneva_data/'+hdfs_partition_path
    path_billing = hdfs_write_path_common +'billing/'+hdfs_partition_path
    path_campaignscustomer = hdfs_write_path_common +'campaigns_customer/'+hdfs_partition_path
    path_campaignsservice = hdfs_write_path_common +'campaigns_service/'+hdfs_partition_path
    #path_roamvoice = hdfs_write_path_common +'usage_geneva_roam_voice/'+hdfs_partition_path !!JOIN WITH IMSI
    path_roamdata = hdfs_write_path_common +'usage_geneva_roam_data/'+hdfs_partition_path
    path_orders_hist = hdfs_write_path_common +'orders/'+hdfs_partition_path
    path_orders_agg = hdfs_write_path_common +'orders_agg/'+hdfs_partition_path
    path_penal_cust = hdfs_write_path_common +'penalties_customer/'+hdfs_partition_path
    path_penal_srv = hdfs_write_path_common +'penalties_service/'+hdfs_partition_path
    path_devices = hdfs_write_path_common +'device_catalogue/'+hdfs_partition_path
    path_ccc = hdfs_write_path_common +'call_centre_calls/'+hdfs_partition_path
    path_tnps = hdfs_write_path_common +'tnps/'+hdfs_partition_path
    path_perms_and_prefs = hdfs_write_path_common +'perms_and_prefs/'+hdfs_partition_path

    # Load HDFS files
    customerDF_load = (spark.read.load(path_customer))
    serviceDF_load = (spark.read.load(path_service))
    customerAggDF_load = (spark.read.load(path_customer_agg))
    voiceUsageDF_load = (spark.read.load(path_voiceusage))
    dataUsageDF_load = (spark.read.load(path_datausage))
    billingDF_load = (spark.read.load(path_billing))
    customerCampaignsDF_load = (spark.read.load(path_campaignscustomer))
    serviceCampaignsDF_load = (spark.read.load(path_campaignsservice))
    #RoamVoiceUsageDF_load = (spark.read.load(path_roamvoice))
    RoamDataUsageDF_load = (spark.read.load(path_roamdata))
    customer_orders_hist_load = (spark.read.load(path_orders_hist))
    customer_orders_agg_load = (spark.read.load(path_orders_agg))
    penalties_cust_level_df_load = (spark.read.load(path_penal_cust))
    penalties_serv_level_df_load = (spark.read.load(path_penal_srv))
    devices_srv_df_load = (spark.read.load(path_devices))
    df_ccc_load = (spark.read.load(path_ccc))
    df_tnps_load = (spark.read.load(path_tnps))
    df_perms_and_prefs=(spark.read.load(path_perms_and_prefs))

    # JOIN
    df_amdocs_ids_service_level=(customerDF_load
        .join(serviceDF_load, 'NUM_CLIENTE', 'inner')
        .join(customerAggDF_load, 'NUM_CLIENTE', 'inner')
        .join(voiceUsageDF_load, (col('msisdn') == col('id_msisdn')), 'leftouter')
        .join(dataUsageDF_load, (col('msisdn')==col('id_msisdn_data')), 'leftouter')
        .join(billingDF_load, col('customeraccount') == customerDF_load.NUM_CLIENTE, 'leftouter')
        .join(customerCampaignsDF_load, col('nif_cliente')==col('cif_nif'), 'leftouter')
        .join(serviceCampaignsDF_load, 'msisdn', 'leftouter')
        #.join(RoamVoiceUsageDF_load, (col('msisdn')==col('id_msisdn_voice_roam')), 'leftouter') NEEDED IMSI FOR JOIN!!!!!!
        .join(RoamDataUsageDF_load, (col('msisdn')==col('id_msisdn_data_roam')), 'leftouter')
        .join(customer_orders_hist_load, 'NUM_CLIENTE', 'leftouter')
        .join(customer_orders_agg_load, 'NUM_CLIENTE', 'leftouter')
        .join(penalties_cust_level_df_load,'NUM_CLIENTE','leftouter')
        .join(penalties_serv_level_df_load, ['NUM_CLIENTE','Instancia_P'], 'leftouter')
        .join(devices_srv_df_load, 'msisdn','leftouter')
        .join(df_ccc_load, 'msisdn','leftouter')
        .join(df_tnps_load, 'msisdn','leftouter')
        .join(df_perms_and_prefs, customerDF_load.NUM_CLIENTE==df_perms_and_prefs.CUSTOMER_ID,'leftouter')
        .drop(*['id_msisdn_data', 'id_msisdn', 'cif_nif', 'customeraccount','id_msisdn_data_roam','id_msisdn_voice_roam','rowNum'])
        .withColumn('ClosingDay',lit(ClosingDay))
        )
    
    return df_amdocs_ids_service_level