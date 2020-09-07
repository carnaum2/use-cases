# PRE-REQUISITE: Set TMPDIR env variable in ~/.bash_profile to a writeable directory with enough free space before starting the R session
# #spark2-submit $SPARK_COMMON_OPTS --conf spark.port.maxRetries=500 --conf spark.driver.memory=16g --conf spark.executor.memory=16g amdocs_car_main.py --starting-day 20180401 --closing-day 20180430
# #spark2-submit $SPARK_COMMON_OPTS --conf spark.port.maxRetries=500 --conf spark.driver.memory=16g --conf spark.executor.memory=16g amdocs_car_main.py --starting-day 20180601 --closing-day 20180626
# #spark2-submit $SPARK_COMMON_OPTS --conf spark.port.maxRetries=500 --conf spark.driver.memory=16g --conf spark.executor.memory=16g amdocs_car_main.py --starting-day 20180701 --closing-day 20180723
# #spark2-submit $SPARK_COMMON_OPTS --executor-memory 4G --driver-memory 4G --conf spark.driver.maxResultSize=4G --conf spark.yarn.executor.memoryOverhead=4G ~/fy17.capsule/convergence/src/main/python/DP_prepare_input_rbl_ids_srv.py --debug 201805 201807
# spark2-submit $SPARK_COMMON_OPTS --executor-memory 4G --driver-memory 4G --conf spark.driver.maxResultSize=4G --conf spark.yarn.executor.memoryOverhead=4G /var/SP/data/home/bbergua/fy17.capsule/convergence/src/main/python/DP_prepare_input_amdocs_ids.py --debug 201805 201807
# spark2-submit $SPARK_COMMON_OPTS --executor-memory 4G --driver-memory 4G --conf spark.driver.maxResultSize=4G --conf spark.yarn.executor.memoryOverhead=4G /var/SP/data/home/bbergua/fy17.capsule/convergence/src/main/python/DP_prepare_input_amdocs_ids.py --debug 201806 201808
# spark2-submit $SPARK_COMMON_OPTS --executor-memory 6G --driver-memory 6G --conf spark.driver.maxResultSize=6G ~/fy17.capsule/convergence/src/main/python/convergence.py --only-data 201805 201807 2>&1 | tee salida.convergence
# spark2-submit $SPARK_COMMON_OPTS --executor-memory 6G --driver-memory 6G --conf spark.driver.maxResultSize=6G ~/fy17.capsule/convergence/src/main/python/convergence.py --only-data 201806 201808 2>&1 | tee salida.convergence
# hdfs dfs -get /tmp/bbergua/convergence_data/train_data/csv/201806/part-00000-*.csv train_data_csv_201806.csv
# hdfs dfs -get /tmp/bbergua/convergence_data/predict_data/csv/201808/part-00000-*.csv predict_data_csv_201808.csv
# scp vgddp387hr:/var/SP/data/home/bbergua/train_data_csv_201806.csv .
# scp vgddp387hr:/var/SP/data/home/bbergua/predict_data_csv_201808.csv .

library(data.table)
library(h2o)

#h2o.shutdown()
localH2O = h2o.init()#nthreads = -1, max_mem_size = "16G")#, ice_root=Sys.getenv('TMPDIR'), log_dir=Sys.getenv('TMPDIR'))
h2o.removeAll()

model_month <- '201806'
preds_day <- '20180820'
preds_month <- substr(preds_day, 1, 6)

#dt.train <- fread('/var/SP/data/projects/vf_es/bbergua/train_data_csv_201803.csv', stringsAsFactors = TRUE)
dt.train <- fread(paste0('/var/SP/data/projects/vf_es/bbergua/train_data_csv_', model_month, '.csv'), stringsAsFactors = TRUE)
dt.train[, class := as.factor(class)]
dt.train[, pred := as.factor(pred)]

# # string columns
# dt.train[, nif := NULL]
# dt.train[, MSISDN := NULL]
# dt.train[, NOMBRE := NULL]
# dt.train[, PRIM_APELLIDO := NULL]
# dt.train[, SEG_APELLIDO := NULL]
# dt.train[, DIR_LINEA1 := NULL]
# dt.train[, DIR_LINEA2 := NULL]
# dt.train[, DIR_LINEA3 := NULL]
# dt.train[, NOM_COMPLETO := NULL]
# dt.train[, DIR_FACTURA1 := NULL]
# dt.train[, DIR_FACTURA2 := NULL]
# dt.train[, DIR_FACTURA3 := NULL]
# dt.train[, DIR_FACTURA4 := NULL]
# #dt.train[, CODIGO_POSTAL := NULL]
# dt.train[, NOMBRE_CLI_FACT := NULL]
# dt.train[, APELLIDO1_CLI_FACT := NULL]
# dt.train[, APELLIDO2_CLI_FACT := NULL]
# dt.train[, CTA_CORREO_CONTACTO := NULL]
# dt.train[, CTA_CORREO := NULL]
# dt.train[, NIF_FACTURACION := NULL]
# dt.train[, x_id_gescal := NULL]
# 
# # numeric columns
# dt.train[, DIR_NUM_DIRECCION := NULL]
# dt.train[, NUM_CLIENTE := NULL]
# dt.train[, Instancia_P := NULL]
# dt.train[, OBJID := NULL]
# dt.train[, IMSI := NULL]
# 
# dt.train[, CAMPO1 := NULL] # NUM_CLIENTE deanon. Based on deanonymized objid
# dt.train[, CAMPO2 := NULL] # MSISDN deanon. Based on deanonymized objid
# dt.train[, CAMPO3 := NULL] # OBJID deanon.
id_cols <- c(
  # string columns
  'nif'
  , 'NIF_CLIENTE'
  , 'MSISDN'
  , 'NOMBRE'
  , 'PRIM_APELLIDO'
  , 'SEG_APELLIDO'
  , 'DIR_LINEA1'
  , 'DIR_LINEA2'
  , 'DIR_LINEA3'
  , 'NOM_COMPLETO'
  , 'DIR_FACTURA1'
  , 'DIR_FACTURA2'
  , 'DIR_FACTURA3'
  , 'DIR_FACTURA4'
  #, 'CODIGO_POSTAL'
  , 'NOMBRE_CLI_FACT'
  , 'APELLIDO1_CLI_FACT'
  , 'APELLIDO2_CLI_FACT'
  , 'CTA_CORREO_CONTACTO'
  , 'CTA_CORREO'
  , 'NIF_FACTURACION'
  , 'x_id_gescal'
  
  # numeric columns
  , 'DIR_NUM_DIRECCION'
  , 'NUM_CLIENTE'
  , 'Instancia_P'
  , 'OBJID'
  , 'IMSI'
  
  , 'CAMPO1' # NUM_CLIENTE deanon. Based on deanonymized objid
  , 'CAMPO2' # MSISDN deanon. Based on deanonymized objid
  , 'CAMPO3' # OBJID deanon.
)
id_cols <- c(id_cols, unlist(lapply(id_cols, tolower)))

# Fechas -> Años
#last.day <- strptime('20180331', "%Y%m%d")
first.day.next.month <- seq.POSIXt(strptime(paste0(model_month, '01'), "%Y%m%d"), length=2, by="+1 months")[2]
last.day.model.month <- seq.POSIXt(first.day.next.month, length=2, by="-1 day")[2]

dt.train[, birth_date := as.POSIXct(strptime(birth_date, "%Y-%m-%d"))]
dt.train[, EDAD := as.numeric(last.day.model.month - birth_date)/365]
dt.train[, birth_date := NULL]

dt.train[, FECHA_MIGRACION := as.POSIXct(strptime(FECHA_MIGRACION, "%Y-%m-%d"))]
dt.train[, ANYOS_MIGRACION := as.numeric(last.day.model.month - FECHA_MIGRACION)/365]
dt.train[, FECHA_MIGRACION := NULL]

if (FALSE) {
  ###
  # Dates fields
  fx_cols <- c()
  for (col in colnames(dt.train)) {
    if ((length(grep('fecha',  col, ignore.case = TRUE)) > 0) |
        (length(grep('date',   col, ignore.case = TRUE)) > 0) |
        (length(grep('fx',     col, ignore.case = TRUE)) > 0) | 
        (length(grep('tacada', col, ignore.case = TRUE)) > 0))
      fx_cols <- c(fx_cols, col)
  }
  fx_cols
  fx_cols_str <- paste0("['", paste0(fx_cols, collapse = "', '"), "']")
  View(head(dt.train[, ..fx_cols], 100))
  ###
  cols <- c()
  for (col in colnames(dt.train)) {
    if ((! col %in% fx_cols) & !(startsWith(col, "GNV_") | startsWith(col, "Camp_") | startsWith(col, "Bill_") | startsWith(col, "ccc_")))
      cols <- c(cols, col)
  }
  View(head(dt.train[, ..cols], 20))
  ###
  for (col in cols) {
    if (! col %in% c('CODIGO_POSTAL', 'CIUDAD', 'PROVINCIA'))
      cat(col, length(levels(dt.train[[col]])), '\n')
  }
  ###
  #Encoding(levels(dt.train$TRATAMIENTO)) <- 'latin1'
  Encoding(levels(dt.train$TRATAMIENTO)) <- 'UTF-8'
  #Encoding(levels(dt.train$TRATAMIENTO)) <- 'unknown'
  levels(dt.train$TRATAMIENTO)
  ###
}

#summary(dt.train)
summary(dt.train[, class])

factors <- data.frame(variable=character(), levels=numeric())
#nc <- 0
for (col in names(dt.train)) {
  if (class(dt.train[[col]]) == 'factor') {
    n <- length(levels(dt.train[[col]]))
    cat(col, ' ', n, '\n')
    factors <- rbind(factors, data.frame(variable=col, levels=n))
  }
  #nc <- nc + 1
}
#table(dt.train$GNV_hour_2_WE_Music_Pass_Data_Volume_MB)

dt.train.class1 <- dt.train[class == 'true']
dt.train.class0 <- dt.train[class == 'false']
sum(dt.train.class0$tv_services > 0)
#dt.train.class0 <- dt.train[class == 'false' & tv_services == 0]
dt.train.class0.balanced <- dt.train.class0[sample(1:nrow(dt.train.class0), nrow(dt.train.class1))]
dt.train.balanced <- rbind(dt.train.class1, dt.train.class0.balanced)
#summary(dt.train.balanced)
summary(dt.train.balanced[, class])

#h2o.removeAll()

nrow(dt.train.balanced)
h2o.ls()
# h2o.rm("train.hex")
train.hex <- as.h2o(dt.train.balanced, "train.hex")
dim(train.hex)

# Split the data into Train/Test/Validation with Train having 70% and test and validation 15% each
#train,test,valid = train.hex.split_frame(ratios=[.7, .15])
train.hex.split <- h2o.splitFrame(train.hex, ratios = 0.8, seed = -1)
train.h2o <- train.hex.split[[1]]
test.h2o <- train.hex.split[[2]]
h2o.ls()

#types(train.hex["class"])
#train.hex["class"] = as.factor(train.hex["class"])
y <- 'class'
x <- setdiff(colnames(dt.train.balanced), c(y, id_cols))

h2o.rf <- h2o.randomForest(x = x, 
                           y = y, 
                           training_frame = train.h2o
                           #, leaderboard_frame = test.h2o
                           , ntrees = 500 #trees
                           , seed = 1234
                           #, model_id = "h2orf"
                           #, max_runtime_secs = 60
                           , binomial_double_trees = T
                           )
print(h2o.rf)
#par(mfrow=c(1,1))
#h2o.varimp_plot(h2o.rf, num_of_features = 30)
#plot(h2o.rf, main = paste0("Error evolution for model month"))
varimp_all <- as.data.frame(h2o.varimp(h2o.rf))

if (FALSE) {
  #c <- 'GNV_hour_2_WE_Music_Pass_Data_Volume_MB'
  cat('Calculating count distinct of every variable', nrow(varimp_all))
  varimp_all['num_distinct'] = 0
  i <- 0
  for (c in varimp_all$variable) {
    if (i %% trunc(nrow(varimp_all)/100) == 0)
      cat (paste0(trunc(i*100/nrow(varimp_all)), '%'), ' ')
    n <- length(unique(dt.train[! is.na(dt.train[[c]])][[c]]))
    varimp_all[varimp_all$variable == c, "num_distinct"] = n
    i <- i + 1
  }
  varimp_all_bak <- varimp_all
  #unique(dt.train$FACTURA_ELECTRONICA)
  #unique(dt.train$Camp_SRV_Retention_Voice_SAT_Target_0)
  #unique(dt.train$ccc_Churn_Cancellations_Collections_process)
  #unique(dt.train$)
  
  print(varimp_all[varimp_all$relative_importance > 0, ])
  #print(varimp_all[varimp_all$scaled_importance > 0, ])
  #print(varimp_all[varimp_all$percentage > 0, ])
  varimp_str <- paste0("['", paste0(varimp_all[varimp_all$relative_importance > 0, 'variable'], collapse = "', '"), "']")
  #varimp_str <- paste0("['", paste0(varimp_all[varimp_all$scaled_importance > 0, 'variable'], collapse = "', '"), "']")
  #varimp_str <- paste0("['", paste0(varimp_all[varimp_all$percentage > 0, 'variable'], collapse = "', '"), "']")
  #varimp_str <- paste0("['", paste0(varimp_all[1:400, 'variable'], collapse = "', '"), "']")
}

perf <- h2o.performance(h2o.rf, newdata = test.h2o)
print(perf)
print(h2o.gainsLift(h2o.rf, newdata = test.h2o))
#print(h2o.gainsLift(perf))
par(mfrow=c(1,1))
plot(perf)
# Manually plot ROC curve
# plot(h2o.fpr(perf)$fpr, h2o.tpr(perf)$tpr, main='True Positive Rate vs False Positive Rate', xlab='False Positive Rate', ylab='True Positive Rate')
# abline(a=0, b=1, lty=2)

ofolder <- file.path('/var/SP/data/projects/vf_es/bbergua/models', model_month)
if (!file.exists(ofolder)) {
  cat("[INFO] Creating Folder\n")
  dir.create(ofolder, recursive = T)
}
ofile <- file.path(ofolder, "rf.model.h2o")
cat("[SAVE] ", ofile, "\n")
model_path <- h2o.saveModel(h2o.rf, path = ofile, force = T)
cat("[SAVE] Model saved in", model_path, "\n")
#h2o.rf <- h2o.loadModel(file.path(ofile, 'DRF_model_R_1527491062514_49'))  # Con este modelo salieron las predicciones para Junio 2018
#h2o.rf <- h2o.loadModel(file.path(ofile, 'DRF_model_R_1527764962812_1'))
#h2o.rf <- h2o.loadModel(file.path(ofile, 'DRF_model_R_1530085127382_1'))   # Con este modelo salieron las predicciones para Julio 2018
#h2o.rf <- h2o.loadModel(file.path(ofile, 'DRF_model_R_1532342821898_226')) # Con este modelo salieron las predicciones para Agosto 2018
#h2o.rf <- h2o.loadModel(file.path(ofile, 'DRF_model_R_1534838197938_1')) # Con este modelo salieron las predicciones para Septiembre 2018

if (FALSE) {
  aml <- h2o.automl(training_frame = train.h2o
                    , leaderboard_frame = test.h2o
                    , x = x
                    , y = y
                    , seed = 1234
                    , max_runtime_secs = 60)
  # View the AutoML Leaderboard
  lb <- aml@leaderboard
  lb
  
  # The leader model is stored here
  aml@leader
  
  # If you need to generate predictions on a test set, you can make
  # predictions directly on the `"H2OAutoML"` object, or on the leader
  # model object directly
  
  #pred <- h2o.predict(aml, test)  #Not functional yet: https://0xdata.atlassian.net/browse/PUBDEV-4428
  
  # or:
  pred <- h2o.predict(aml@leader, test)
}

###########
# Predict #
###########

#dt.pred <- fread('/var/SP/data/projects/vf_es/bbergua/predict_data_csv_201805.csv')
dt.pred <- fread(paste0('/var/SP/data/projects/vf_es/bbergua/predict_data_csv_', preds_month, '.csv'))
nrow(dt.pred)
h2o.rm("pred.hex")
pred.hex <- as.h2o(dt.pred, "pred.hex")
dim(pred.hex)
h2o.ls()
preds <- h2o.predict(h2o.rf, pred.hex)
preds.dt <- as.data.table(preds)
nrow(preds.dt)
preds.dt[, nif := dt.pred$nif]
preds.dt[, CAMPO1 := dt.pred$CAMPO1]
nrow(preds.dt)

names(preds.dt)
preds.new <- copy(preds.dt)
preds.new[, predict := NULL]
setnames(preds.new, c("SCORE_NO_PROPENSO_CONV", "SCORE_PROPENSO_CONV", "nif", "CAMPO1"))
setcolorder(preds.new, c("nif", "CAMPO1", "SCORE_PROPENSO_CONV", "SCORE_NO_PROPENSO_CONV"))
#setkey(preds.new, SCORE_NO_PROPENSO_CONV)
preds.new <- preds.new[order(-SCORE_PROPENSO_CONV)]
preds.new[, SCORE_NO_PROPENSO_CONV := NULL]
names(preds.new)

#ofolder <- file.path('/var/SP/data/projects/vf_es/bbergua/predictions/201805')
ofolder <- file.path('/var/SP/data/projects/vf_es/bbergua/predictions', preds_month)
if (!file.exists(ofolder)) {
  cat(paste0("[INFO] Creating Folder ", ofolder, "\n"))
  dir.create(ofolder, recursive = T)
}

#ofile <- file.path(ofolder, paste0("convergence.predictions.amdocs.20180528.RData"))
ofile <- file.path(ofolder, paste0("convergence.predictions.amdocs.", preds_day, ".RData"))
cat("[SAVE]", ofile, "\n")
save(preds.new, file = ofile)

preds.new.csv <- copy(preds.new)
preds.new.csv[, SCORE_PROPENSO_CONV := round(SCORE_PROPENSO_CONV, digits = 2)]
###
preds.new.csv[, nif := NULL]
dim(preds.new.csv)
preds.new.csv <- preds.new.csv[! is.na(CAMPO1)]
dim(preds.new.csv)
names(preds.new.csv)
setnames(preds.new.csv, c("NUM_CLIENTE_INT", "SCORE_PROPENSO_CONV"))
names(preds.new.csv)
###
#ofile <- file.path(ofolder, paste0("convergence.predictions.amdocs.20180528.csv"))
ofile <- file.path(ofolder, paste0("convergence.predictions.amdocs.", preds_day, ".csv"))
cat("[SAVE]", ofile, "\n")
write.table(preds.new.csv, file = ofile, sep = "|", quote = F, row.names = F)


####################

hist(preds.dt$false)
hist(preds.dt$true)

####################

# Comparativa scorings Oracle vs Amdocs

preds.amd <- fread(ofile) # Anonymized
preds.ora <- fread('/var/SP/data/projects/vf_es/bbergua/fy17/predictions/201804/convergence.predictions.20180423.csv')



#h2o.shutdown()
