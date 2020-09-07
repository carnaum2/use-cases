source("configuration.R")
suppressMessages(library(caret))
suppressMessages(library(doMC))
suppressMessages(library(xgboost))

getConvergidosPrevMonth <- function(month) {
  ifile <- file.path(data.folder, "Campaign", month, paste0("dt.Campaigns.", month, ".RData"))
  cat("[LOAD] ", ifile, "\n")
  load(ifile) # FIXME
  
  setnames(dt.Campaigns, "CIF_NIF", "x_num_ident")
  setkey(dt.Campaigns, x_num_ident)
  
  ifile <- file.path(datasets.folder, month, paste0("dt.Convergidos-train-", month, ".RData"))
  cat("[LOAD] ", ifile, "\n")
  load(ifile) # FIXME
  
  dt.Convergidos[, CLASS := 1]
  dt.Convergidos <- dt.Convergidos[, !c("SUM_VOZ_M1", "SUM_ADSL_M1", "SUM_HZ_M1", "SUM_FTTH_M1", 
                                        "SUM_LPD_M1", "SUM_TIVO_M1", "SUM_FLAGFUTBOL_M1"), with = F]
  
  setkey(dt.Convergidos, x_num_ident)
  dt.Convergidos <- dt.Campaigns[dt.Convergidos]
  nombres <- names(dt.Campaigns)
  nombres <- nombres[!nombres %in% "x_num_ident" ]
  dt.Convergidos[is.na(number_of_contacts), nombres := -1, with = F]
  
  return(dt.Convergidos)
}

cleanNA <- function(dt.data) {
  dt.data[is.na(IND_PROP_ADSL_FIBRA), IND_PROP_ADSL_FIBRA := -1]
  dt.data[is.na(IND_PROP_HZ), IND_PROP_HZ := -1]
  dt.data[is.na(IND_PROP_TABLET), IND_PROP_TABLET := -1]
  dt.data[is.na(IND_PROP_LPD), IND_PROP_LPD := -1]
  dt.data[is.na(IND_PROP_UP_VOZ_MONO), IND_PROP_UP_VOZ_MONO := -1]
  dt.data[is.na(IND_PROP_UP_VOZ_MULTI), IND_PROP_UP_VOZ_MULTI := -1]
  dt.data[is.na(IND_MUY_PROP_ADSL_FIBRA), IND_MUY_PROP_ADSL_FIBRA := -1]
  
  dt.data[is.na(IND_MUY_PROP_HZ), IND_MUY_PROP_HZ := -1]
  dt.data[is.na(IND_MUY_PROP_TABLET), IND_MUY_PROP_TABLET := -1]
  dt.data[is.na(IND_MUY_PROP_LPD), IND_MUY_PROP_LPD := -1]
  dt.data[is.na(IND_MUY_PROP_UP_VOZ_MONO), IND_MUY_PROP_UP_VOZ_MONO := -1]
  dt.data[is.na(IND_MUY_PROP_UP_VOZ_MULTI), IND_MUY_PROP_UP_VOZ_MULTI := -1]
  dt.data[is.na(IND_PROP_TV), IND_PROP_TV := -1]
  dt.data[is.na(IND_MUY_PROP_TV), IND_MUY_PROP_TV := -1]
  dt.data[is.na(IND_PROP_AOU), IND_PROP_AOU := -1]
  dt.data[is.na(IND_MUY_PROP_AOU), IND_MUY_PROP_AOU := -1]
}

generateTrain <- function(month, type, dt.Convergidos = NULL, dt.NoConvergidos = NULL) {
  prev.month <- paste0(month, "01")
  prev.month <- as.POSIXct(strptime(prev.month, "%Y%m%d"))
  prev.month <- prev.month - 2
  prev.month <- as.character(prev.month)
  prev.month <- substr(prev.month, 1, 7)
  prev.month <- gsub("-", "", prev.month)
  
  ifile <- file.path(datasets.folder, month, paste0("dt.Convergidos-train-", month, ".RData"))
  if (is.null(dt.Convergidos)) {
    cat("[LOAD] ", ifile, "\n")
    load(ifile)
  } else {
    cat("[INFO] Skipping", ifile, ", already loaded\n")
  }

  dt.Convergidos[, CLASS := 1]
  if (type == "mono") {
    dt.Convergidos <- dt.Convergidos[NUM_TOTAL == 1]
  } else if (type == "multi") {
    dt.Convergidos <- dt.Convergidos[NUM_TOTAL > 1]
  }
  dt.Convergidos <- dt.Convergidos[, !c("SUM_VOZ_M1", "SUM_ADSL_M1", "SUM_HZ_M1", "SUM_FTTH_M1", 
                                        "SUM_LPD_M1", "SUM_TIVO_M1", "SUM_FLAGFUTBOL_M1"), with = F]
  
  setkey(dt.Convergidos, x_num_ident)
  
  # Join with Accenture Output
  # There is not par_propensos_cross_nif in the first month of data
  if (strptime(paste0(prev.month, "01"), "%Y%m%d") >= strptime(paste0(first.month.with.data.prop, "01"), "%Y%m%d")) {
    ifile <- file.path(data.folder, "ACC", prev.month, paste0("dt_par_propensos_cross_nif_", prev.month, ".RData"))
    cat("[LOAD] ", ifile, "\n")
    load(ifile) # FIXME
    
    setkey(dt_par_propensos_cross_nif, x_num_ident)
    
    dt.Convergidos <- dt_par_propensos_cross_nif[dt.Convergidos]
    dt.Convergidos <- cleanNA(dt.Convergidos)
  } else {
    cat("[INFO] Skipping", paste0("dt_par_propensos_cross_nif_", prev.month, ".RData"), "because the first month with this data file is", first.month.with.data.prop, "\n")
  }
  
  # Join with Campaigns
  # ifile <- file.path(data.folder, "Campaign", month, paste0("dt.Campaigns.", month, ".RData"))
  # cat("[LOAD] ", ifile, "\n")
  # load(ifile) # FIXME
  # setnames(dt.Campaigns, "CIF_NIF", "x_num_ident")
  # setkey(dt.Campaigns, x_num_ident)
  #
  # dt.Convergidos <- dt.Campaigns[dt.Convergidos]
  # nombres <- names(dt.Campaigns)
  # nombres <- nombres[!nombres %in% "x_num_ident" ]
  # dt.Convergidos[is.na(number_of_contacts), nombres := -1, with = F]
  
  # TODO: Join with clusters
  
  # Join with channel propensity
  # ofolder <- file.path(data.folder, "Contacts", month)
  # ifile <- file.path(ofolder, paste0("dt.contacts.hist.byid-", month, ".RData"))
  # cat("[LOAD] ", ifile, "\n")
  # load(ifile)
  # setkey(dt.contacts.hist.byid, x_num_ident)
  # dt.contacts.hist.byid[, GENESIS_HIST_TASA_INTENTO := NULL]
  # dt.contacts.hist.byid[, GENESIS_HIST_MAX_TASA_INTENTO := NULL]
  # dt.contacts.hist.byid[, GENESIS_HIST_PROP_CANAL_TELEVENTA := NULL]
  # dt.contacts.hist.byid[, GENESIS_HIST_MAX_PROP_CANAL_TELEVENTA := NULL]
  # 
  # dt.Convergidos <- dt.contacts.hist.byid[dt.Convergidos]
  # nombres <- names(dt.contacts.hist.byid)
  # nombres <- nombres[!nombres %in% "x_num_ident"]
  # dt.Convergidos[is.na(GENESIS_HIST_NUM_INTENTOS), nombres := 0, with = F]

  print(summary(dt.Convergidos))

  ifile <- file.path(datasets.folder, month, paste0("dt.NoConvergidos-train-", month, ".RData"))
  if (is.null(dt.NoConvergidos)) {
    cat("[LOAD] ", ifile, "\n")
    load(ifile)
  } else {
    cat("[INFO] Skipping", ifile, ", already loaded\n")
  }

  all.rows <- nrow(dt.NoConvergidos)
  dt.NoConvergidos[, CLASS := 0]
  if (type == "mono") {
    dt.NoConvergidos <- dt.NoConvergidos[NUM_TOTAL == 1]
  } else if (type == "multi") {
    dt.NoConvergidos <- dt.NoConvergidos[NUM_TOTAL > 1]
  }

  setkey(dt.NoConvergidos, x_num_ident)
  
  # Join with Accenture Output
  if (exists("dt_par_propensos_cross_nif")) {
    dt.NoConvergidos <- dt_par_propensos_cross_nif[dt.NoConvergidos]
    dt.NoConvergidos <- cleanNA(dt.NoConvergidos)
  }
  
  # Join with Campaigns
  # dt.NoConvergidos <- dt.Campaigns[dt.NoConvergidos]
  # nombres <- names(dt.Campaigns)
  # nombres <- nombres[!nombres %in% "x_num_ident" ]
  # dt.NoConvergidos[is.na(number_of_contacts), nombres := -1L, with = F]
  
  # TODO: Join with clusters
  
  # Join with channel propensity
  # dt.NoConvergidos <- dt.contacts.hist.byid[dt.NoConvergidos]
  # nombres <- names(dt.contacts.hist.byid)
  # nombres <- nombres[!nombres %in% "x_num_ident"]
  # dt.NoConvergidos[is.na(GENESIS_HIST_NUM_INTENTOS), nombres := 0, with = F]


  suppressWarnings(dt.Convergidos[, FLAG_COBERTURA_ADSL := NULL])
  suppressWarnings(dt.NoConvergidos[, FLAG_COBERTURA_ADSL := NULL])

  cat("[INFO] Convergidos:    ", nrow(dt.Convergidos), "\n")
  cat("[INFO] No Convergidos: ", nrow(dt.NoConvergidos), "\n")
  
  if (nrow(dt.Convergidos) > nrow(dt.NoConvergidos))
    stop("Converged data set is greater than Not Converged!", call.=FALSE)
  
  dt.NoConvergidosBalanced <- dt.NoConvergidos[sample(1:nrow(dt.NoConvergidos), nrow(dt.Convergidos))]

  cat("[INFO] No Convergidos: ", nrow(dt.NoConvergidosBalanced), "(balanced) chosen from ", all.rows, "\n")

  cat("[INFO] ", colnames(dt.Convergidos)[!colnames(dt.Convergidos) %in% colnames(dt.NoConvergidos)], "\n")
  #cat("[INFO] ", colnames(dt.NoConvergidos), "\n")
  
  print(summary(dt.NoConvergidos))
  
  dt.train <- list(balanced=rbind(dt.Convergidos, dt.NoConvergidosBalanced),
                   all=rbind(dt.Convergidos, dt.NoConvergidos))
}

generateValid <- function(month, type, dt.Convergidos = NULL, dt.NoConvergidos = NULL) {
  #dt.Convergidos.prev <- getConvergidosPrevMonth("201608")
  ifile <- file.path(data.folder, "Campaign", month, paste0("dt.Campaigns.", month, ".RData"))
  cat("[LOAD] ", ifile, "\n")
  load(ifile) # FIXME
  
  setnames(dt.Campaigns, "CIF_NIF", "x_num_ident")
  setkey(dt.Campaigns, x_num_ident)
  
  ifile <- file.path(datasets.folder, month, paste0("dt.Convergidos-train-", month, ".RData"))
  if (is.null(dt.Convergidos)) {
    cat("[LOAD] ", ifile, "\n")
    load(ifile)
  } else {
    cat("[INFO] Skipping", ifile, ", already loaded\n")
  }
  
  dt.Convergidos[, CLASS := 1]
  if (type == "mono") {
    dt.Convergidos <- dt.Convergidos[NUM_TOTAL == 1]
  } else if (type == "multi") {
    dt.Convergidos <- dt.Convergidos[NUM_TOTAL > 1]
  }
  dt.Convergidos <- dt.Convergidos[, !c("SUM_VOZ_M1", "SUM_ADSL_M1", "SUM_HZ_M1", "SUM_FTTH_M1", 
                                        "SUM_LPD_M1", "SUM_TIVO_M1", "SUM_FLAGFUTBOL_M1"), with = F]
  
  setkey(dt.Convergidos, x_num_ident)
  dt.Convergidos <- dt.Campaigns[dt.Convergidos]
  nombres <- names(dt.Campaigns)
  nombres <- nombres[!nombres %in% "x_num_ident" ]
  dt.Convergidos[is.na(number_of_contacts), nombres := -1, with = F]
  
  #dt.Convergidos <- rbind(dt.Convergidos, dt.Convergidos.prev)
  #ifile <- file.path(datasets.folder, month, paste0("dt.NoConvergidos-train-camp-", month, ".RData"))
  ifile <- file.path(datasets.folder, month, paste0("dt.NoConvergidos-train-", month, ".RData"))
  if (is.null(dt.NoConvergidos)) {
    cat("[LOAD] ", ifile, "\n")
    load(ifile)
  } else {
    cat("[INFO] Skipping", ifile, ", already loaded\n")
  }
  
  all.rows <- nrow(dt.NoConvergidos)
  dt.NoConvergidos[, CLASS := 0]
  if (type == "mono") {
    dt.NoConvergidos <- dt.NoConvergidos[NUM_TOTAL == 1]
  } else if (type == "multi") {
    dt.NoConvergidos <- dt.NoConvergidos[NUM_TOTAL > 1]
  }
  
  # Without sampling
  # dt.NoConvergidos <- dt.NoConvergidos[sample(1:nrow(dt.NoConvergidos), nrow(dt.Convergidos))]
  
  setkey(dt.NoConvergidos, x_num_ident)
  dt.NoConvergidos <- dt.Campaigns[dt.NoConvergidos]
  nombres <- names(dt.Campaigns)
  nombres <- nombres[!nombres %in% "x_num_ident" ]
  dt.NoConvergidos[is.na(number_of_contacts), nombres := -1L, with = F]
  
  cat("[INFO] Convergidos:     ", nrow(dt.Convergidos), "\n")
  cat("[INFO] No Convergidos: ", nrow(dt.NoConvergidos), " chosen from ", all.rows, "\n")
  
  dt.valid <- rbind(dt.Convergidos, dt.NoConvergidos)
}

prepareData <- function(dt.data) {
  cat("[INFO] prepareData()\n")
  
  dt.data[, x_nacionalidad := as.numeric(x_nacionalidad)]
  dt.data[, x_plan := as.numeric(x_plan)]
  dt.data[, PLANDATOS := as.numeric(PLANDATOS)]
  
  dt.data[, PROMOCION_VF := as.numeric(PROMOCION_VF)]
  dt.data[, sistema_operativo := as.numeric(sistema_operativo)]
  dt.data[, PPRECIOS_DESTINO := as.numeric(PPRECIOS_DESTINO)]
  dt.data[, ppid_destino := as.numeric(ppid_destino)]
  
  # Load Latitude and Longitude
  #load("/Users/fede/Downloads/dt.CodPostalLongLat.rda")
  #setkey(dt.CodPostalLongLat, CODIGO_POSTAL)
  #dt.data[, CODIGO_POSTAL := as.character(CODIGO_POSTAL)]
  #setkey(dt.data, CODIGO_POSTAL)
  #dt.data <- dt.CodPostalLongLat[dt.data]
  #dt.data[, Codigo_Postal_long := as.numeric(Codigo_Postal_long)]
  #dt.data[, Codigo_Postal_lat := as.numeric(Codigo_Postal_lat)]
  #dt.data[, CODIGO_POSTAL := NULL]
  dt.data[, CODIGO_POSTAL := as.numeric(CODIGO_POSTAL)]
  
  dt.data[, modelo := as.numeric(modelo)]
  
  dt.data[, DIAS_DESDE_CREACION_CUENTA := NULL]
  dt.data[, DIAS_DESDE_CREACION_SERVICIO := NULL]
  dt.data[, DIAS_DESDE_INI_PROV := NULL]
  #dt.data[, DIAS_DESDE_ACTIVACION := NULL]
  dt.data[, DIAS_DESDE_LAST_UPD := NULL]
  dt.data[, FLAG_HUELLA_ONO := NULL]
  dt.data[, FLAG_HUELLA_VF := NULL]
  
  dt.data[, x_num_ident := NULL] 
  
  dt.data[, x_tipo_ident := NULL]
  
  dt.data[, PROMOCION_TARIFA := NULL]
  
  dt.data[, part_status := as.numeric(part_status)]
}

trainRFModel <- function(dt.train, train.class, i, month, trees, type) {
  cat("[INFO] trainRF...\n")
  bootControl <- trainControl(method = "none")
  #bootControl <- trainControl(method = "cv", number = 4)
  
  cat("Training with ", ncol(dt.train), " variables and ", nrow(dt.train), "records ...\n")
  rf.fit <- train(dt.train, train.class, method = "rf", tuneGrid = data.frame(mtry = sqrt(ncol(dt.train))),
                  trControl = bootControl, 
                  scaled = F, do.trace = T, ntree = trees)
  
  print(rf.fit)
  print(rf.fit$finalModel)
  
  ofolder <- file.path(models.folder, month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- file.path(ofolder, paste0("rf.error.caret-", month, ".", type, "-", i, ".png"))
  png(filename = ofile)
  plot(rf.fit$finalModel, main = paste0("Error evolution for model month ", month, " and type ", type,  "-", i))
  dev.off()
  
  ofile <- file.path(ofolder, paste0("varImpPlot.caret-", month, ".", type, "-", i,".png"))
  png(filename = ofile)
  varImpPlot(rf.fit$finalModel, main = paste0("VarImp for model month ", month, " and type ", type,  "-", i))
  dev.off()
  
  return(rf.fit)
}

trainRF <- function(month, trees, type, iter.samples, dt.Convergidos = NULL, dt.NoConvergidos = NULL) {
  for (i in 1:iter.samples) { 
    cat("[INFO] Iteration: ", i, "\n")
    data.Train <- generateTrain(month, type, dt.Convergidos, dt.NoConvergidos)
    dt.train <- data.Train$balanced
    dt.train <- prepareData(dt.train)
    
    train.class <- dt.train[["CLASS"]]
    dt.train[, CLASS := NULL]
    train.class <- as.factor(train.class)
    
    rf.fit <- trainRFModel(dt.train, train.class, i, month, trees, type)
    
    if (i == 1) {
      rf.combined <- rf.fit$finalModel
    }
    else{
      rf.combined <- combine(rf.combined, rf.fit$finalModel)
    }
  }
  
  ofolder <- file.path(models.folder, month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- file.path(ofolder, paste0("rf.model.caret-", month, ".", type, ".RData"))
  cat("[SAVE] ", ofile, "\n")
  save(rf.combined, file = ofile)
}

trainXG <- function(month, trees, type, valid.month, dt.Convergidos = NULL, dt.NoConvergidos = NULL) {
  data.Train <- generateTrain(month, type, dt.Convergidos, dt.NoConvergidos)
  dt.train <- data.Train$balanced
  dt.train <- prepareData(dt.train)
  
  train.class <- dt.train[["CLASS"]]
  dt.train[, CLASS := NULL]
  #train.class <- as.factor(train.class)
  
  dtrain <- xgb.DMatrix(data = data.matrix(dt.train),label = train.class)
  params <- list(booster = "gbtree",
                 objective = "binary:logistic",
                 eval_metric = "auc",
                 max_depth = 3, 
                 eta = 0.1,
                 colsample_bytree = .8, 
                 subsample = .8) 
  
  set.seed(1234)
  #xgb.model.cv <- xgb.cv(params = params, data = dtrain,
  #                       nfold = 4, nrounds = 2000,
  #                       #early.stop.round = 20,
  #                       print.every = 1,
  #                       metrics = list("auc", "logloss"),
  #                       maximize = T)
  
  if (valid.month != "none") {
    data.Test <- generateTrain(valid.month, type, dt.Convergidos, dt.NoConvergidos)
    dt.test <- data.Test$balanced
    dt.test <- prepareData(dt.test)
  
    test.class <- dt.test[["CLASS"]]
    dt.test[, CLASS := NULL]
    dtest <- xgb.DMatrix(data = data.matrix(dt.test),label = test.class)
  
    watchlist <- list(test = dtest, train = dtrain)
  
    xgb.model <- xgb.train(params = params, data = dtrain,
                         nrounds = 1000,
                         maximize = T,
                         watchlist = watchlist,
                         early.stop.round = 50,
                         print.every.n = 1)
  } else {
    watchlist <- list(train = dtrain)
    
    xgb.model <- xgb.train(params = params, data = dtrain,
                           nrounds = 1000,
                           maximize = T,
                           watchlist = watchlist,
                           print.every.n = 1)
    
  }
  
  ofolder <- file.path(models.folder, month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- file.path(ofolder, paste0("xgb.model-", month, ".RData"))
  cat("[SAVE] ", ofile, "\n")
  save(xgb.model, file = ofile)
  
  #xgb.grid <- data.frame(nrounds = 500,
  #                       eta = 0.5, #c(0.01,0.05,0.1),
  #                       max_depth = 4, # c(2,4,6,8,10,14),
  #                       gamma = 0,
  #                       colsample_bytree = .8,
  #                       min_child_weight = 1,
  #                       subsample = .9
  #)
  # The final values used for the model were nrounds = 500, max_depth = 4, eta = 0.05, gamma = 0, colsample_bytree =
  # 0.8, min_child_weight = 1 and subsample = 0.9. 
  
  #xgb.fit <- train(as.matrix(dt.train), train.class, method = "xgbTree",  trControl = bootControl, tuneGrid = xgb.grid,
  #                 verbose = T, metric = "auc")
}

trainH2ORF <- function(month, trees, type, valid.month, dt.Convergidos = NULL, dt.NoConvergidos = NULL) {
  cat("[INFO] trainH2ORF(", month, ",", trees, ",", type, ",", valid.month, ")\n")
  
  suppressMessages(library(h2o))
  set.seed(1234)
  
  data.Train <- generateTrain(month, type, dt.Convergidos, dt.NoConvergidos)
  dt.train <- data.Train$balanced
  dt.train <- prepareData(dt.train)
  dt.all <- data.Train$all
  dt.all <- prepareData(dt.all)
  
  dt.train[, CLASS := as.factor(CLASS)]
  if (valid.month != "none") {
    dt.valid <- generateValid(valid.month, type, dt.Convergidos, dt.NoConvergidos)
    dt.valid <- prepareData(dt.valid)
    dt.valid[, CLASS := as.factor(CLASS)]
  }
  
  localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
  dev.null <- h2o.removeAll()
  
  cat("[INFO] Loading Data into H2O Cluster...\n")
  train.hex <- as.h2o(dt.train, "train.hex")
  all.hex <- as.h2o(dt.all, "all.hex")
  
  cat("[INFO] Training model...\n")
  if (valid.month != "none") {
    valid.hex <- as.h2o(dt.valid, "valid.hex")
    h2o.rf <- h2o.randomForest(training_frame = train.hex, x = setdiff(colnames(dt.train), "CLASS"), y = "CLASS", 
                             ntrees = trees, seed = 1234, validation_frame = valid.hex, score_each_iteration = F,
                             stopping_rounds = 5, model_id = "h2orf")
  } else {
    h2o.rf <- h2o.randomForest(training_frame = train.hex, x = setdiff(colnames(dt.train), "CLASS"), y = "CLASS", 
                               ntrees = trees, seed = 1234, model_id = "h2orf", binomial_double_trees = T)
  }
  
  print(h2o.rf)
  
  # Just for testing a model after creation
  if (FALSE) {
    month <- "201612"
    trees <- 500
    type <- "all"
    valid.month <- "none"
    source("configuration.R")
    suppressMessages(library(h2o))
    set.seed(1234)
    load(file.path(datasets.folder, month, paste0("dt.Convergidos-train-", month, ".RData")))
    load(file.path(datasets.folder, month, paste0("dt.NoConvergidos-train-", month, ".RData")))
    data.Train <- generateTrain(month, type, dt.Convergidos, dt.NoConvergidos) 
    dt.train <- data.Train$balanced
    dt.train <- prepareData(dt.train)
    dt.all <- data.Train$all
    dt.all <- prepareData(dt.all)
    dt.train[, CLASS := as.factor(CLASS)]
    localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
    dev.null <- h2o.removeAll()
    all.hex <- as.h2o(dt.all, "all.hex")
    ifile <- file.path(models.folder, month, "rf.model.h2o", "h2orf")
    h2o.rf <- h2o.loadModel(ifile)
  }
  
  #cat("[INFO] Calculating H2O Gains/Lift Table\n")
  #print(h2o.gainsLift(h2o.rf, newdata = all.hex))
  
  cat("[INFO] Generating Model Performance Metrics using all input data\n")
  perf <- h2o.performance(h2o.rf, newdata = all.hex)
  print(perf); cat("\n\n")
  
  #cat("[INFO] Calculating H2O Gains/Lift Table using all input data\n")
  #print(h2o.gainsLift(perf))
  
  cat("[INFO] Predicting train data using all input data...\n")
  preds.new <- h2o.predict(object = h2o.rf, newdata = all.hex)
  preds.df <- as.data.frame(preds.new)
  #preds.dt <- as.data.table(preds.df)
  
  cat("[INFO] Computing lift\n")
  source("lift.R")
  print(computeLift(preds.df$p1, dt.all$CLASS))
  #plot(computeLift(preds.df$p1, dt.all$CLASS))
  
  ofolder <- file.path(models.folder, month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }

  ofile <- file.path(ofolder, paste0("rf.metrics.h2o-", month, ".", type, ".txt"))
  write(capture.output(h2o.rf), file=ofile, append = FALSE)
  write("\nModel Performance Metrics using all input data", file=ofile, append = TRUE)
  write(    "==============================================\n", file=ofile, append = TRUE)
  write(capture.output(perf), file=ofile, append = TRUE)
  write(capture.output(computeLift(preds.df$p1, dt.all$CLASS)), file=ofile, append = TRUE)
  
  ofile <- file.path(ofolder, paste0("varImpPlot.H2ORF-", month, ".", type, ".png"))
  png(filename = ofile)
  h2o.varimp_plot(h2o.rf, num_of_features = 30)
  dev.off()
  
  ofile <- file.path(ofolder, paste0("rf.error.h2o-", month, ".", type, ".png"))
  png(filename = ofile)
  plot(h2o.rf, main = paste0("Error evolution for model month ", month, " and type ", type))
  dev.off()
  
  ofile <- file.path(ofolder, "rf.model.h2o")
  cat("[SAVE] ", ofile, "\n")

  local.path <- h2o.saveModel(h2o.rf, path = ofile, force = T)
  h2o.shutdown(prompt = F)
}

trainH2OGBM <- function(month, trees, type, valid.month, dt.Convergidos = NULL, dt.NoConvergidos = NULL) {
  suppressMessages(library(h2o))
  set.seed(1234)
  
  data.Train <- generateTrain(month, type, dt.Convergidos, dt.NoConvergidos) 
  dt.train <- data.Train$balanced
  dt.train <- prepareData(dt.train)
  dt.train[, CLASS := as.factor(CLASS)]
  if (valid.month != "none") {
    dt.valid <- generateValid(valid.month, type, dt.Convergidos, dt.NoConvergidos)
    dt.valid <- prepareData(dt.valid)
    dt.valid[, CLASS := as.factor(CLASS)]
  }
  
  localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
  dev.null <- h2o.removeAll()
  train.hex <- as.h2o(dt.train, "train.hex")
  if (valid.month != "none") {
    valid.hex <- as.h2o(dt.valid, "valid.hex")
    h2ogbm <- h2o.gbm(training_frame = train.hex, x = 1:(ncol(dt.train)-1), y = ncol(dt.train), 
                               ntrees = trees, seed = 1234, validation_frame = valid.hex, score_each_iteration = T,
                               stopping_rounds = 5, model_id = "h2ogbm",
                               sample_rate = 0.7, 
                               col_sample_rate = 0.7)
  } else {
    h2ogbm <- h2o.gbm(training_frame = train.hex, x = 1:(ncol(dt.train)-1), y = ncol(dt.train), 
                               ntrees = trees, seed = 1234, model_id = "h2ogbm",
                                #max_depth = 10,
                                sample_rate = 0.7, 
                                col_sample_rate = 0.7
                                #learn_rate = 0.3
                       )
  }
  
  print(h2ogbm)

  ofolder <- file.path(models.folder, month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- file.path(ofolder, paste0("varImpPlot.H2OGBM-", month, ".", type, ".png"))
  png(filename = ofile)
  h2o.varimp_plot(h2ogbm, num_of_features = 30)
  dev.off()
  
  ofile <- file.path(ofolder, paste0("gbm.error.h2o-", month, ".", type, ".png"))
  png(filename = ofile)
  plot(h2ogbm, main = paste0("Error evolution for model month ", month, " and type ", type))
  dev.off()
  
  ofile <- file.path(ofolder, "gbm.model.h2o")
  cat("[SAVE] ", ofile, "\n")
  
  local.path <- h2o.saveModel(h2ogbm, path = ofile, force = T)
  h2o.shutdown(prompt = F)
}

main_06_train_convergence_model <- function(month, iter.samples, trees, model, type, valid.month, dt.Convergidos = NULL, dt.NoConvergidos = NULL) {
  cat ("\n[MAIN] 06_train_convergence_model", month, iter.samples, trees, model, type, valid.month, "\n")
  
  set.seed(1234)
 
  if (model == "rf") {
    trainRF(month, trees, type, iter.samples, dt.Convergidos, dt.NoConvergidos)
  
  } else if (model == "xg") {
    trainXG(month, trees, type, valid.month, dt.Convergidos, dt.NoConvergidos)
  
  } else if (model == "h2orf") {
    trainH2ORF(month, trees, type, valid.month, dt.Convergidos, dt.NoConvergidos)

  } else if (model == "h2ogbm") {
    trainH2OGBM(month, trees, type, valid.month, dt.Convergidos, dt.NoConvergidos)
  }
  
}

#--------------------------------------------------------------------------------------------------
if (!exists("sourced")) {
  option_list <- list(
    make_option(c("-m", "--month"), type = "character", default = NULL, help = "input month (YYYYMM)", 
                metavar = "character"),
    make_option(c("-s", "--samples"), type = "character", default = NULL, help = "number of under_sampling iterations", 
                metavar = "character"),
    make_option(c("-t", "--trees"), type = "numeric", default = NULL, help = "number of trees", 
                metavar = "numeric"),
    make_option(c("-o", "--model"), type = "character", default = NULL, help = "model", 
                metavar = "character"),
    make_option(c("-y", "--type"), type = "character", default = NULL, help = "[mono | multi | all]", 
                metavar = "character"),
    make_option(c("-v", "--valid"), type = "character", default = NULL, help = "Validation Month", 
                metavar = "character")
  )
  
  opt_parser <- OptionParser(option_list = option_list)
  opt <- parse_args(opt_parser)
  
  if (is.null(opt$month)) {
    print_help(opt_parser)
    stop("At least one parameter must be supplied (input month: YYYYMM)", call.=FALSE)
  } else {
    main_06_train_convergence_model(opt$month, opt$samples, opt$trees, opt$model, opt$type, opt$valid, NULL, NULL)
  }
}
