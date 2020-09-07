source("configuration.R")
suppressMessages(library(caret))
suppressMessages(library(ROCR))
suppressMessages(library(xgboost))
suppressMessages(library(h2o))
suppressMessages(library(pROC))


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
  dt.data[is.na(IND_PROP_AOU), IND_PROP_AOU := -1]
  dt.data[is.na(IND_MUY_PROP_TV), IND_MUY_PROP_TV := -1]
  dt.data[is.na(IND_MUY_PROP_AOU), IND_MUY_PROP_AOU := -1]
}

generateTest <- function(test.month) {
  cat("[INFO] generateTest(", test.month, ")\n")
  
  month <- test.month
  prev.month <- paste0(month, "01")
  prev.month <- as.POSIXct(strptime(prev.month, "%Y%m%d"))
  prev.month <- prev.month - 2
  prev.month <- as.character(prev.month)
  prev.month <- substr(prev.month, 1, 7)
  prev.month <- gsub("-", "", prev.month)
  
  ifile <- file.path(data.folder, "ACC", prev.month, paste0("dt_par_propensos_cross_nif_", prev.month, ".RData"))
  if (file.exists(ifile)) {
    cat("[LOAD] ", ifile, "\n")
    load(ifile)
  } else {
    cat("[WARN] File", ifile, "does not exist!\n")
    
    # Check previous month
    
    # Get the first day of the month
    month <- paste0(month, "01")
    month.posix <- as.POSIXct(strptime(month, "%Y%m%d"))
    prev.day <- seq.POSIXt(month.posix, length=2, by="-1 months")[2]
    prev.month2 <- format(prev.day, format = "%Y%m")
    cat("[WARN] Trying previous month:", prev.month2, "\n")
    
    ifile <- file.path(data.folder, "ACC", prev.month2, paste0("dt_par_propensos_cross_nif_", prev.month2, ".RData"))
    cat("[LOAD] ", ifile, "\n")
    load(ifile)
  }
  
  setkey(dt_par_propensos_cross_nif, x_num_ident)
  
  
  ifile <- file.path(datasets.folder, test.month, paste0("dt.Convergidos-train-", test.month, ".RData"))
  cat("[LOAD] ", ifile, "\n")
  load(ifile)
  
  dt.Convergidos[, CLASS := 1]
  dt.Convergidos <- dt.Convergidos[, !c("SUM_VOZ_M1", "SUM_ADSL_M1", "SUM_HZ_M1", "SUM_FTTH_M1", 
                                        "SUM_LPD_M1", "SUM_TIVO_M1", "SUM_FLAGFUTBOL_M1"), with = F]
  
  setkey(dt.Convergidos, x_num_ident)
  
  # Join with Accenture Output
  dt.Convergidos <- dt_par_propensos_cross_nif[dt.Convergidos]
  dt.Convergidos <- cleanNA(dt.Convergidos)
  
  # Join with Campaigns
  # ifile <- file.path(data.folder, "Campaign", month, paste0("dt.Campaigns.", month, ".RData"))
  # cat("[LOAD] ", ifile, "\n")
  # load(ifile)
  # setnames(dt.Campaigns, "CIF_NIF", "x_num_ident")
  # setkey(dt.Campaigns, x_num_ident)
  #
  # dt.Convergidos <- dt.Campaigns[dt.Convergidos]
  # nombres <- names(dt.Campaigns)
  # nombres <- nombres[!nombres %in% "x_num_ident" ]
  # dt.Convergidos[is.na(number_of_contacts), nombres := -1, with = F]
  
  # TODO: Join with clusters
  
  # Join with channel propensity
  ofolder <- file.path(data.folder, "Contacts", month)
  ifile <- file.path(ofolder, paste0("dt.contacts.hist.byid-", month, ".RData"))
  cat("[LOAD] ", ifile, "\n")
  load(ifile)
  setkey(dt.contacts.hist.byid, x_num_ident)
  dt.contacts.hist.byid[, GENESIS_HIST_TASA_INTENTO := NULL]
  dt.contacts.hist.byid[, GENESIS_HIST_MAX_TASA_INTENTO := NULL]
  dt.contacts.hist.byid[, GENESIS_HIST_PROP_CANAL_TELEVENTA := NULL]
  dt.contacts.hist.byid[, GENESIS_HIST_MAX_PROP_CANAL_TELEVENTA := NULL]
  
  dt.Convergidos <- dt.contacts.hist.byid[dt.Convergidos]
  nombres <- names(dt.contacts.hist.byid)
  nombres <- nombres[!nombres %in% "x_num_ident"]
  dt.Convergidos[is.na(GENESIS_HIST_NUM_INTENTOS), nombres := 0, with = F]
  
  
  ifile <- file.path(datasets.folder, test.month, paste0("dt.NoConvergidos-train-", test.month, ".RData"))
  cat("[LOAD] ", ifile, "\n")
  load(ifile)
  dt.NoConvergidos[, CLASS := 0]
  
  setkey(dt.NoConvergidos, x_num_ident)
  
  # Join with Accenture Output
  dt.NoConvergidos <- dt_par_propensos_cross_nif[dt.NoConvergidos]
  dt.NoConvergidos <- cleanNA(dt.NoConvergidos)
  
  # Join with Campaigns
  #dt.NoConvergidos <- dt.Campaigns[dt.NoConvergidos]
  #nombres <- names(dt.Campaigns)
  #nombres <- nombres[!nombres %in% "x_num_ident" ]
  #dt.NoConvergidos[is.na(number_of_contacts), nombres := -1L, with = F]
  
  # TODO: Join with clusters
  
  # Join with channel propensity
  dt.NoConvergidos <- dt.contacts.hist.byid[dt.NoConvergidos]
  nombres <- names(dt.contacts.hist.byid)
  nombres <- nombres[!nombres %in% "x_num_ident"]
  dt.NoConvergidos[is.na(GENESIS_HIST_NUM_INTENTOS), nombres := 0, with = F]
  
  
  cat("[INFO] Convergidos:    ", nrow(dt.Convergidos), "\n")
  cat("[INFO] No Convergidos: ", nrow(dt.NoConvergidos), "\n")
  
  dt.train <- rbind(dt.Convergidos, dt.NoConvergidos)
}

prepareDataXG <- function(dt.data) {
  cat("[INFO] prepareDataXG()\n")
  
  dt.data[, x_nacionalidad := as.numeric(x_nacionalidad)]
  dt.data[, x_plan := as.numeric(x_plan)]
  dt.data[, PLANDATOS := as.numeric(PLANDATOS)]
  
  dt.data[, PROMOCION_VF := as.numeric(PROMOCION_VF)]
  dt.data[, sistema_operativo := as.numeric(sistema_operativo)]
  dt.data[, PPRECIOS_DESTINO := as.numeric(PPRECIOS_DESTINO)]
  dt.data[, ppid_destino := as.numeric(ppid_destino)]
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

prepareData <- function(dt.data) {
  cat("[INFO] prepareData()\n")
  
  dt.data[, x_nacionalidad := as.numeric(x_nacionalidad)]
  dt.data[, x_plan := as.numeric(x_plan)]
  dt.data[, PLANDATOS := as.numeric(PLANDATOS)]
  
  dt.data[, PROMOCION_VF := as.numeric(PROMOCION_VF)]
  dt.data[, sistema_operativo := as.numeric(sistema_operativo)]
  dt.data[, PPRECIOS_DESTINO := as.numeric(PPRECIOS_DESTINO)]
  dt.data[, ppid_destino := as.numeric(ppid_destino)]
  dt.data[, CODIGO_POSTAL := as.numeric(CODIGO_POSTAL)]
  dt.data[, modelo := as.numeric(modelo)]
  
  dt.data[, DIAS_DESDE_CREACION_CUENTA := NULL]
  dt.data[, DIAS_DESDE_CREACION_SERVICIO := NULL]
  dt.data[, DIAS_DESDE_INI_PROV := NULL]
  #dt.data[, DIAS_DESDE_ACTIVACION := NULL]
  dt.data[, DIAS_DESDE_LAST_UPD := NULL]
  dt.data[, FLAG_HUELLA_ONO := NULL]
  dt.data[, FLAG_HUELLA_VF := NULL]
  
  #dt.data[, x_num_ident := NULL] 
  
  dt.data[, x_tipo_ident := NULL]
  
  dt.data[, PROMOCION_TARIFA := NULL]
  dt.data[, part_status := as.numeric(part_status)]
}

evaluateRF <- function(month, dt.test) {
  ifile <- file.path(models.folder, month, paste0("rf.model.caret-", month, ".RData"))
  cat("[LOAD] ", ifile, "\n")
  load(ifile)
  
  cat("[INFO] Predicting...\n")
  pred.1 <- predict(rf.combined, dt.test, type = "prob")
  
  return(pred.1[,2])
}

evaluateXG <- function(month, dt.test) {
  ifile <- file.path(models.folder, month, paste0("xgb.model-", month, ".RData"))
  cat("[LOAD] ", ifile, "\n")
  load(ifile)
  
  cat("[INFO] Predicting...\n")
  pred.1 <- predict(xgb.model, data.matrix(dt.test))
  pred.1 <- as.data.frame(pred.2)
  
  return(pred.1[,1])
}

evaluateH2ORF <- function(month, dt.test) {
  localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
  dev.null <- h2o.removeAll()
  cat("[INFO] Loading Data into H2O Cluster...\n")
  test.hex <- as.h2o(dt.test, "test.hex")
  
  ifile <- file.path(models.folder, month, "rf.model.h2o", "h2orf")
  cat("[LOAD] ", ifile, "\n")
  h2o.rf <- h2o.loadModel(ifile)
  
  cat("[INFO] Predicting...\n")
  pred.1 <- h2o.predict(object = h2o.rf, newdata = test.hex)
  pred.1 <- as.data.frame(pred.1)
  
  #cat("[INFO] Calculating H2O Gains/Lift Table\n")
  #print(h2o.gainsLift(h2o.rf, newdata = test.hex))

  cat("[INFO] Generating Model Performance Metrics\n")
  h2o.perf <- h2o.performance(h2o.rf, newdata = test.hex)
  log <- capture.output(h2o.perf)

  #cat("[INFO] Calculating H2O Gains/Lift Table\n")
  #log <- c(log, capture.output(h2o.gainsLift(h2o.perf)))

  #cat("[INFO] Calculating H2O Metrics\n")
  #metrics <- h2o.metric(h2o.perf)

  cat("[INFO] Computing lift\n")
  source("lift.R")
  log <- c(log, capture.output(computeLift(pred.1$p1, dt.test$CLASS)))
  #plot(computeLift(pred.1$p1, dt.test$CLASS))

  #cat(log, sep = "\n")

  h2o.shutdown(prompt = F)
  
  return(list(pred.1=pred.1[,3], log=log))
}

evaluateH2OGBM <- function(month, test.hex) {
  ifile <- file.path(models.folder, month, "gbm.model.h2o", "h2ogbm")
  cat("[LOAD] ", ifile, "\n")
  h2ogbm <- h2o.loadModel(ifile)
  
  cat("[INFO] Predicting...\n")
  pred.1 <- h2o.predict(object = h2ogbm, newdata = test.hex)
  pred.1 <- as.data.frame(pred.1)

  return(pred.1[,3])
}

main_07_evaluate_convergence_model <- function(month, test.month, model) {
  cat ("\n[MAIN] 07_evaluate_convergence_model", month, test.month, model, "\n")
  
  dt.test <- generateTest(test.month)

  if (model == "rf") {
    dt.test <- prepareData(dt.test)
    pred.1  <- evaluateRF(month, dt.test)
    
  } else if (model == "xg") {
    dt.test <- prepareDataXG(dt.test)
    pred.1  <- evaluateXG(month, dt.test)
    
  } else if (model == "h2orf") {
    dt.test <- prepareData(dt.test)
    ret.list  <- evaluateH2ORF(month, dt.test)
    pred.1 <- ret.list$pred.1
    log <- ret.list$log
    
  } else if (model == "h2ogbm") {
    dt.test <- prepareData(dt.test)
    
    localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
    dev.null <- h2o.removeAll()
    cat("[INFO] Loading Data into H2O Cluster...\n")
    test.hex <- as.h2o(dt.test, "test.hex")
    
    pred.1  <- evaluateH2OGBM(month, test.hex)
    h2o.shutdown(prompt = F)
    
  } else if (model == "combined") {
    
    dt.test <- prepareData(dt.test)
    
    localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
    dev.null <- h2o.removeAll()
    cat("[INFO] Loading Data into H2O Cluster...\n")
    test.hex <- as.h2o(dt.test, "test.hex")
    
    pred.1  <- evaluateH2ORF(month, test.hex)
    pred.2  <- evaluateH2OGBM(month, test.hex)
    
    h2o.shutdown(prompt = F)
    
    pred.1 <- (.5 * pred.1) + (.5 * pred.2)
  }

    
  #cat("Predicting...\n")
  #pred.1 <- predict(rf.combined, dt.test)
  #dt.test[, PRED := pred.1]
  #head(dt.test[, c("CLASS", "PRED"), with =F])
  #print(table(dt.test$CLASS, dt.test$PRED))
  #print(prop.table(table(dt.test$CLASS, dt.test$PRED), margin = 2))
  #pred.comb <- (pred.1[,2] + pred.2[,1])/2
  #pred <- prediction(pred.comb, dt.test$CLASS)
  
  pred <- prediction(pred.1, dt.test$CLASS)
  
  dt.test[, PRED := pred.1]
  
  auc  <- performance(pred, "auc")
  auc <- unlist(slot(auc, "y.values"))
  cat("AUC: ", auc, "\n")
  perf <- performance(pred, "tpr", "fpr")
  
  now <- as.character(format(Sys.time(), "%d-%m-%Y_%H:%M:%S"))
  base.filename <- paste0(model, ".", month, ".pred.", test.month, ".auc.", auc, ".", now)
  
  ofile <- file.path(models.folder, month, paste0(base.filename, ".png"))
  png(filename = ofile)
  plot(perf, main = paste0("Model: ", month, " test ", test.month, " AUC ", round(auc, digits = 4)))
  
  dev.off()
  
  ofile <- file.path(models.folder, month, paste0(base.filename, ".txt"))
  log <- c(paste0("Model month: ", month, "\nTest  month:", test.month, "\nModel: ", model, "\n"), log)
  cat(log, sep = "\n")
  write(log, file=ofile, append = FALSE)
  
  ofile <- file.path(models.folder, month, paste0(base.filename, ".csv"))
  cat("[SAVE] ", ofile, "\n")
  write.table(dt.test[, c("x_num_ident", "PRED", "CLASS"), with = F], 
              file = ofile, sep = "|", quote = F, row.names = F)
}

#--------------------------------------------------------------------------------------------------
if (!exists("sourced")) {
  option_list <- list(
    make_option(c("-m", "--modelmonth"), type = "character", default = NULL, help = "model month (YYYYMM)", 
                metavar = "character"),
    make_option(c("-t", "--testmonth"), type = "character", default = NULL, help = "test month (YYYYMM)", 
                metavar = "character"),
    make_option(c("-o", "--model"), type = "character", default = NULL, help = "model type (rf, xg, h2orf, h2ogbm)", 
                metavar = "character")
  )
  
  opt_parser <- OptionParser(option_list = option_list)
  opt <- parse_args(opt_parser)
  
  if (is.null(opt$modelmonth) | is.null(opt$testmonth) | is.null(opt$model)) {
    print_help(opt_parser)
    stop("Error: --modelmonth, --testmonth, and --model parameters must be supplied", call.=FALSE)
  } else {
    main_07_evaluate_convergence_model(opt$modelmonth, opt$testmonth, opt$model)
  }
}
