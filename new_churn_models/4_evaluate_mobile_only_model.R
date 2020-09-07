source("configuration.R")

computeLift <- function(preds, targets, nile = 10) {
  results.deciles <- getDeciles(preds, nile)
  
  # Computing lift
  churnrate <- sum(as.logical(targets))/length(targets)
  results.lift <- rep(0, nile)
  for(i in 1:nile) {
    targetdec <- as.logical(targets[results.deciles$label==i])
    results.lift[i] <- (sum(targetdec==TRUE)/length(targetdec))/churnrate
    rm(targetdec)
  }
  results.lift
}

getDeciles <- function(preds, nile = 10) {
  numobsperdec <- round(length(preds)/nile)
  idxordpreds <- order(preds, decreasing = T)
  
  decile.label <- rep(0, length(preds))
  
  for(i in nile:1) {
    temp <- idxordpreds[((nile-i)*numobsperdec+1):((nile-i+1)*numobsperdec)]
    decile.label[temp] <- i
    rm(temp)
  }
  
  deciles.output <- list(data = preds, label = decile.label, idx = idxordpreds)
  
  deciles.output
}

generateTest <- function(dt.test) {
  remove.cols <- c(      
    "NUM_FACT_SIN_SMS_M0"  , "SEG_NACIONALIDAD",
    "NUM_FACT_INTERNAC_M0",  "NUM_ACCESOS_3G_M0", "NUM_DATOS_3G_M0"      
  )
  
  dt.test <- dt.test[, !remove.cols, with = F]
  
  dt.test[, SEG_TIPO_IDENT := as.factor(SEG_TIPO_IDENT)]
  dt.test[, SEG_DESCUENTO_MAX := as.factor(SEG_DESCUENTO_MAX)]
  dt.test[, SEG_SISTEMA_OPERATIVO:= as.factor(SEG_SISTEMA_OPERATIVO)]
  dt.test[, SEG_TIPO_HOGAR_HH := as.factor(SEG_TIPO_HOGAR_HH)]
  dt.test[, SEG_CICLO_VIDA := as.factor(SEG_CICLO_VIDA)]
  #dt.valid[, SEG_NACIONALIDAD := as.factor(SEG_NACIONALIDAD)]
  dt.test[, SEG_SEXO := as.factor(SEG_SEXO)]
}

evaluateH2ORF <- function(model.month, test.hex) {
  ifile <- paste0(models.folder, "/", model.month, "/rf.model.h2o/h2orf")
  cat("[LOAD] ", ifile, "\n")
  
  h2o.rf <- h2o.loadModel(ifile)
  
  cat("[INFO] Predicting...\n")
  pred.1 <- h2o.predict(object = h2o.rf, newdata = test.hex)
  print(h2o.gainsLift(h2o.rf, newdata = test.hex))
  pred.1 <- as.data.frame(pred.1)
  
  return(pred.1[,3])
}

evaluateH2OGBM <- function(model.month, test.hex) {
  ifile <- paste0(models.folder, "/", model.month, "/gbm.model.h2o/h2ogbm")
  cat("[LOAD] ", ifile, "\n")
  
  h2ogbm <- h2o.loadModel(ifile)
  
  pred.1 <- h2o.predict(object = h2ogbm, newdata = test.hex)
  print(h2o.gainsLift(h2ogbm, newdata = test.hex))
  
  pred.1 <- as.data.frame(pred.1)
  
  return(pred.1[,3])
}

main <- function(car.month, sol.month, model.month, model.type) {
  
  ifile <- paste0(churn.datasets.folder, "/dt.test.mo.", car.month, ".", sol.month, ".RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  
  dt.test <- generateTest(dt.test)
  
  if (model.type == "rf") {
    pred.1  <- evaluateRF(model.month, dt.test)
    
  } else if (model.type == "xg") {
    pred.1  <- evaluateXG(model.month, dt.test)
    
  } else if (model.type == "h2orf") {
    
    localH2O = h2o.init(nthreads = -1, max_mem_size = "10G", enable_assertions = F)
    h2o.removeAll()
    cat("[INFO] Loading Data into H2O Cluster...\n")
    test.hex <- as.h2o(dt.test, "test.hex")
    
    pred.1  <- evaluateH2ORF(model.month, test.hex)
    h2o.shutdown(prompt = F)
    
  } else if (model.type == "h2ogbm") {

    localH2O = h2o.init(nthreads = -1, max_mem_size = "10G", enable_assertions = F)
    h2o.removeAll()
    cat("[INFO] Loading Data into H2O Cluster...\n")
    test.hex <- as.h2o(dt.test, "test.hex")
    
    pred.1  <- evaluateH2OGBM(model.month, test.hex)
    h2o.shutdown(prompt = F)
    
  } else if (model.type == "combined") {
    
    dt.test <- prepareData(dt.test)
    
    localH2O = h2o.init(nthreads = -1, max_mem_size = "10G", enable_assertions = F)
    h2o.removeAll()
    cat("[INFO] Loading Data into H2O Cluster...\n")
    test.hex <- as.h2o(dt.test, "test.hex")
    
    pred.1  <- evaluateH2ORF(month, test.hex)
    pred.2  <- evaluateH2OGBM(month, test.hex)
    
    h2o.shutdown(prompt = F)
    
    pred.1 <- (.5 * pred.1) + (.5 * pred.2)
  }
  
  computeLift(pred.1, dt.test$CLASS==1)
  
  pred <- prediction(pred.1, dt.test$CLASS)
  
  dt.test[, PRED := pred.1]
  
  auc  <- performance(pred, "auc")
  auc <- unlist(slot(auc, "y.values"))
  cat("AUC: ", auc, "\n")
  perf <- performance(pred, "tpr", "fpr")
  
  #ofile = paste0(models.folder, "/", month, "/", model, ".auc.", month, ".pred.", test.month, ".", auc, 
                 #as.character(format(Sys.time(), "%d-%m-%Y-%H:%M:%S")), 
  #               ".png")
  #png(filename = ofile)
  #plot(perf, main = paste0("Model: ", month, " test ", test.month, " AUC ", round(auc, digits = 4)))
  
  #dev.off()
  
  ofile <- paste0(predictions.folder, model.type, ".", month, ".pred.", car.month, ".", auc, ".", 
                  as.character(format(Sys.time(), "%d-%m-%Y-%H:%M:%S")), 
                  ".csv")
  cat("[SAVE] ", ofile, "\n")
  write.table(dt.test[, c("MSISDN", "PRED", "CLASS"), with = F], file = ofile, sep = "|", quote = F, row.names = F)
}

#---------------------------------------------------------------------------------------------------------
option_list <- list(
  make_option(c("-mm", "--model_month"), type = "character", default = NULL, help = "input month (YYYYMM)", 
              metavar = "character"),
  make_option(c("-dm", "--data_month"), type = "character", default = NULL, help = "input month (YYYYMM)", 
              metavar = "character"),
  make_option(c("-tm", "--test_month"), type = "character", default = NULL, help = "input month (YYYYMM)", 
              metavar = "character"),
  make_option(c("-model", "--model_type"), type = "character", default = NULL, help = "input month (YYYYMM)", 
              metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

if (is.null(opt$month)) {
  print_help(opt_parser)
  stop("At least one parameter must be supplied (input month: YYYYMM)", call.=FALSE)
} else {
  main(opt$model_month, opt$data_month, opt$test_month, opt$model_type)
}
