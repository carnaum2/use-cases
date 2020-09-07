source("configuration.R")

# Evaluate a trained model using a specific input test file. This input test must have a known target variable (groundtruth).
# It outputs the GainsLift and the predictions with the obtained AUC.

evaluateH2ORF <- function(model.date, test.hex) {
  ifile <- paste0(models.folder, "/", model.date, "/rf.model.h2o.EBU/h2orf")
  cat("[LOAD] ", ifile, "\n")
  
  h2o.rf <- h2o.loadModel(ifile)
  
  cat("[INFO] Predicting...\n")
  pred.1 <- h2o.predict(object = h2o.rf, newdata = test.hex)
  print(h2o.gainsLift(h2o.rf, valid = TRUE))
  print(h2o.gainsLift(h2o.rf, newdata = test.hex))
  pred.1 <- as.data.frame(pred.1)
  
  return(pred.1[,3])
}

main <- function(model.date, test.date, model.type) {
  ifile <- paste0(datasets.folder, "/", test.date, "/dt.test-", test.date, "_EBU.RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  
  if (model == "rf") {
    
  } else if (model == "xg") {
    
  } else if (model == "h2orf") {
    localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
    h2o.removeAll()
    cat("[INFO] Loading Data into H2O Cluster...\n")
    test.hex <- as.h2o(dt.test, "test.hex")
    
    pred.1  <- evaluateH2ORF(model.date, test.hex)
    h2o.shutdown(prompt = F)
    
  } else if (model == "h2ogbm") {
    localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
    h2o.removeAll()
    cat("[INFO] Loading Data into H2O Cluster...\n")
    test.hex <- as.h2o(dt.test, "test.hex")
    
    pred.1  <- evaluateH2OGBM(model.date, test.hex)
    h2o.shutdown(prompt = F)
  }
  
  pred <- prediction(pred.1, dt.test$CLASS)
  
  dt.test[, PRED := pred.1]
  
  auc  <- performance(pred, "auc")
  auc <- unlist(slot(auc, "y.values"))
  cat("AUC: ", auc, "\n")
  perf <- performance(pred, "tpr", "fpr")
  
  ofolder <- paste0(predictions.folder, "/")
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile = paste0(models.folder, "/", model.date, "/", model, ".auc.", model.date, ".pred.", test.date, ".", auc, 
                 #as.character(format(Sys.time(), "%d-%m-%Y-%H:%M:%S")), 
                 ".EBU.png")
  png(filename = ofile)
  plot(perf, main = paste0("Model: ", model.date, " test ", test.date, " AUC ", round(auc, digits = 4)))
  dev.off()
  
  ofile <- paste0(predictions.folder, model.date, ".", model.date, ".pred.", test.date, ".", auc, 
                  #as.character(format(Sys.time(), "%d-%m-%Y-%H:%M:%S")), 
                  ".EBU.csv")
  cat("[SAVE] ", ofile, "\n")
  write.table(dt.test[, c("NIF", "PRED", "CLASS"), with = F], file = ofile, sep = "|", quote = F, row.names = F)
}

#--------------------------------------------------------------------------------------------------
option_list <- list(
  make_option(c("-md", "--model_date"), type = "character", default = NULL, help = "model date [YYYYMM|YYYYMMDD]", 
              metavar = "character"),
  make_option(c("-td", "--test_date"), type = "character", default = NULL, help = "test date [YYYYMM|YYYYMMDD]", 
              metavar = "character"),
  make_option(c("-model", "--model_type"), type = "character", default = NULL, help = "model type", 
              metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# TODO parameter check
if (is.null(opt$model_date)) {
  print_help(opt_parser)
  stop("At least three parameters must be supplied: model_date test_date model_type", call.=FALSE)
} else {
  main(opt$model_date, opt$test_date, opt$model_type)
}