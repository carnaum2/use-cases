source("configuration.R")

# This script will be used to generate a set of predictions for the current Mobile Only Customers. The input file must be cleaned
# beforehand using the script 04_clean_and_prepare_predict_set.R

predictH2ORF <- function(model.date, predict.hex) {
  ifile <- paste0(models.folder, "/", model.date, "/rf.model.h2o.EBU/h2orf")
  cat("[LOAD] ", ifile, "\n")
  h2o.rf <- h2o.loadModel(ifile)
  
  cat("[INFO] Predicting...\n")
  pred.1 <- h2o.predict(object = h2o.rf, newdata = predict.hex)
  pred.1 <- as.data.frame(pred.1)
  
  return(pred.1)
}

main <- function(model.date, predict.date, model) {
  ifile <- paste0(datasets.folder, "/", predict.date, "/dt.predict-", predict.date, "_EBU.RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  
  if (model == "rf") {
    
  } else if (model == "xg") {
    
  } else if (model == "h2orf") {
    localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
    h2o.removeAll()
    cat("[INFO] Loading Data into H2O Cluster...\n")
    predict.hex <- as.h2o(dt.predict, "predict.hex")
    
    pred.1  <- predictH2ORF(model.date, predict.hex)
    h2o.shutdown(prompt = F)
    
  } else if (model == "h2ogbm") {
    localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
    h2o.removeAll()
    cat("[INFO] Loading Data into H2O Cluster...\n")
    predict.hex <- as.h2o(dt.predict, "predict.hex")
    
    pred.1  <- predictH2OGBM(model.date, predict.hex)
    h2o.shutdown(prompt = F)
  }
  
  pred.1 <- as.data.table(pred.1)
  pred.1[, predict := NULL]
  pred.1[, p0 := NULL]
  setnames(pred.1, "SCORE_CONV")
  pred.1[, NO_PROP := 1 - SCORE_CONV]
  setkey(pred.1, NO_PROP)
  pred.1[, CIF_NIF := dt.predict$NIF]
  pred.1[, NO_PROP := NULL]
  setcolorder(pred.1, c("CIF_NIF", "SCORE_CONV"))
  pred.1[, SCORE_CONV := round(SCORE_CONV, digits = 2)]

  ofolder <- paste0(predictions.folder, "/")
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <-paste0(predictions.folder, "pred.ebu.conv.", predict.date, ".csv")
  cat("[SAVE] ", ofile, "\n")
  write.table(pred.1, file = ofile, sep = "|", quote = F, row.names = F)
}
#--------------------------------------------------------------------------------------------------
option_list <- list(
  make_option(c("-md", "--model_date"), type = "character", default = NULL, help = "model date [YYYYMM|YYYYMMDD]", 
              metavar = "character"),
  make_option(c("-pd", "--predict_date"), type = "character", default = NULL, help = "predict date [YYYYMM|YYYYMMDD]", 
              metavar = "character"),
  make_option(c("-model", "--model"), type = "character", default = NULL, help = "model type", 
              metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# TODO parameter check
if (is.null(opt$model_date)) {
  print_help(opt_parser)
  stop("At least three parameters must be supplied", call.=FALSE)
} else {
  main(opt$model_date, opt$predict_date, opt$model)
}