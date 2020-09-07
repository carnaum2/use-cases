source("configuration.R")

# It trains a machine learning model from a specific training dataset of a particular date.
# Currently it supports the following models: Xgboost, H2o Random Forest, H2O GBM and Random Forest from R package.
# The number of trees is an input parameter. The script can also get a validation input file from a specifc date.
# It store the generated model, the variable importance and the error evolution.

trainH2ORF <- function(date, trees, train.hex, class.pos) {

  h2o.rf <- h2o.randomForest(training_frame = train.hex, x = 1:(class.pos-1), y = class.pos, 
                             ntrees = trees, seed = 1234, model_id = "h2orf", binomial_double_trees = T 
                             #, nfolds = 4
                             )
  
  print(h2o.rf)

  ofolder <- paste0(models.folder, "/", date)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- paste0(ofolder, "/varImpPlot.H2ORF-EBU-", date, ".png")
  png(filename = ofile)
  h2o.varimp_plot(h2o.rf, num_of_features = 30)
  dev.off()
  
  ofile <- paste0(ofolder, "/rf.error.h2o-EBU-", date, ".png")
  png(filename = ofile)
  plot(h2o.rf, main = paste0("Error evolution for model in date ", date))
  dev.off()
  
  ofile <- paste0(ofolder, "/rf.model.h2o.EBU")
  cat("[SAVE]", ofile, "\n")
  
  local.path <- h2o.saveModel(h2o.rf, path = ofile, force = T)
}

main <- function(date, trees, model, valid.date) {
  set.seed(1234)
  
  ifile <- paste0(datasets.folder, "/", date, "/dt.train-", date, "_EBU.RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  dt.train.NIFS <- dt.train[["NIF"]]
  dt.train[, NIF := NULL]
  
  dt.train[, CLASS := as.factor(CLASS)]
  
  cat("[INFO] Training Model on ", nrow(dt.train), " rows ", "\n")
  if (model == "rf") {
    trainRF(date, trees)
    
  } else if (model == "xg") {
    trainXG(date, trees)
    
  } else if (model == "h2orf") {
    localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
    h2o.removeAll()
    train.hex <- as.h2o(dt.train, "train.hex")
    
    trainH2ORF(date, trees, train.hex, ncol(dt.train))
    
    h2o.shutdown(prompt = F)
    
  } else if (model == "h2ogbm") {
    trainH2OGBM(date, trees)
  }
}

#--------------------------------------------------------------------------------------------------
option_list <- list(
  make_option(c("-d", "--date"), type = "character", default = NULL, help = "input date [YYYYMM|YYYYMMDD]", 
              metavar = "character"),
  make_option(c("-t", "--trees"), type = "numeric", default = NULL, help = "number of trees", 
              metavar = "numeric"),
  make_option(c("-model", "--model"), type = "character", default = NULL, help = "model", 
              metavar = "character"),
  make_option(c("-valid", "--valid"), type = "character", default = NULL, help = "Validation Date", 
              metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# TODO parameter check
if (is.null(opt$date)) {
  print_help(opt_parser)
  stop("At least four parameters must be supplied", call.=FALSE)
} else {
  main(opt$date, opt$trees, opt$model, opt$valid)
}
