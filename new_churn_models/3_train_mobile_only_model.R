source("configuration.R")

generateValid <- function(dt.valid) {
  remove.cols <- c("NIF", "MSISDN",       
                   "NUM_FACT_SIN_SMS_M0"  , "SEG_NACIONALIDAD",
                   "NUM_FACT_INTERNAC_M0",  "NUM_ACCESOS_3G_M0", "NUM_DATOS_3G_M0"      
  )
  
  dt.valid <- dt.valid[, !remove.cols, with = F]
  
  dt.valid[, SEG_TIPO_IDENT := as.factor(SEG_TIPO_IDENT)]
  dt.valid[, SEG_DESCUENTO_MAX := as.factor(SEG_DESCUENTO_MAX)]
  dt.valid[, SEG_SISTEMA_OPERATIVO:= as.factor(SEG_SISTEMA_OPERATIVO)]
  dt.valid[, SEG_TIPO_HOGAR_HH := as.factor(SEG_TIPO_HOGAR_HH)]
  dt.valid[, SEG_CICLO_VIDA := as.factor(SEG_CICLO_VIDA)]
  #dt.valid[, SEG_NACIONALIDAD := as.factor(SEG_NACIONALIDAD)]
  dt.valid[, SEG_SEXO := as.factor(SEG_SEXO)]
}

generateTrain <- function(dt.train) {
  remove.cols <- c("NIF", "MSISDN",       
                    "NUM_FACT_SIN_SMS_M0", "SEG_NACIONALIDAD",
                    "NUM_FACT_INTERNAC_M0", "NUM_ACCESOS_3G_M0", "NUM_DATOS_3G_M0"      
                   )
  
  dt.train <- dt.train[, !remove.cols, with = F]
  
  dt.train.pos <- dt.train[CLASS == 1]
  dt.train.neg <- dt.train[CLASS == 0]
  dt.train.neg.bal <- dt.train.neg[sample(1:nrow(dt.train.neg), nrow(dt.train.pos))]
  dt.train <- rbind(dt.train.pos, dt.train.neg.bal)
  
  dt.train[, SEG_TIPO_IDENT := as.factor(SEG_TIPO_IDENT)]
  dt.train[, SEG_DESCUENTO_MAX := as.factor(SEG_DESCUENTO_MAX)]
  dt.train[, SEG_SISTEMA_OPERATIVO:= as.factor(SEG_SISTEMA_OPERATIVO)]
  #dt.train[, SEG_TIPO_HOGAR_HH := as.factor(SEG_TIPO_HOGAR_HH)]
  dt.train[, SEG_CICLO_VIDA := as.factor(SEG_CICLO_VIDA)]
  #dt.train[, SEG_NACIONALIDAD := as.factor(SEG_NACIONALIDAD)]
  dt.train[, SEG_SEXO := as.factor(SEG_SEXO)]
  
return(dt.train)
}


trainRF <- function(car.month, sol.month, trees) {
  ifile <- paste0("/Users/fcastanf/Desktop/CVM-Fede/datasets/dt.train.mo.", car.month, ".", sol.month,".RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
 
  dt.train <- generateTrain(dt.train)
  
  ofolder <- paste0(models.folder, "/", car.month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- paste0(ofolder, "/rf.model.caret-" ,car.month, ".RData")
  cat("[SAVE] ", ofile, "\n")
  save(rf.combined, file = ofile)
}

trainXG <- function(month, trees, valid.month) {
  dt.train <- generateTrain(month) # TODO. avoid reload input file each time
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
    dt.test <- generateTrain(valid.month) # TODO. avoid reload input file each time
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
  
  ofolder <- paste0(models.folder, "/", month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- paste0(ofolder, "/xgb.model-", month, ".RData")
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

trainH2ORF <- function(car.month, sol.month, trees, valid.month) {
  ifile <- paste0(datasets.folder, "dt.train.mo.", car.month, ".", sol.month,".RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  
  dt.train <- generateTrain(dt.train)
  dt.train[, CLASS := as.factor(CLASS)]

  if (valid.month != "none") {
    ifile <- paste0(datasets.folder, valid.month, "/dt.valid.preven.", valid.month,".RData")
    cat("[LOAD]", ifile, "\n")
    load(ifile)
    
    dt.valid <- generateValid(dt.valid)
    dt.valid[, CLASS := as.factor(CLASS)]
  }
  cat("Starting H2O...\n")
  localH2O = h2o.init(nthreads = -1, max_mem_size = "10G", startH2O = T)
  h2o.removeAll()
  train.hex <- as.h2o(dt.train, "train.hex")
  if (valid.month != "none") {
    valid.hex <- as.h2o(dt.valid, "valid.hex")
    h2orf <- h2o.randomForest(training_frame = train.hex, x = 1:(ncol(dt.train)-1), y = ncol(dt.train), 
                             ntrees = trees, seed = 1234, validation_frame = valid.hex, score_each_iteration = F,
                             stopping_rounds = 5, model_id = "h2orf")
  } else {
    h2orf <- h2o.randomForest(training_frame = train.hex, x = 1:(ncol(dt.train)-1), y = ncol(dt.train),
                              ntrees = trees, seed = 1234, model_id = "h2orf",
                              col_sample_rate_per_tree = 0.55,
                              max_depth = 19,
                              sample_rate = 0.9,
                              #stopping_rounds = 10,
                              #stopping_metric = "AUC",
                              #nfolds = 5,
                              #keep_cross_validation_predictions = T,
                              #fold_assignment = "Modulo",
                              binomial_double_trees = T)
  }
  
  print(h2orf)
  if (valid.month != "none") {
    print(h2o.gainsLift(h2orf), valid = T)
  } else {
    print(h2o.gainsLift(h2orf))
  }
  
  ofolder <- paste0(models.folder, "/", car.month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- paste0(ofolder, "/varImpPlot.H2ORF-", car.month, ".", sol.month, ".png")
  png(filename = ofile)
  h2o.varimp_plot(h2orf, num_of_features = 30)
  dev.off()
  
  ofile <- paste0(ofolder, "/rf.error.h2o-", car.month, ".", sol.month, ".png")
  png(filename = ofile)
  plot(h2orf)
  dev.off()
  
  ofile <- paste0(ofolder, "/rf.model.h2o")
  cat("[SAVE] ", ofile, "\n")
  
  local.path <- h2o.saveModel(h2orf, path = ofile, force = T)
  h2o.shutdown(prompt = F)
}

trainH2OGBM <- function(car.month, sol.month, trees, valid.month) {
  ifile <- paste0(churn.datasets.folder, "dt.train.mo.", car.month, ".", sol.month,".RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  
  dt.train <- generateTrain(dt.train)
  dt.train[, CLASS := as.factor(CLASS)]
  
  if (valid.month != "none") {
    ifile <- paste0(datasets.folder, "dt.test.mo.", valid.month, ".201703.RData")
    cat("[LOAD]", ifile, "\n")
    load(ifile)
    dt.valid <- generateValid(dt.test)
    dt.valid[, CLASS := as.factor(CLASS)]
  }
  
  cat("Starting H2O...\n")
  localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
  h2o.removeAll()
  train.hex <- as.h2o(dt.train, "train.hex")
  if (valid.month != "none") {
    cat("[INFO] Loading Validation Data\n")
    valid.hex <- as.h2o(dt.valid, "valid.hex")
    h2ogbm <- h2o.gbm(training_frame = train.hex, x = 1:(ncol(dt.train)-1), y = ncol(dt.train), 
                      ntrees = trees, seed = 1234, validation_frame = valid.hex, score_each_iteration = T,
                      stopping_rounds = 5, model_id = "h2ogbm",
                      sample_rate = 0.7, 
                      col_sample_rate = 0.7)
  } else if (valid.month == "grid") {
    
    ntrees_opt <- 100000 #seq(50, 150, 10) #c(1000)
    learn_rate_opt <- 0.001 #seq(0.01, 0.1, 0.02)
    max_depth_opt <- seq(1,20)
    min_rows_opt <- c(1,5,10,20,50,100)
    
    sample_rate_opt <-  seq(0.3, 1.0, 0.05)
    col_sample_rate_opt <-  seq(0.3, 1.0, 0.05)
    #col_sample_rate_per_tree_opt = seq(0.3,1,0.05)
    
    hyper_params <- list(learn_rate = learn_rate_opt,
                         max_depth = max_depth_opt,
                         sample_rate = sample_rate_opt,
                         col_sample_rate = col_sample_rate_opt,
                         ntrees = ntrees_opt)
    #  col_sample_rate_per_tree = col_sample_rate_per_tree_opt)
    
    search_criteria <- list(strategy = "RandomDiscrete",
                            max_runtime_secs = 3600, max_models = 1000, 
                            stopping_metric = "AUC", 
                            stopping_tolerance = 0.0001, 
                            stopping_rounds = 10, 
                            seed = 1)
    
    gbm_grid <- h2o.grid(algorithm = "gbm",
                         grid_id = "my_gbm_grid",
                         x = 1:(ncol(dt.train)-1), y = ncol(dt.train),
                         training_frame = train.hex,
                         seed = 1,
                         nfolds = 5,
                         #distribution="gaussian", 
                         fold_assignment = "Modulo",
                         keep_cross_validation_predictions = TRUE,
                         hyper_params = hyper_params,
                         search_criteria = search_criteria)
    
    gbm.sorted.grid <- h2o.getGrid(grid_id = "my_gbm_grid", sort_by = "AUC")
    print(gbm.sorted.grid)
    
  }else {
    h2ogbm <- h2o.gbm(training_frame = train.hex, x = 1:(ncol(dt.train)-1), y = ncol(dt.train), 
                      ntrees = trees, seed = 1234, model_id = "h2ogbm",
                      #max_depth = 10,
                      sample_rate = 0.75, 
                      col_sample_rate = 0.45,
                      max_depth = 7,
                      learn_rate = 0.01
                      #stopping_rounds = 5,
                      #stopping_metric = "AUC",
                      #nfolds = 5,
                      #keep_cross_validation_predictions = T,
                      #fold_assignment = "Modulo"
                      #learn_rate = 0.3
    )
  }
  
  print(h2ogbm)
  if (valid.month != "none") {
    print(h2o.gainsLift(h2ogbm), valid = T)
  } else {
    print(h2o.gainsLift(h2ogbm))
  }
  
  ofolder <- paste0(models.folder, "/", car.month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- paste0(ofolder, "/varImpPlot.H2OGBM-MO-", car.month, ".", sol.month, ".png")
  png(filename = ofile)
  h2o.varimp_plot(h2ogbm, num_of_features = 30)
  dev.off()
  
  ofile <- paste0(ofolder, "/gbm.error.h2o-MO-", car.month, ".", sol.month, ".png")
  png(filename = ofile)
  plot(h2ogbm)
  dev.off()
  
  ofile <- paste0(ofolder, "/gbm.model.h2o.mo")
  cat("[SAVE] ", ofile, "\n")
  
  local.path <- h2o.saveModel(h2ogbm, path = ofile, force = T)
  h2o.shutdown(prompt = F)
}

trainH2OGLM <- function(car.month, sol.month, trees, valid.month) {
  ifile <- paste0(datasets.folder, "dt.train.mo.", car.month, ".", sol.month,".RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  
  dt.train <- generateTrain(dt.train)
  dt.train <- dt.train[, !c("SEG_TIPO_IDENT", "SEG_DESCUENTO_MAX", "SEG_SISTEMA_OPERATIVO", "SEG_TIPO_HOGAR_HH",
                            "SEG_CICLO_VIDA", "SEG_SEXO"), with = F]
  dt.train[, CLASS := as.factor(CLASS)]
  
  if (valid.month != "none") {
    ifile <- paste0(datasets.folder, valid.month, "/dt.valid.preven.", valid.month,".RData")
    cat("[LOAD]", ifile, "\n")
    load(ifile)
    
    dt.valid <- generateValid(dt.valid)
    dt.valid <- dt.valid[, !c("SEG_TIPO_IDENT", "SEG_DESCUENTO_MAX", "SEG_SISTEMA_OPERATIVO", "SEG_TIPO_HOGAR_HH",
                              "SEG_CICLO_VIDA", "SEG_SEXO"), with = F]
    
    dt.valid[, CLASS := as.factor(CLASS)]
  }
  cat("Starting H2O...\n")
  localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
  h2o.removeAll()
  train.hex <- as.h2o(dt.train, "train.hex")
  if (valid.month != "none") {
    valid.hex <- as.h2o(dt.valid, "valid.hex")
    h2oglm <- h2o.glm(training_frame = train.hex, x = 1:(ncol(dt.train)-1), y = ncol(dt.train), 
                      family="binomial",standardize=T,
                      lambda_search=T)
  } else {
    h2oglm <- h2o.glm(training_frame = train.hex, x = 1:(ncol(dt.train)-1), y = ncol(dt.train), 
                      family="binomial",standardize=T, 
                      lambda_search=T, remove_collinear_columns = T
    )
  }
  
  print(h2oglm)
  if (valid.month != "none") {
    print(h2o.gainsLift(h2oglm), valid = T)
  } else {
    print(h2o.gainsLift(h2oglm))
  }
  
  ofolder <- paste0(models.folder, "/", car.month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- paste0(ofolder, "/varImpPlot.H2OGLM-", car.month, ".", sol.month, ".png")
  png(filename = ofile)
  h2o.varimp_plot(h2oglm, num_of_features = 30)
  dev.off()
  
  ofile <- paste0(ofolder, "/glm.error.h2o-", car.month, ".", sol.month, ".png")
  png(filename = ofile)
  plot(h2oglm)
  dev.off()
  
  ofile <- paste0(ofolder, "/gbm.model.h2o")
  cat("[SAVE] ", ofile, "\n")
  
  local.path <- h2o.saveModel(h2oglm, path = ofile, force = T)
  h2o.shutdown(prompt = F)
}

computeEnsemble <- function(car.month, sol.month, models.month) {
  cat("Starting H2O...\n")
  localH2O = h2o.init(nthreads = -1, max_mem_size = "10G")
  h2o.removeAll()
  
  #ifile <- paste0(models.folder, "/", models.month, "/rf.model.h2o/h2orf")
  #cat("[LOAD] ", ifile, "\n")
  #h2orf <- h2o.loadModel(ifile)
  
  #ifile <- paste0(models.folder, "/", models.month, "/gbm.model.h2o/h2ogbm")
  #cat("[LOAD] ", ifile, "\n")
  #h2ogbm <- h2o.loadModel(ifile)
  
  ifile <- paste0(datasets.folder, "dt.train.mo.", car.month, ".", sol.month,".RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  
  dt.train <- generateTrain(dt.train)
  dt.train[, CLASS := as.factor(CLASS)]
  train.hex <- as.h2o(dt.train, "train.hex")
  
  h2orf <- h2o.randomForest(training_frame = train.hex, x = 1:(ncol(dt.train)-1), y = ncol(dt.train),
                            ntrees = 422, seed = 1234, model_id = "h2orf",
                            col_sample_rate_per_tree = 0.55,
                            max_depth = 19,
                            sample_rate = 0.9,
                            nfolds = 5,
                            keep_cross_validation_predictions = T,
                            fold_assignment = "Modulo",
                            binomial_double_trees = T)
  
  h2ogbm <- h2o.gbm(training_frame = train.hex, x = 1:(ncol(dt.train)-1), y = ncol(dt.train), 
                    ntrees = 1000, seed = 1234, model_id = "h2ogbm",
                    sample_rate = 0.75, 
                    col_sample_rate = 0.45,
                    max_depth = 7,
                    learn_rate = 0.01,
                    nfolds = 5,
                    keep_cross_validation_predictions = T,
                    fold_assignment = "Modulo"
  )
  ensemble <- h2o.stackedEnsemble(training_frame = train.hex, x = 1:(ncol(dt.train)-1), y = ncol(dt.train),
                                  model_id = "churn_ensemble_",
                                  base_models = list(h2orf@model_id, h2ogbm@model_id))
  
  
return(ensemble)  
  
}

main <- function(car.month, sol.month, trees, model, valid.month) {
  start.time <- Sys.time()
  set.seed(1234)
  
  if (model == "rf") {
    trainRF(car.month, sol.month, trees)
    
  } else if (model == "xg") {
    trainXG(car.month, sol.month, trees, valid.month)
    
  } else if (model == "h2orf") {
    trainH2ORF(car.month, sol.month, trees, valid.month)
    
  } else if (model == "h2ogbm") {
    trainH2OGBM(car.month, sol.month, trees, valid.month)
    
  } else if (model == "h2oglm") {
    trainH2OGLM(car.month, sol.month, trees, valid.month)
  }
  end.time <- Sys.time()
  time.taken <- difftime(end.time,  start.time, units = "secs")
  cat("[INFO] It took ", time.taken, " secs \n")
  
}

#--------------------------------------------------------------------------------------------------
option_list <- list(
  make_option(c("-cm", "--carmonth"), type = "character", default = NULL, help = "input month (YYYYMM)", 
              metavar = "character"),
  make_option(c("-sm", "--solmonth"), type = "character", default = NULL, help = "input month (YYYYMM)", 
              metavar = "character"),
  make_option(c("-t", "--trees"), type = "numeric", default = NULL, help = "number of trees", 
              metavar = "numeric"),
  make_option(c("-model", "--model"), type = "character", default = NULL, help = "model", 
              metavar = "character"),
  make_option(c("-valid", "--valid"), type = "character", default = NULL, help = "Validation Month", 
              metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

if (is.null(opt$carmonth)) {
  print_help(opt_parser)
  stop("At least one parameter must be supplied (input month: YYYYMM)", call.=FALSE)
} else {
  main(opt$carmonth, opt$solmonth, opt$trees, opt$model, opt$valid)
}

# main("201706", "201708", 500, "h2ogbm", "none")
#main("201707", "201709", 500, "h2ogbm", "none")
