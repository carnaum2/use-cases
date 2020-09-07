source("configuration.R")
source("clean_data.R")

# It reads the convergidos customers and the CAR files and generate a (balanced) trained data set for a specific date.
# In order, to generate the training date calls the CleanData() function from clean_data.R file.

main <- function(date) {
  
  ifile <- paste0(datasets.folder, "/", date, "/dt.convergidos.", date, "_EBU_CIF.RData")
  cat("[LOAD] ", ifile, "\n")
  load(ifile)
  
  ifile <- paste0(data.folder, "/ACC/", date, "/dt_EMP_EXPLICATIVAS_CLI_", date, ".RData")
  cat("[LOAD] ", ifile, "\n")
  load(ifile)
  
  # Filter out big companies and got only freelance and SOHO
  dt.CAR.EMP <- dt.CAR.EMP[SEG_SEGMENTO_MERCADO == "A" | SEG_SEGMENTO_MERCADO == "S"]
  dt.train.pos <- dt.CAR.EMP[NIF %in% dt.convergidos$CIF_NIF_PPAL]
  dt.train.neg <- dt.CAR.EMP[!NIF %in% dt.convergidos$CIF_NIF_PPAL]
  
  # Balance training set
  dt.train.neg.bal <- dt.train.neg[sample(1:nrow(dt.train.neg), nrow(dt.train.pos))]
  
  dt.train.pos[, CLASS := 1]
  dt.train.neg.bal[, CLASS := 0]
  dt.train <- rbind(dt.train.pos, dt.train.neg.bal)
  
  dt.train <- CleanData(dt.train)
  
  ofolder <- paste0(datasets.folder, "/", date)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- paste0(datasets.folder, "/", date, "/dt.train-", date, "_EBU.RData")
  cat("[SAVE] ", ofile, " with ", nrow(dt.train), " records \n")
  save(dt.train, file = ofile)
}

#---------------------------------------------------------
option_list <- list(
    make_option(c("-d", "--date"), type = "character", default = NULL, help = "input date [YYYYMM|YYYYMMDD]", 
                metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# TODO parameter check
if (is.null(opt$date)) {
  print_help(opt_parser)
  stop("At least one parameter must be supplied (input date: [YYYYMM|YYYYMMDD])", call.=FALSE)
} else {
  main(opt$date)
}
