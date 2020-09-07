source("configuration.R")
source("clean_data.R")

# It reads the convergidos customers and the CAR files and generates a test set (unbaled) useful to evaluate the trained model.
# It also employs the CleanData() function.

main <- function(date) {
  
  ifile <- paste0(datasets.folder, "/", date, "/dt.convergidos.", date, "_EBU_CIF.RData")
  cat("[LOAD] ", ifile, "\n")
  load(ifile)
  
  ifile <- paste0(data.folder, "/ACC/", date, "/dt_EMP_EXPLICATIVAS_CLI_", date, ".RData")
  cat("[LOAD] ", ifile, "\n")
  load(ifile)
  
  # Filter out big companies and got only freelance and SOHO
  dt.CAR.EMP <- dt.CAR.EMP[SEG_SEGMENTO_MERCADO == "A" | SEG_SEGMENTO_MERCADO == "S"]
  dt.test.pos <- dt.CAR.EMP[NIF %in% dt.convergidos$CIF_NIF_PPAL]
  dt.test.neg <- dt.CAR.EMP[!NIF %in% dt.convergidos$CIF_NIF_PPAL]
  
  dt.test.pos[, CLASS := 1]
  dt.test.neg[, CLASS := 0]
  dt.test <- rbind(dt.test.pos, dt.test.neg)
  
  dt.test <- CleanData(dt.test)
  
  ofolder <- paste0(datsets.folder, "/", date)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- paste0(datasets.folder, "/", date, "/dt.test-", date, "_EBU.RData")
  cat("[SAVE]", ofile, " with ", nrow(dt.test), " records \n")
  save(dt.test, file = ofile)
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
