source("configuration.R")
source("clean_data.R")

# It prepare the input file to be fed into a trained model. 
# It reads the Mobile Only Customers for a specific date, clean the variables by using the CleanData() function 
# and filter out those customers that are in the process of acquiring DSL/Fiber (provisionados).

main <- function(date, car.date, prov.date) {
  
  ifile <- paste0(datasets.folder, "/", date, "/dt.mobile.only.", date, "_EBU.RData")
  cat("[LOAD] ", ifile, "\n")
  load(ifile)
  cat("[INFO] Mobile Only Customers: ", nrow(dt.mobile.only), "\n")
  
  ifile <- paste0(data.folder, "/ACC/", car.date, "/dt_EMP_EXPLICATIVAS_CLI_", car.date, ".RData")
  cat("[LOAD] ", ifile, "\n")
  load(ifile)
  
  # Filter out big companies and got only freelance and SOHO
  dt.CAR.EMP <- dt.CAR.EMP[SEG_SEGMENTO_MERCADO == "A" | SEG_SEGMENTO_MERCADO == "S"]
  dt.predict <- dt.CAR.EMP[NIF %in% dt.mobile.only$CIF_NIF_PPAL]
  
  dt.predict <- CleanData(dt.predict)
  setkey(dt.predict, NIF)
  dt.predict <- unique(dt.predict)
  
  # Filter out those customers under provision or already installed in the meantime
  ifile <- paste0(input.folder.prov, "/Provisionadas_", prov.date, ".txt")
  cat("[LOAD] ", ifile, "\n")
  dt.prov <- fread(ifile)
  
  cat("[INFO] Before Filtering Provisionadas" , nrow(dt.predict), " records \n")
  dt.predict <- dt.predict[!NIF %in% dt.prov$CIF_NIF]
  
  ofolder <- paste0(datasets.folder, "/", date)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- paste0(datasets.folder, "/", date, "/dt.predict-", date, "_EBU.RData")
  cat("[SAVE]", ofile, " with ", nrow(dt.predict), " records \n")
  save(dt.predict, file = ofile)
}

#---------------------------------------------------------
option_list <- list(
   make_option(c("-d", "--date"), type = "character", default = NULL, help = "input date [YYYYMM|YYYYMMDD]", 
                metavar = "character"),
   make_option(c("-cd", "--car_date"), type = "character", default = NULL, help = "car date [YYYYMM]", 
               metavar = "character"),
   make_option(c("-pd", "--prov_date"), type = "character", default = NULL, help = "provision date [YYYYMM|YYYYMMDD]", 
               metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# TODO parameter check
if (is.null(opt$date)) {
  print_help(opt_parser)
  stop("At least two parameter must be supplied date, car_date  and prov_date", call.=FALSE)
} else {
  main(opt$date, opt$car_date, opt$prov_date)
}
