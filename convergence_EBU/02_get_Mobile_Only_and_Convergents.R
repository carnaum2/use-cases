source("configuration.R")

# It require the current Voice price plans to obtain those plans referred to voice only. With this file and 
# dt_AC_EMP_CARTERA_SERVICIO and dt_AC_EMP_CARTERA_CLIENTE obtains the customers that are Mobile Only and Convergent
# in the specific date. Date can be YYYYMM or YYYYMMDD

main <- function(date) {
  # Load voice Price Plans
  ifile <- paste0(input.folder.cvm, "planes_voz_BI.txt")
  cat("[LOAD] ", ifile, "\n")
  dt.planes.voz <- fread(ifile)

  ifile <- paste0(data.folder,"/CVM/", date, "/dt_AC_EMP_CARTERA_SERVICIO_", date, ".RData")
  cat("[LOAD] ", ifile, "\n")
  load(ifile)

  dt.AtLeastVoice <- dt_AC_EMP_CARTERA_SERVICIO[PP_VOZ %in% dt.planes.voz$PLAN_GSM]
  dt.AtLeastVoice <- dt.AtLeastVoice[ ,.(CIF_NIF, CUENTA, MSISDN)]
  setkey(dt.AtLeastVoice, CIF_NIF)
  dt.AtLeastVoice <- unique(dt.AtLeastVoice)
  rm(dt_AC_EMP_CARTERA_SERVICIO)
  
  ifile <- paste0(data.folder,"/CVM/", date, "/dt_AC_EMP_CARTERA_CLIENTE_", date, ".RData")
  cat("[LOAD] ", ifile, "\n")
  load(ifile)
  
  setkey(dt_AC_EMP_CARTERA_CLIENTE, CIF_NIF)
  cat("\nTotal number of Customers: ", nrow(dt_AC_EMP_CARTERA_CLIENTE), "\n")
  
  dt.mobile.only <- dt_AC_EMP_CARTERA_CLIENTE[CIF_NIF_PPAL %in% dt.AtLeastVoice$CIF_NIF]
  dt.mobile.only <- dt.mobile.only[NUM_ADSL == 0 & NUM_FIBRA == 0]
  cat("Mobile Only Customers:", nrow(dt.mobile.only), " ", nrow(dt.mobile.only)/nrow(dt_AC_EMP_CARTERA_CLIENTE), "\n")
  
  ofolder <- paste0(datasets.folder, "/", date)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  ofile <- paste0(datasets.folder, "/", date, "/dt.mobile.only.", date, "_EBU.RData")
  cat("[SAVE]", ofile, "\n")
  save(dt.mobile.only, file = ofile)
  
  dt.convergentes <- dt_AC_EMP_CARTERA_CLIENTE[CIF_NIF_PPAL %in% dt.AtLeastVoice$CIF_NIF]
  dt.convergentes <- dt.convergentes[NUM_ADSL > 0 | NUM_FIBRA > 0]
  cat("Total number of convergent customers: ", nrow(dt.convergentes), " " ,  nrow(dt.convergentes)/nrow(dt_AC_EMP_CARTERA_CLIENTE), "\n")
  
  ofile <- paste0(datasets.folder, "/", date, "/dt.convergentes.", date, "_EBU.RData")
  cat("[SAVE]", ofile, "\n")
  save(dt.convergentes, file = ofile)
}
#---------------
option_list <- list(
  make_option(c("-d", "--date"), type = "character", default = NULL, help = "input date [YYYYMM|YYYYMMDD]", 
              metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# TODO parameter check
if (is.null(opt$date)) {
  print_help(opt_parser)
  stop("At least one parameter must be supplied (input date: YYYYMM)", call.=FALSE)
} else {
  main(opt$date)
}
