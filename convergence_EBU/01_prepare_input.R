source("configuration.R")
#
# Read the input files in txt/csv format and store them in RData format.
# It gets two paramers:
#  - date: It gets the parameter in the format YYYYMM. In the case of AC_EMP_CARTERA_CLIENTE and AC_EMP_CARTERA_SERVICIO
# can also be YYYYMMDD. EMP_EXPLICATIVAS_CLI file always refer to the previous date (Accenture output).
# - type: must be one of the following strings [CVM|ACC|ALL]. 

main_cvm <- function(date) {
  cat("[INFO] Preparing Input Data for date: ", date, "\n")
  ifile <- paste0(input.folder.cvm, date, "/AC_EMP_CARTERA_CLIENTE_", date ,".TXT")
  cat("[LOAD] ", ifile, "\n")
  dt_AC_EMP_CARTERA_CLIENTE <- fread(ifile)
  
  ofolder <- paste0(data.folder,"/CVM/", date)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  ofile <- paste0(data.folder,"/CVM/", date, "/dt_AC_EMP_CARTERA_CLIENTE_", date, ".RData")
  cat("[SAVE] ", ofile, "\n")
  save(dt_AC_EMP_CARTERA_CLIENTE, file = ofile)
  rm(dt_AC_EMP_CARTERA_CLIENTE)
  gc()
  
  ifile <- paste0(input.folder.cvm, date, "/AC_EMP_CARTERA_SERVICIO_", date ,".TXT")
  cat("[LOAD] ", ifile, "\n")
  dt_AC_EMP_CARTERA_SERVICIO <- fread(ifile)
  
  ofile <- paste0(data.folder,"/CVM/", date, "/dt_AC_EMP_CARTERA_SERVICIO_", date, ".RData")
  cat("[SAVE] ", ofile, "\n")
  save(dt_AC_EMP_CARTERA_SERVICIO, file = ofile)
}

main_acc <- function(date) {
  
  ifile <- paste0(input.folder.acc.car, date, "/EMP_EXPLICATIVAS_CLI_", date, "_.txt")
  cat("[LOAD] ", ifile, "\n")
  dt.CAR.EMP <- fread(ifile)
  
  
  ofolder <- paste0(data.folder, "/ACC/", date)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  ofile <- paste0(data.folder, "/ACC/", date, "/dt_EMP_EXPLICATIVAS_CLI_", date, ".RData")
  cat("[SAVE] ", ofile, "\n")
  save(dt.CAR.EMP, file = ofile)
}

main <- function(date, type) {
  if (type == "CVM") {
    main_cvm(date)
  } else if (type == "ACC") {
    main_acc(date)
  } else if (type == "ALL") {
    main_cvm(date)
    main_acc(date)
  }
}
#--------
option_list <- list(
  make_option(c("-d", "--date"), type = "character", default = NULL, help = "input date (YYYYMM)", 
              metavar = "character"),
  make_option(c("-t", "--type"), type = "character", default = NULL, help = "type [CVM|ACC|LALL]", 
              metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# TODO parameter check
if (is.null(opt$date)) {
  print_help(opt_parser)
  stop("At least two parameters must be supplied (input date: YYYYMM ant type [CVM|ACC|ALL])", call.=FALSE)
} else {
  main(opt$date, opt$type)
}
