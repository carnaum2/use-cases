source("configuration.R")

main_01_DP_prepare_input_acc_car <- function(month) {
  ifile <- file.path(input.folder.cvm, month, input.folder.acc.car, paste0("PAR_EXPLIC_CLI_6M_", month, ".txt"))
  if (!file.exists(ifile))
      cat("[WARN] File", ifile, "does not exist\n")
      
  ofolder <- file.path(data.folder, "ACC", month)
  ofile <- file.path(ofolder, paste0("dt.PAR_EXPLIC_CLI_6M_", month, ".RData"))
  
  ifile.ctime <- file.info(ifile)$ctime
  ofile.ctime <- file.info(ofile)$ctime
  
  if ( (!file.exists(ofile)) || (file.exists(ifile) && (ofile.ctime < ifile.ctime)) ) {
    cat("[INFO] Preparing Input Data for Month: ", month, "\n")
    cat("[LOAD] ", ifile, "\n")
    dt.PAR_EXPLIC_CLI_6M <- fread(ifile, sep ="\t")
    
    if (!file.exists(ofolder)) {
      cat("[INFO] Creating Folder\n")
      dir.create(ofolder, recursive = T)
    }
    
    cat("[SAVE] ", ofile)
    save(dt.PAR_EXPLIC_CLI_6M, file = ofile)
  } else {
    cat("[INFO] Skipping", ifile, "\n")
    cat("[LOAD] ", ofile)
    load(ofile)
  }
  cat(" -", nrow(dt.PAR_EXPLIC_CLI_6M), "\n")
  
  invisible(dt.PAR_EXPLIC_CLI_6M)
}

#--------

if (!exists("sourced")) {
  option_list <- list(
    make_option(c("-m", "--month"), type = "character", default = NULL, help = "input month (YYYYMM)", 
                metavar = "character")
  )
  
  opt_parser <- OptionParser(option_list = option_list)
  opt <- parse_args(opt_parser)
  
  if (is.null(opt$month)) {
    print_help(opt_parser)
    stop("At least one parameter must be supplied (input month: YYYYMM)", call.=FALSE)
  } else {
    main_01_DP_prepare_input_acc_car(opt$month)
  }
}
