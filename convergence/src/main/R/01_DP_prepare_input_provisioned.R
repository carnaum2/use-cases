source("configuration.R")

# Required files:
#   <month>/INPUT/Provisionadas_<month>.txt
main_01_DP_prepare_input_provisioned <- function(month) {
  ifile <- file.path(input.folder.cvm, month, input.folder.acc.in, paste0("Provisionadas_", month, ".txt"))
  if (!file.exists(ifile))
    cat("[WARN] File", ifile, "does not exist\n")
  
  ofolder <- file.path(data.folder, "CVM", month)
  ofile <- file.path(ofolder, paste0("dt.Provisioned_", month, ".RData"))
  
  ifile.ctime <- file.info(ifile)$ctime
  ofile.ctime <- file.info(ofile)$ctime
  
  if ( (!file.exists(ofile)) || (file.exists(ifile) && (ofile.ctime < ifile.ctime)) ) {
    if (file.exists(ifile)) {
      cat("[INFO] Preparing Input Data for Month: ", month, "\n")
      cat("[LOAD]", ifile, "\n")
      dt.provisioned <- fread(ifile)
      
      if (!file.exists(ofolder)) {
        cat("[INFO] Creating Folder\n")
        dir.create(ofolder, recursive = T)
      }
      
      cat("[SAVE] ", ofile)
      save(dt.provisioned, file = ofile)
    } else {
      cat("[WARN] File", ifile, "does not exist\n")
      return(NULL)
    }
  } else {
    cat("[INFO] Skipping", ifile, "\n")
    cat("[LOAD] ", ofile)
    load(ofile)
  }
  cat(" -", nrow(dt.provisioned), "\n")
  
  invisible(dt.provisioned)
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
    main_01_DP_prepare_input_provisioned(opt$month)
  }
}
