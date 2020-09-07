source("configuration.R")

# Required files:
#   <month>/INPUT/EXTR_NIFS_COMPARTIDOS_<month>.TXT
main_01_DP_prepare_input_cvm_nifs_compartidos <- function(month) {
  ifile <- file.path(input.folder.cvm, month, input.folder.acc.in, paste0("EXTR_NIFS_COMPARTIDOS_", month ,".TXT"))
  if (!file.exists(ifile))
    cat("[WARN] File", ifile, "does not exist\n")
      
  ofolder <- file.path(data.folder, "CVM", month)
  ofile <- file.path(ofolder, paste0("dt_EXTR_NIFS_COMPARTIDOS_", month, ".RData"))
  
  ifile.ctime <- file.info(ifile)$ctime
  ofile.ctime <- file.info(ofile)$ctime
  
  if ( (!file.exists(ofile)) || (file.exists(ifile) && (ofile.ctime < ifile.ctime)) ) {
    if (file.exists(ifile)) {
      cat("[INFO] Preparing Input Data for Month: ", month, "\n")
      cat("[LOAD] ", ifile, "\n")
      dt_EXTR_NIFS_COMPARTIDOS <- fread(ifile)
    
      if (!file.exists(ofolder)) {
        cat("[INFO] Creating Folder\n")
        dir.create(ofolder, recursive = T)
      }
      
      cat("[SAVE] ", ofile)
      setnames(dt_EXTR_NIFS_COMPARTIDOS, "NIF", "x_num_ident")
      setkey(dt_EXTR_NIFS_COMPARTIDOS, x_num_ident)
      save(dt_EXTR_NIFS_COMPARTIDOS, file = ofile)
    } else {
      cat("[WARN] File", ifile, "does not exist\n")
      return(NULL)
    }
  } else {
    cat("[INFO] Skipping", ifile, "\n")
    cat("[LOAD] ", ofile)
    load(ofile)
  }
  cat(" -", nrow(dt_EXTR_NIFS_COMPARTIDOS), "\n")
  
  invisible(dt_EXTR_NIFS_COMPARTIDOS)
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
    main_01_DP_prepare_input_cvm_nifs_compartidos(opt$month)
  }
}
