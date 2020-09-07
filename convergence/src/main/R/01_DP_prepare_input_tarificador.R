source("configuration.R")

main_01_DP_prepare_input_tarificador <- function(month) {
  ifile <- file.path(input.folder.cvm, month, input.folder.acc.in, paste0("EXTR_TARIFICADOR_POS_", month ,".TXT"))
  if (!file.exists(ifile))
      cat("[WARN] File", ifile, "does not exist\n")
      
  ofolder <- file.path(data.folder, "CVM", month)
  ofile <- file.path(ofolder, paste0("dt_EXTR_TARIFICADOR_POS_", month, ".RData"))
  
  ifile.ctime <- file.info(ifile)$ctime
  ofile.ctime <- file.info(ofile)$ctime
  
  if ( (!file.exists(ofile)) || (file.exists(ifile) && (ofile.ctime < ifile.ctime)) ) {
    cat("[LOAD] ", ifile, "\n")
    dt_EXTR_TARIFICADOR_POS <- fread(ifile)
    setnames(dt_EXTR_TARIFICADOR_POS, "nif_pagador", "x_num_ident")
    
    dt_EXTR_TARIFICADOR_POS <- dt_EXTR_TARIFICADOR_POS[nzchar(x_num_ident) == TRUE]
    
    if (!file.exists(ofolder)) {
      cat("[INFO] Creating Folder\n")
      dir.create(ofolder, recursive = T)
    }
    
    cat("[SAVE] ", ofile) 
    setkey(dt_EXTR_TARIFICADOR_POS, x_num_ident)
    save(dt_EXTR_TARIFICADOR_POS, file = ofile)
  } else {
    cat("[INFO] Skipping", ifile, "\n")
    cat("[LOAD] ", ofile)
    load(ofile)
  }
  cat(" -", nrow(dt_EXTR_TARIFICADOR_POS), "\n")
  
  invisible(dt_EXTR_TARIFICADOR_POS)
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
    main_01_DP_prepare_input_tarificador(opt$month)
  }
}
