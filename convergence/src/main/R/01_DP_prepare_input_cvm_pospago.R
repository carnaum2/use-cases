source("configuration.R")

# initial.options <- commandArgs(trailingOnly = FALSE)
# file.arg.name <- "--file="
# script.name <- sub(file.arg.name, "", initial.options[grep(file.arg.name, initial.options)])
# script.basename <- dirname(script.name)
# other.name <- paste(sep="/", script.basename, "01_DP_prepare_input_cvm_pospago.R")
# print(paste("2 Sourcing",other.name,"from",script.name))
# quit()

# Required files:
#   <month>/INPUT/EXTR_AC_FINAL_POSPAGO_<month>.TXT
main_01_DP_prepare_input_cvm_pospago <- function(month) {
  ifile <- file.path(input.folder.cvm, month, input.folder.acc.in, paste0("EXTR_AC_FINAL_POSPAGO_", month ,".TXT"))
  if (!file.exists(ifile))
      cat("[WARN] File", ifile, "does not exist\n")
      
  ofolder <- file.path(data.folder, "CVM", month)
  ofile <- file.path(ofolder, paste0("dt_EXTR_AC_FINAL_POSPAGO_", month, ".RData"))
  
  ifile.ctime <- file.info(ifile)$ctime
  ofile.ctime <- file.info(ofile)$ctime
  
  if ( (!file.exists(ofile)) || (file.exists(ifile) && (ofile.ctime < ifile.ctime)) ) {
    cat("[INFO] Preparing Input Data for Month: ", month, "\n")
    cat("[LOAD] ", ifile, "\n")
    dt_EXTR_AC_FINAL_POSPAGO <- fread(ifile)
    
    # Replace ',' by '.', and converto to numeric
    dt_EXTR_AC_FINAL_POSPAGO[, ARPU := gsub(',', '.', dt_EXTR_AC_FINAL_POSPAGO$ARPU)]
    dt_EXTR_AC_FINAL_POSPAGO[, ARPU := as.numeric(dt_EXTR_AC_FINAL_POSPAGO$ARPU)]
    
    if ("CANTIDAD_PENDIENTE" %in% colnames(dt_EXTR_AC_FINAL_POSPAGO)) {
      dt_EXTR_AC_FINAL_POSPAGO[, CANTIDAD_PENDIENTE := gsub(',', '.', dt_EXTR_AC_FINAL_POSPAGO$CANTIDAD_PENDIENTE)]
      dt_EXTR_AC_FINAL_POSPAGO[, CANTIDAD_PENDIENTE := as.numeric(dt_EXTR_AC_FINAL_POSPAGO$CANTIDAD_PENDIENTE)]
    }
    
    if (!file.exists(ofolder)) {
      cat("[INFO] Creating Folder\n")
      dir.create(ofolder, recursive = T)
    }
    
    cat("[SAVE] ", ofile, "-", nrow(dt_EXTR_AC_FINAL_POSPAGO), "\n")
    setkey(dt_EXTR_AC_FINAL_POSPAGO, x_id_red)
    save(dt_EXTR_AC_FINAL_POSPAGO, file = ofile)
    
    # Output id and SEG_CLIENTE
    if ("SEG_CLIENTE" %in% colnames(dt_EXTR_AC_FINAL_POSPAGO)) {
      # In punctual extractions field "SEG_CLIENTE" does not exist. Only in monthly extractions.
      dt_EXTR_AC_FINAL_POSPAGO_SEG_CLIENTE <- dt_EXTR_AC_FINAL_POSPAGO[, .(SEG_CLIENTE = unique(SEG_CLIENTE)), by = x_num_ident]
      ofile <- file.path(ofolder, paste0("dt_EXTR_AC_FINAL_POSPAGO_SEG_CLIENTE_", month, ".RData"))
      cat("[SAVE] ", ofile)
      setkey(dt_EXTR_AC_FINAL_POSPAGO_SEG_CLIENTE, x_num_ident)
      save(dt_EXTR_AC_FINAL_POSPAGO_SEG_CLIENTE, file = ofile)
    }
  } else {
    cat("[INFO] Skipping", ifile, "\n")
    cat("[LOAD] ", ofile)
    load(ofile)
    cat(" -", nrow(dt_EXTR_AC_FINAL_POSPAGO), "\n")
  }

  invisible(dt_EXTR_AC_FINAL_POSPAGO)
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
    main_01_DP_prepare_input_cvm_pospago(opt$month)
  }
}
