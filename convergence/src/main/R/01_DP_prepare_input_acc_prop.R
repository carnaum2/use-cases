source("configuration.R")

# Required files:
#   <month>/OUTPUT/par_propensos_cross_nif_<month>.txt
main_01_DP_prepare_input_acc_prop <- function(month) {
  # There is not Accenture output for YYYYMMDD, just for YYYYMM
  if (nchar(month) == 8) {
    cat("[WARN] There is not Accenture output for", month, "just for dates in the form YYYYMM\n")
    return(NULL)
  }
  
  ifile <- file.path(input.folder.cvm, month, input.folder.acc.out, paste0("par_propensos_cross_nif_", month, ".txt"))
  if (!file.exists(ifile))
    cat("[WARN] File", ifile, "does not exist\n")
  
  ofolder <- file.path(data.folder, "ACC", month)
  ofile <- file.path(ofolder, paste0("dt_par_propensos_cross_nif_", month, ".RData"))
  
  ifile.ctime <- file.info(ifile)$ctime
  ofile.ctime <- file.info(ofile)$ctime
  
  if ( (!file.exists(ofile)) || (file.exists(ifile) && (ofile.ctime < ifile.ctime)) ) {
    if (file.exists(ifile)) {
      cat("[INFO] Preparing Input Data for Month: ", month, "\n")
      cat("[LOAD]", ifile, "\n")
      dt_par_propensos_cross_nif <- fread(ifile, sep ="\t")
      
      setnames(dt_par_propensos_cross_nif, "NIF", "x_num_ident")
      
      # Starting from 201611 IND_PROP_TV and IND_MUY_PROP_TV desappeared, and *_ADSL, *_FIBRA were introduced
      if (! "IND_PROP_TV" %in% colnames(dt_par_propensos_cross_nif)) {
        dt_par_propensos_cross_nif[, "IND_PROP_TV"] <- 0
        if (all(c("IND_PROP_TV_ADSL", "IND_PROP_TV_FIBRA") %in% colnames(dt_par_propensos_cross_nif)))
          dt_par_propensos_cross_nif[, "IND_PROP_TV"] <- apply(dt_par_propensos_cross_nif[, .(IND_PROP_TV_ADSL, IND_PROP_TV_FIBRA)], 1, max)
      }
      if (! "IND_MUY_PROP_TV" %in% colnames(dt_par_propensos_cross_nif)) {
        dt_par_propensos_cross_nif[, "IND_MUY_PROP_TV"] <- 0
        if (all(c("IND_PROP_TV_ADSL", "IND_PROP_TV_FIBRA") %in% colnames(dt_par_propensos_cross_nif)))
          dt_par_propensos_cross_nif[, "IND_MUY_PROP_TV"] <- apply(dt_par_propensos_cross_nif[, .(IND_MUY_PROP_TV_ADSL, IND_MUY_PROP_TV_FIBRA)], 1, max)
      }
      
      if (!file.exists(ofolder)) {
        cat("[INFO] Creating Folder\n")
        dir.create(ofolder, recursive = T)
      }
      
      cat("[SAVE] ", ofile)
      save(dt_par_propensos_cross_nif, file = ofile)
    } else {
      cat("[WARN] File", ifile, "does not exist\n")
      return(NULL)
    }
  } else {
    cat("[INFO] Skipping", ifile, "\n")
    cat("[LOAD] ", ofile)
    load(ofile)
  }
  cat(" -", nrow(dt_par_propensos_cross_nif), "\n")
  
  invisible(dt_par_propensos_cross_nif)
}

# Backup
# main_01_DP_prepare_input_acc_prop <- function(month) {
#   # There is not Accenture output for YYYYMMDD, just for YYYYMM
#   if (nchar(month) == 8) {
#     cat("[WARN] There is not Accenture output for", month, "just for YYYYMM\n")
#     month <- substr(month, 0, 6)
#   }
#   
#   # First, get ifile of provided month. If it does not exist, try with previous month
#   ifile <- file.path(input.folder.cvm, month, input.folder.acc.out, paste0("par_propensos_cross_nif_", month, ".txt"))
#   if (!file.exists(ifile)) {
#     cat("[WARN] File", ifile, "does not exist!\n")
#     
#     # Check previous month
#     
#     # Get the first day of the month
#     month <- paste0(month, "01")
#     month.posix <- as.POSIXct(strptime(month, "%Y%m%d"))
#     prev.day <- seq.POSIXt(month.posix, length=2, by="-1 months")[2]
#     month <- format(prev.day, format = "%Y%m")
#     rm(month.posix, prev.day)
#     cat("[WARN] Trying previous month:", month, "\n")
#     
#     ifile <- file.path(input.folder.cvm, month, input.folder.acc.out, paste0("par_propensos_cross_nif_", month, ".txt"))
#   }
#   
#   ofolder <- file.path(data.folder, "ACC", month)
#   ofile <- file.path(ofolder, paste0("dt_par_propensos_cross_nif_", month, ".RData"))
#   
#   ifile.ctime <- file.info(ifile)$ctime
#   ofile.ctime <- file.info(ofile)$ctime
#   
#   if ( (!file.exists(ofile)) || (file.exists(ifile) && (ofile.ctime < ifile.ctime)) ) {
#     if (file.exists(ifile)) {
#       cat("[INFO] Preparing Input Data for Month: ", month, "\n")
#       cat("[LOAD]", ifile, "\n")
#       dt_par_propensos_cross_nif <- fread(ifile, sep ="\t")
#     } else {
#       cat("[WARN] File", ifile, "does not exist!\n")
#       return(NULL)
#     }
#     
#     setnames(dt_par_propensos_cross_nif, "NIF", "x_num_ident")
#     
#     # Starting from 201611 IND_PROP_TV and IND_MUY_PROP_TV desappeared, and *_ADSL, *_FIBRA were introduced
#     if (! "IND_PROP_TV" %in% colnames(dt_par_propensos_cross_nif)) {
#       dt_par_propensos_cross_nif[, "IND_PROP_TV"] <- apply(dt_par_propensos_cross_nif[, .(IND_PROP_TV_ADSL, IND_PROP_TV_FIBRA)], 1, max)
#     }
#     if (! "IND_MUY_PROP_TV" %in% colnames(dt_par_propensos_cross_nif)) {
#       dt_par_propensos_cross_nif[, "IND_MUY_PROP_TV"] <- apply(dt_par_propensos_cross_nif[, .(IND_MUY_PROP_TV_ADSL, IND_MUY_PROP_TV_FIBRA)], 1, max)
#     }
#     
#     if (!file.exists(ofolder)) {
#       cat("[INFO] Creating Folder\n")
#       dir.create(ofolder, recursive = T)
#     }
#     
#     cat("[SAVE] ", ofile)
#     save(dt_par_propensos_cross_nif, file = ofile)
#   } else {
#     cat("[INFO] Skipping", ifile, "\n")
#     cat("[LOAD] ", ofile)
#     load(ofile)
#   }
#   cat(" -", nrow(dt_par_propensos_cross_nif), "\n")
#   
#   invisible(dt_par_propensos_cross_nif)
# }

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
    main_01_DP_prepare_input_acc_prop(opt$month)
  }
}
