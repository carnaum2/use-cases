source("configuration.R")

main_04_get_convergidos_and_not_convergidos <- function(month, next.month, dt.MobileOnly = NULL, dt.Convergentes = NULL) {
  cat ("\n[MAIN] 04_get_convergidos_and_not_convergidos", month, next.month, "\n")
  
  ifile <- file.path(datasets.folder, month, paste0("dt.MobileOnly-", month, ".RData"))
  if (is.null(dt.MobileOnly)) {
    cat("[LOAD]", ifile, "\n")
    load(ifile)
  } else {
    cat("[INFO] Skipping", ifile, ", already loaded\n")
  }
  
  ifile <- file.path(datasets.folder, next.month, paste0("dt.Convergentes-", next.month, ".RData"))
  if (is.null(dt.Convergentes)) {
    cat("[LOAD]", ifile, "\n")
    load(ifile)
  } else {
    cat("[INFO] Skipping", ifile, ", already loaded\n")
  }

  dt.Convergentes <- dt.Convergentes[, .(x_num_ident, SUM_VOZ, SUM_ADSL, SUM_HZ, SUM_FTTH, SUM_LPD, SUM_TIVO, 
                                         #SUM_FLAGVFBOX, 
                                         SUM_FLAGFUTBOL)]
  setnames(dt.Convergentes, c("x_num_ident", "SUM_VOZ_M1", "SUM_ADSL_M1", "SUM_HZ_M1", "SUM_FTTH_M1", "SUM_LPD_M1", "SUM_TIVO_M1", 
                              #"SUM_FLAGVFBOX_M1", 
                              "SUM_FLAGFUTBOL_M1"))
  
  dt.Convergidos   <- dt.MobileOnly[x_num_ident %in% dt.Convergentes$x_num_ident]
  setkey(dt.Convergidos, x_num_ident)
  setkey(dt.Convergentes, x_num_ident)
  dt.Convergidos <- dt.Convergentes[dt.Convergidos] # FIXME: This takes the feats of the month in which is convergent?
  
  dt.NoConvergidos <- dt.MobileOnly[!x_num_ident %in% dt.Convergentes$x_num_ident] # FIXME: This also includes those mobile-only that did churn!
  
  ofile <- file.path(datasets.folder, month, paste0("dt.Convergidos-", month, ".RData"))
  cat("[SAVE] ", ofile, "-", nrow(dt.Convergidos), "\n")
  save(dt.Convergidos, file = ofile)
 
  ofile <- file.path(datasets.folder, month, paste0("dt.NoConvergidos-", month, ".RData"))
  cat("[SAVE] ", ofile, "-", nrow(dt.NoConvergidos), "\n")
  save(dt.NoConvergidos, file = ofile)
  
  invisible(list(dt.Convergidos=dt.Convergidos, dt.NoConvergidos=dt.NoConvergidos))
}
#---------------------------------------------------------------------------------------------------
if (!exists("sourced")) {
  option_list <- list(
    make_option(c("-m", "--month"), type = "character", default = NULL, help = "input month (YYYYMM)", 
                metavar = "character"),
    make_option(c("-nm", "--next_month"), type = "character", default = NULL, help = "next month (YYYYMM)", 
                metavar = "character")
  )
  
  opt_parser <- OptionParser(option_list = option_list)
  opt <- parse_args(opt_parser)
  
  if (is.null(opt$month)) {
    print_help(opt_parser)
    stop("At least one parameter must be supplied (input month: YYYYMM)", call.=FALSE)
  } else {
    main_04_get_convergidos_and_not_convergidos(opt$month, opt$next_month, NULL, NULL)
  }
}
