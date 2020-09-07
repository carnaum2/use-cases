source("configuration.R")

# Compute those Mobile Only Customers that became convergent from two specific dates. It read the Mobile Only and Convergentes file
# and Writes two different files: dt.convergidos and dt.Noconvergidos. These files will be used to prepare the training data sets.

main <- function(prev.date, next.date) { 
  ifile <- paste0(datasets.folder, "/", prev.date, "/dt.mobile.only.", prev.date, "_EBU.RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)

  ifile <- paste0(datasets.folder, "/", next.date, "/dt.convergentes.", next.date, "_EBU.RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  
  dt.mobile.only <- dt.mobile.only[SEGMENTACION_MERCADO == "A" | SEGMENTACION_MERCADO == "S"]
  dt.convergidos <- dt.mobile.only[CIF_NIF_PPAL %in% dt.convergentes$CIF_NIF_PPAL]

  cat("Convergented customers: ", nrow(dt.convergidos), " from ", nrow(dt.mobile.only), " ",  
      nrow(dt.convergidos)/nrow(dt.mobile.only), "\n")
  ofile <- paste0(datasets.folder, "/", prev.date, "/dt.convergidos.", prev.date, "_EBU.RData")
  cat("[SAVE]", ofile, "\n")
  save(dt.convergidos, file = ofile)
  
  ofile <- paste0(datasets.folder, "/", prev.date, "/dt.convergidos.", prev.date, "_EBU_CIF.RData")
  cat("[SAVE]", ofile, "\n")
  dt.convergidos <- dt.convergidos[, .(CIF_NIF_PPAL)]
  save(dt.convergidos, file = ofile)
  
  dt.Noconvergidos <- dt.mobile.only[!CIF_NIF_PPAL %in% dt.convergentes$CIF_NIF_PPAL]
  dt.Noconvergidos <- dt.Noconvergidos[SEGMENTACION_MERCADO == "A" | SEGMENTACION_MERCADO == "S"]
  
  cat("Non-convergented customers: ", nrow(dt.Noconvergidos), " from ", nrow(dt.mobile.only), " ",  
      nrow(dt.Noconvergidos)/nrow(dt.mobile.only), "\n")
  
  ofolder <- paste0(datasets.folder, "/", prev.date)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- paste0(datasets.folder, "/", prev.date, "/dt.Noconvergidos.", prev.date, "_EBU.RData")
  cat("[SAVE]", ofile, "\n")
  save(dt.Noconvergidos, file = ofile)
  
  ofile <- paste0(datasets.folder, "/", prev.date, "/dt.Noconvergidos.", prev.date, "_EBU_CIF.RData")
  cat("[SAVE]", ofile, "\n")
  dt.Noconvergidos <- dt.Noconvergidos[, .(CIF_NIF_PPAL)]
  save(dt.Noconvergidos, file = ofile)
}
#---------------

option_list <- list(
  make_option(c("-pd", "--prev_date"), type = "character", default = NULL, help = "Mobile Only Customers Date (YYYYMM)", 
              metavar = "character"),
  make_option(c("-nd", "--next_date"), type = "character", default = NULL, help = "Convergent Customers Date (YYYYMM)", 
              metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

# TODO parameter check
if (is.null(opt$prev_date)) {
  print_help(opt_parser)
  stop("At least two parameters must be supplied --prev_date and --next_date", call.=FALSE)
} else {
  main(opt$prev_date, opt$next_date)
}
