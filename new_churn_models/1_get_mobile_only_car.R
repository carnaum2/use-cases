source("configuration.R")


main <- function(month) {
  
  ifile <- paste0(acc.input.folder, month, "/PAR_EXPLIC_LIN_6M_", month, ".txt")
  cat("[LOAD]", ifile, "\n")
  dt.car.mo <- fread(ifile, sep = "\t", na.strings = c("NULL"))
  
  # Set Header
  ifile <- paste0(acc.input.folder, month, "/header_CBU.txt")
  cat("[LOAD]", ifile, "\n")
  dt.header <- fread(ifile)
  names(dt.car.mo) <- toupper(names(dt.header))
 
  cat(str(dt.car.mo))
  if(!is.numeric(dt.car.mo$NUM_ARPU_AVG)) {
    cat("[ERROR] Numeric variables are readed as Strings. Check Header inside File\n")
    return(-1)
  }
  
  # Get MobileOnly numbers
  ifile <- paste0(datasets.folder, month, "/dt.MobileOnly-NIFS-", month, ".RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  
  dt.car.mo <- dt.car.mo[NIF %in% dt.MobileOnly$x_num_ident,]
  
  setkey(dt.car.mo, NIF)
  dt.car.mo[, CONT := .N, by = NIF]
  
  # Ensure mobile-only mono-line
  dt.car.mo <- dt.car.mo[ CONT == 1,]
  dt.car.mo[, CONT := NULL]
  
  dt.car.mo[is.null(dt.car.mo)] <- -1
  dt.car.mo[is.na(dt.car.mo)] <- -1
  
  ofolder <- paste0(acc.data.folder, month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  ofile <- paste0(ofolder, "/dt.car.mo.", month, ".RData")
  cat("[SAVE]", ofile, "\n")
  save(dt.car.mo, file = ofile)
}

#----------------------------------------------------------------------------

#main("201708")
main("201709")
print(warnings())
