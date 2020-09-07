source("configuration.R")

getMobileOnly <- function(month) {
  ifile <- paste0(cvm.data.folder, month, "/dt_EXTR_AC_FINAL_POSPAGO_", month, ".RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  
  setkey(dt_EXTR_AC_FINAL_POSPAGO, x_num_ident)
  
  # COUNTs
  dt_EXTR_AC_FINAL_POSPAGO[FLAGVOZ == 1,    SUM_VOZ := .N,  by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[FLAGADSL == 1,   SUM_ADSL := .N, by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[FLAGHZ == 1,     SUM_HZ := .N,   by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[FLAGFTTH == 1,   SUM_FTTH := .N, by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[FLAGLPD == 1,    SUM_LPD := .N,  by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[FLAGTIVO == 1,   SUM_TIVO := .N, by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[FLAGFUTBOL == 1, SUM_FLAGFUTBOL := .N, by = x_num_ident]
  
  # FIX NAs
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_VOZ),  SUM_VOZ := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_ADSL), SUM_ADSL := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_HZ),   SUM_HZ := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_FTTH), SUM_FTTH := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_LPD),  SUM_LPD := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_TIVO), SUM_TIVO := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_FLAGFUTBOL), SUM_FLAGFUTBOL := 0]
  
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_VOZ  := max(SUM_VOZ),  by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_ADSL := max(SUM_ADSL), by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_HZ   := max(SUM_HZ),   by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_FTTH := max(SUM_FTTH), by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_LPD  := max(SUM_LPD),  by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_TIVO := max(SUM_TIVO), by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_FLAGFUTBOL := max(SUM_FLAGFUTBOL), by = x_num_ident]
  
  dt_EXTR_AC_FINAL_POSPAGO[, ARPU := sum(ARPU), by = x_num_ident]
  
  setkey(dt_EXTR_AC_FINAL_POSPAGO, x_num_ident)
  dt_EXTR_AC_FINAL_POSPAGO <- unique(dt_EXTR_AC_FINAL_POSPAGO)
  
  dt.MobileOnly <- dt_EXTR_AC_FINAL_POSPAGO[SUM_VOZ > 0 & SUM_ADSL == 0 & SUM_HZ == 0 & SUM_FTTH == 0 & 
                                              SUM_LPD == 0 &SUM_TIVO == 0 & SUM_FLAGFUTBOL == 0]
  rm(dt_EXTR_AC_FINAL_POSPAGO)
  dev.null <- gc()
  
  ifile <- paste0(cvm.data.folder, month, "/dt_EXTR_NIFS_COMPARTIDOS_", month, ".RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  
  dt.MobileOnly <- dt.MobileOnly[!x_num_ident %in% dt_EXTR_NIFS_COMPARTIDOS$NIF]
  
  # Filter out invalid tariffs
  dt.MobileOnly <- dt.MobileOnly[x_plan %in% valid.tariffs,]
  # Active customers
  dt.MobileOnly <- dt.MobileOnly[part_status == "AC",]
  # Without pending payments
  dt.MobileOnly <- dt.MobileOnly[DEUDA == 0,]
  
  dt.MobileOnly <- dt.MobileOnly[, .(x_num_ident)]
  
  # Save File
  ofolder <- paste0(datasets.folder, month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }    
  ofile <- paste0(datasets.folder, "/", month, "/dt.MobileOnly-NIFS-", month, ".RData")
  cat("[SAVE] ", ofile, "\n")
  save(dt.MobileOnly, file = ofile)
}

main <- function(month) {
  getMobileOnly(month)
}

#----------------------------------------------------------------------------

#main("201706")
#main("201706")
main(201707)
#main("201709")