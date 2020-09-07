source("configuration.R")

# Required files:
#   CVM/<day>/INPUT/Provisionadas_<day>.txt"
main_03_get_convergent_postpaid <- function(month, prov = NULL, dt_EXTR_AC_FINAL_POSPAGO = NULL, dt_EXTR_NIFS_COMPARTIDOS = NULL, dt.provisioned = NULL) {
  cat ("\n[MAIN] 03_get_convergent_postpaid", month, prov, "\n")
  
  ifile <- file.path(data.folder, "CVM", month, paste0("dt_EXTR_AC_FINAL_POSPAGO_", month, ".RData"))
  if (is.null(dt_EXTR_AC_FINAL_POSPAGO)) {
    cat("[LOAD]", ifile, "\n")
    load(ifile)
  } else {
    cat("[INFO] Skipping", ifile, ", already loaded\n")
  }
  
  setkey(dt_EXTR_AC_FINAL_POSPAGO, x_num_ident)
  
  # COUNTs
  dt_EXTR_AC_FINAL_POSPAGO[FLAGVOZ    == 1, SUM_VOZ        := .N, by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[FLAGADSL   == 1, SUM_ADSL       := .N, by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[FLAGHZ     == 1, SUM_HZ         := .N, by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[FLAGFTTH   == 1, SUM_FTTH       := .N, by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[FLAGLPD    == 1, SUM_LPD        := .N, by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[FLAGTIVO   == 1, SUM_TIVO       := .N, by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[FLAGFUTBOL == 1, SUM_FLAGFUTBOL := .N, by = x_num_ident]
  
  # FIX NAs
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_VOZ),        SUM_VOZ        := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_ADSL),       SUM_ADSL       := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_HZ),         SUM_HZ         := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_FTTH),       SUM_FTTH       := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_LPD),        SUM_LPD        := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_TIVO),       SUM_TIVO       := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_FLAGFUTBOL), SUM_FLAGFUTBOL := 0]
  
  # Reduce
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_VOZ        := max(SUM_VOZ),        by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_ADSL       := max(SUM_ADSL),       by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_HZ         := max(SUM_HZ),         by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_FTTH       := max(SUM_FTTH),       by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_LPD        := max(SUM_LPD),        by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_TIVO       := max(SUM_TIVO),       by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_FLAGFUTBOL := max(SUM_FLAGFUTBOL), by = x_num_ident]

  dt_EXTR_AC_FINAL_POSPAGO[, ARPU := sum(ARPU), by = x_num_ident]
  
  setkey(dt_EXTR_AC_FINAL_POSPAGO, x_num_ident)
  dt_EXTR_AC_FINAL_POSPAGO <- unique(dt_EXTR_AC_FINAL_POSPAGO, by=key(dt_EXTR_AC_FINAL_POSPAGO))

  dt_EXTR_AC_FINAL_POSPAGO[, IS_CONVERGENTE := 0]
  
  dt_EXTR_AC_FINAL_POSPAGO[SUM_VOZ > 0 & SUM_ADSL       > 0, IS_CONVERGENTE := 1]
  dt_EXTR_AC_FINAL_POSPAGO[SUM_VOZ > 0 & SUM_HZ         > 0, IS_CONVERGENTE := 1]
  dt_EXTR_AC_FINAL_POSPAGO[SUM_VOZ > 0 & SUM_FTTH       > 0, IS_CONVERGENTE := 1]
  dt_EXTR_AC_FINAL_POSPAGO[SUM_VOZ > 0 & SUM_LPD        > 0, IS_CONVERGENTE := 1]
  dt_EXTR_AC_FINAL_POSPAGO[SUM_VOZ > 0 & SUM_TIVO       > 0, IS_CONVERGENTE := 1]
  dt_EXTR_AC_FINAL_POSPAGO[SUM_VOZ > 0 & SUM_FLAGFUTBOL > 0, IS_CONVERGENTE := 1]
  
  dt.Convergentes <- dt_EXTR_AC_FINAL_POSPAGO[IS_CONVERGENTE == 1,]
  
  if (!is.null(prov)) {
    ifile <- file.path(data.folder, "CVM", month, paste0("dt.Provisionadas_", month, ".RData"))
    if (is.null(dt.provisioned)) {
      cat("[LOAD]", ifile, "\n")
      load(ifile)
    } else {
      cat("[INFO] Skipping", ifile, ", already loaded\n")
    }
    
    dt.missed <- dt.provisioned[!CIF_NIF %in% dt.Convergentes$x_num_ident]
    cat("[INFO] Missing", nrow(dt.missed), "provisioned NIFS\n")
    
    dt.Add.Convergentes <- dt_EXTR_AC_FINAL_POSPAGO[x_num_ident %in% dt.missed$CIF_NIF]
    cat("[INFO] Adding", nrow(dt.Add.Convergentes), "convergent NIFS\n")
    
    dt.Add.Convergentes[, IS_CONVERGENTE := 1]
    dt.Add.Convergentes[, SUM_FTTH := 1] # FIXME. Get from Provisionadas if it is FTTH, NEBA or DSL
    
    dt.Convergentes <- rbind(dt.Convergentes, dt.Add.Convergentes)
  }


  # Add those Vodafone mobile-only that are also in Ono stack
  
  if (is.null(dt_EXTR_NIFS_COMPARTIDOS)) {
    ifile <- file.path(data.folder, "CVM", month, paste0("dt_EXTR_NIFS_COMPARTIDOS_", month, ".RData"))
    if (file.exists(ifile)) {
      cat("[LOAD]", ifile, "\n")
      load(ifile)
    } else {
      cat("[WARN] File", ifile, "does not exist\n")
    }
  } else {
    cat("[INFO] Skipping", ifile, ", already loaded\n")
  }
  
  #dt.Convergentes <- dt.Convergentes[!x_num_ident %in% dt_EXTR_NIFS_COMPARTIDOS$NIF] # FIXME: Hay que añadir a los solo-voz que esten en Ono, no quitarlos
  mobile.only <- dt_EXTR_AC_FINAL_POSPAGO[SUM_VOZ > 0 & IS_CONVERGENTE == 0]
  
  if (! is.null(dt_EXTR_NIFS_COMPARTIDOS)) {
    dt.Add.Convergentes <- mobile.only[x_num_ident %in% dt_EXTR_NIFS_COMPARTIDOS$NIF] # FIXME: Could it be that a client present in Ono stack is mobile-only?
    cat("[INFO] Adding to convergents", nrow(dt.Add.Convergentes), "Vodafone mobile-only that are also in Ono stack\n")
  } else {
    dt.Add.Convergentes <- data.table()
  }
  
  if (nrow(dt.Add.Convergentes) > 0) {
    dt.Add.Convergentes[, IS_CONVERGENTE := 1]
    dt.Add.Convergentes[, SUM_FTTH := 1] # FIXME. Don't know whether they are FTTH, NEBA or DSL
    
    dt.Convergentes <- rbind(dt.Convergentes, dt.Add.Convergentes)
  }
  
  rm(dt_EXTR_AC_FINAL_POSPAGO)
  dev.null <- gc()
  
  
  #ofile <- file.path(datasets.folder, month, paste0("Convergentes-", month, ".csv"))
  #cat("[SAVE] ", ofile, "-", nrow(dt.Convergentes), "\n")
  #write.table(dt.Convergentes, file = ofile, row.names = F, sep ="|", quote = F)

  ofolder <- file.path(datasets.folder, month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  ofile <- file.path(datasets.folder, month, paste0("dt.Convergentes-", month, ".RData"))
  cat("[SAVE] ", ofile, "-", nrow(dt.Convergentes), "\n")
  save(dt.Convergentes, file = ofile)
  
  dt.Convergentes.nifs <- dt.Convergentes[, .(x_num_ident)]

  #ofile <- file.path(datasets.folder, month, paste0("Convergentes-NIFS-", month, ".csv"))
  #cat("[SAVE] ", ofile, "-", nrow(dt.Convergentes.nifs), "\n")
  #write.table(dt.Convergentes.nifs, file = ofile, row.names = F, sep ="|", quote = F)
  
  ofile <- file.path(datasets.folder, month, paste0("dt.Convergentes-NIFS-", month, ".RData"))
  cat("[SAVE] ", ofile, "-", nrow(dt.Convergentes.nifs), "\n")
  save(dt.Convergentes.nifs, file = ofile)
  
  invisible(dt.Convergentes)
}
#---------------------------------------------------------------------------------------------------
if (!exists("sourced")) {
  option_list <- list(
    make_option(c("-m", "--month"), type = "character", default = NULL, help = "input date (YYYYMMDD)", 
                metavar = "character"),
    make_option(c("-p", "--prov"), type = "character", default = NULL, help = "input date (YYYYMMDD)", 
                metavar = "character")
  )
  
  opt_parser <- OptionParser(option_list = option_list)
  opt <- parse_args(opt_parser)
  
  if (is.null(opt$month)) {
    print_help(opt_parser)
    stop("At least one parameter must be supplied (input month: YYYYMM)", call.=FALSE)
  } else {
    main_03_get_convergent_postpaid(opt$month, opt$prov, NULL, NULL, NULL)
  }
}
