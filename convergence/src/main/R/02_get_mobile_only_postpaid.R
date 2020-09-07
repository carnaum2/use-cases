source("configuration.R")

# FIXME. Algunas variables en AC_FINAL_POSPAGO son distintas en los multi-linea

# SEG_CLIENTE
# INSTALL_DATE
# X_FECHA_CREACION_SERVICIO
# x_fecha_ini_prov
# x_plan
# PLANDATOS
# PROMOCION_VF
# FECHA_FIN_CP_VF
# MESES_FIN_CP_VF
# MESES_FIN_CP_TARIFA
# modelo
# sistema_operativo
# X_SFID_SERVICIO
# x_fecha_recepcion
# LAST_UPD
# PPRECIOS_DESTINO
# FECHA_CAMBIO
# TARIFA_CANJE
# FECHA_ALTA_SERVICIO


main_02_get_mobile_only_postpaid <- function(month, dt_EXTR_AC_FINAL_POSPAGO = NULL, dt_EXTR_NIFS_COMPARTIDOS = NULL) {
  cat ("\n[MAIN] 02_get_mobile_only_postpaid", month, "\n")
  
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
  #dt_EXTR_AC_FINAL_POSPAGO[FLAGVFBOX  == 1, SUM_FLAGVFBOX  := .N, by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[FLAGFUTBOL == 1, SUM_FLAGFUTBOL := .N, by = x_num_ident]
  
  # FIX NAs
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_VOZ),        SUM_VOZ        := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_ADSL),       SUM_ADSL       := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_HZ),         SUM_HZ         := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_FTTH),       SUM_FTTH       := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_LPD),        SUM_LPD        := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_TIVO),       SUM_TIVO       := 0]
  #dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_FLAGVFBOX),  SUM_FLAGVFBOX  := 0]
  dt_EXTR_AC_FINAL_POSPAGO[is.na(SUM_FLAGFUTBOL), SUM_FLAGFUTBOL := 0]
  
  # Reduce
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_VOZ        := max(SUM_VOZ),        by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_ADSL       := max(SUM_ADSL),       by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_HZ         := max(SUM_HZ),         by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_FTTH       := max(SUM_FTTH),       by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_LPD        := max(SUM_LPD),        by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_TIVO       := max(SUM_TIVO),       by = x_num_ident]
  #dt_EXTR_AC_FINAL_POSPAGO[, SUM_FLAGVFBOX  := max(SUM_FLAGVFBOX),  by = x_num_ident]
  dt_EXTR_AC_FINAL_POSPAGO[, SUM_FLAGFUTBOL := max(SUM_FLAGFUTBOL), by = x_num_ident]
  
  dt_EXTR_AC_FINAL_POSPAGO[, ARPU := sum(ARPU), by = x_num_ident]
  
  # FIXME: En dt_EXTR_AC_FINAL_POSPAGO un mismo x_num_ident puede tener varios SEG_CLIENTE diferentes (uno por cada servicio),
  #        y puede pasar con más variables, así que hay decir qué se hace en cada caso
  
  setkey(dt_EXTR_AC_FINAL_POSPAGO, x_num_ident)
  dt_EXTR_AC_FINAL_POSPAGO <- unique(dt_EXTR_AC_FINAL_POSPAGO, by=key(dt_EXTR_AC_FINAL_POSPAGO))
  
  # Select those who has got voice service, but no other service
  dt.MobileOnly <- dt_EXTR_AC_FINAL_POSPAGO[SUM_VOZ > 0 & SUM_ADSL == 0 & SUM_HZ == 0 & SUM_FTTH == 0 & SUM_LPD == 0 & 
                                         SUM_TIVO == 0 & 
                                         SUM_FLAGFUTBOL == 0 
                                         #& SUM_FLAGVFBOX == 0,
                                         ]

  rm(dt_EXTR_AC_FINAL_POSPAGO)
  dev.null <- gc()
  
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
  
  if (! is.null(dt_EXTR_NIFS_COMPARTIDOS)) {
    # Filter out those who appear in Ono stack (what means that they has some fixed service)
    cat("[INFO] Removing from mobile-only", nrow(dt.MobileOnly[x_num_ident %in% dt_EXTR_NIFS_COMPARTIDOS$NIF]), "Vodafone mobile-only that are also in Ono stack\n")
    dt.MobileOnly <- dt.MobileOnly[!x_num_ident %in% dt_EXTR_NIFS_COMPARTIDOS$NIF]
  }
  
  ofolder <- file.path(datasets.folder, month)
  if (!file.exists(ofolder)) {
    cat("[INFO] Creating Folder\n")
    dir.create(ofolder, recursive = T)
  }
  
  #ofile <- file.path(ofolder, paste0("MobileOnly-", month, ".csv"))
  #cat("[SAVE] ", ofile, "-", nrow(dt.MobileOnly), "\n")
  #write.table(dt.MobileOnly, file = ofile, row.names = F, sep ="|", quote = F)

  ofile <- file.path(ofolder, paste0("dt.MobileOnly-", month, ".RData"))
  cat("[SAVE] ", ofile, "-", nrow(dt.MobileOnly), "\n")
  save(dt.MobileOnly, file = ofile)
  
  dt.MobileOnly.nifs <- dt.MobileOnly[, .(x_num_ident)]
  
  #ofile <- file.path(ofolder, paste0("MobileOnly-NIFS-", month, ".csv"))
  #cat("[SAVE] ", ofile, "-", nrow(dt.MobileOnly.nifs), "\n")
  #write.table(dt.MobileOnly.nifs, file = ofile, row.names = F, sep ="|", quote = F)
  
  ofile <- file.path(ofolder, paste0("dt.MobileOnly-NIFS-", month, ".RData"))
  cat("[SAVE] ", ofile, "-", nrow(dt.MobileOnly.nifs), "\n")
  save(dt.MobileOnly.nifs, file = ofile)
  
  invisible(dt.MobileOnly)
}
#---------------------------------------------------------------------------------------------------
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
    main_02_get_mobile_only_postpaid(opt$month, NULL, NULL)
  }
  # Buscar las campañas que se les han mandado a los clientes MobileOnly y que no tuvieron exito
  # para ser covergentes.
  # 
}
