source("configuration.R")

# First month with data from Genesis
genesis.init.month <- "201611"
genesis.init.first.day.month.posix <- as.POSIXct(strptime(paste0(genesis.init.month, "01"), "%Y%m%d"))

#dt.data <- copy(dt.BDC_GENESIS)
#View(head(dt.data, 100))

cleanData <- function(dt.data) {
  cat("[INFO]  CleanData() \n")
  
  # Get the last day of the month
  # month <- paste0(month, "01")
  # month.posix <- as.POSIXct(strptime(month, "%Y%m%d"))
  # last.day <- seq.POSIXt(month.posix, length=2, by="months")[2]
  
  # dt.data[, FECHA_LLAM := as.POSIXct(strptime(FECHA_LLAM, "%d/%m/%Y %H:%M:%S"))]
  # #dt.data[, IS_NA_FECHA_LLAM := 0]
  # #dt.data[is.na(FECHA_LLAM), IS_NA_FECHA_LLAM := 1]

  dt.data[, AGENTE := as.factor(AGENTE)]
  dt.data[, FINALIZ_TELEF := as.factor(FINALIZ_TELEF)]
  
  dt.data[, FINALIZ_NEGO := tolower(FINALIZ_NEGO)] # Factor w/ 426 levels -> Factor w/ 392 levels
  dt.data[, FINALIZ_NEGO := as.factor(FINALIZ_NEGO)]
  
  dt.data[, TIPO_CAMP := as.factor(TIPO_CAMP)]
  dt.data[, CAMPAÑA := as.factor(CAMPAÑA)]
  dt.data[, AGENCIA := as.factor(AGENCIA)]
  dt.data[, CENTRO := as.factor(CENTRO)]
  
  # dt.data[, FECHA_RELLAM := as.POSIXct(strptime(FECHA_RELLAM, "%d/%m/%Y %H:%M"))]
  # dt.data[, IS_NA_FECHA_RELLAM := 0]
  # dt.data[is.na(FECHA_RELLAM), IS_NA_FECHA_RELLAM := 1]
  # 
  # dt.data[, T_HABLANDO := as.POSIXct(strptime(paste0("1970-01-01", T_HABLANDO), "%Y-%m-%d %H:%M:%S"))]
  # dt.data[, IS_NA_T_HABLANDO := 0]
  # dt.data[is.na(T_HABLANDO), IS_NA_T_HABLANDO := 1]
  # 
  # dt.data[, T_ACW := as.POSIXct(strptime(paste0("1970-01-01", T_ACW), "%Y-%m-%d %H:%M:%S"))]
  # dt.data[, IS_NA_T_ACW := 0]
  # dt.data[is.na(T_ACW), IS_NA_T_ACW := 1]
  
  # Same as FECHA_LLAM
  # dt.data[, FECHA_1_INTEN := as.POSIXct(strptime(FECHA_1_INTEN, "%d/%m/%Y"))]
  # dt.data[, IS_NA_FECHA_1_INTEN := 0]
  # dt.data[is.na(FECHA_1_INTEN), IS_NA_FECHA_1_INTEN := 1]
  # 
  # dt.data[, HORA_1_INTEN := as.POSIXct(strptime(HORA_1_INTEN, "%H:%M:%S"))]
  # dt.data[, IS_NA_HORA_1_INTEN := 0]
  # dt.data[is.na(HORA_1_INTEN), IS_NA_HORA_1_INTEN := 1]
  
  setkey(dt.data, TELEFONO, FECHA_LLAM)
  
  dt.data <- dt.data[TELEFONO != ""]
  dt.data <- dt.data[TELEFONO != "#N/A"]
  dt.data <- dt.data[TELEFONO != "..."]
  dt.data <- dt.data[TELEFONO != "0"]
  dt.data <- dt.data[TELEFONO != "000000000"]
  dt.data <- dt.data[TELEFONO != "?"]
  
  return(dt.data)
}

#dt.data.bak <- copy(dt.data)
#View(head(dt.data.bak, 100))
#View(head(dt.data, 100))

prepareData <- function(dt.data) {
  cat("[INFO]  prepareData() \n")
  
  dt.data <- dt.data[TIPO_CAMP == "TELEVENTA"] #   AMDOCS C2C TELEVENTA USAC
  
  setkey(dt.data, TELEFONO, FECHA_LLAM)
  
  dt.data[, WDAY := as.POSIXlt(FECHA_LLAM)$wday]
  
  table(dt.data$FINALIZ_TELEF)
  table(dt.data$FINALIZ_NEGO)
  table(dt.data$TIPO_CAMP)
  
  dt.data[, CONTACTADO := 0]
  dt.data[FINALIZ_TELEF == "Contactado", CONTACTADO := 1]
  
  dt.data[, ACEPTA := 0]
  dt.data[FINALIZ_NEGO == "acepta" | FINALIZ_NEGO == "acepta futbol total 20" | FINALIZ_NEGO == "acepta liga 6" | 
          FINALIZ_NEGO == "acepta oferta" | FINALIZ_NEGO == "acepta-dto 25%" | FINALIZ_NEGO == "acepta-migra 120 mb" | 
          FINALIZ_NEGO == "acepta-migra 120 mb + ilimitada" | FINALIZ_NEGO == "acepta-migra 300 mb" | 
          FINALIZ_NEGO == "acepta-migra 300 mb + ilimitada" | FINALIZ_NEGO == "acepta-migra 50 mb" | 
          FINALIZ_NEGO == "acepta-migra 50 mb + ilimitada" | FINALIZ_NEGO == "cliente acepta oferta" | 
          FINALIZ_NEGO == "contrata (f_u_p)" | FINALIZ_NEGO == "util acepta oferta", 
          ACEPTA := 1]
  
  dt.data <- dt.data[!(CONTACTADO == 0 & ACEPTA == 1)]
  
  # ¿ no pertece nãºmero, programado a otro telefono ?
  dt.data[, NO_TITULAR := 0]
  dt.data[FINALIZ_NEGO == "contacto no titular" | FINALIZ_NEGO == "dejo aviso no titular" | FINALIZ_NEGO == "menor de edad" | 
          FINALIZ_NEGO == "no contacto-no titular" | FINALIZ_NEGO == "no es el titular (f_u_n)" | 
          FINALIZ_NEGO == "no es el titular de la linea" | FINALIZ_NEGO == "no es titular" | 
          FINALIZ_NEGO == "no titular" | FINALIZ_NEGO == "fax", 
          NO_TITULAR := 1]
  
  # ¿ telefono de prepago, telefono de pymes ?
  dt.data[, DESCARTE := 0]
  dt.data[FINALIZ_NEGO == "no acepta reciente renovacion con vf" | FINALIZ_NEGO == "no util_ya cliente" | 
          FINALIZ_NEGO == "oferta aplicada a nombre de otro titular" | FINALIZ_NEGO == "oferta aplicada-ya tiene fibra" | 
          FINALIZ_NEGO == "oferta ya aplicada o en curso" | FINALIZ_NEGO == "ya cliente (f_nu_n)" | 
          FINALIZ_NEGO == "ya gestionado" | FINALIZ_NEGO == "ya negociado" | FINALIZ_NEGO == "ya resuelto-aplicado" |
          FINALIZ_NEGO == "ya tengo promocion", 
          DESCARTE := 1]
  
  #dt.data[TELEFONO == 626534361]
  #dt.data[TELEFONO == 678763812]
  #dt.data[TELEFONO == 610284727]
  #dt.data[TELEFONO == 600000000]
  #dt.data[TELEFONO == 671052617]
  
  return(dt.data)
}

channelPropensityByMSISDN <- function(dt.data, dt_EXTR_AC_FINAL_POSPAGO) {
  cat("[INFO]  channelPropensityByMSISDN() \n")
  
  setkey(dt.data, TELEFONO)
  
  #dt.data.contactados[TELEFONO == 626534361, .(NUM_INTENTOS = .N), by = TELEFONO] # NUM_CONTACTOS
  #dt.data.contactados[TELEFONO == 626534361 & FINALIZ_TELEF == "Contactado", .(NUM_CONTACTOS = .N), by = TELEFONO] # NUM_CONTACTOS
  
  dt.intentos     <- dt.data[, .(NUM_INTENTOS = .N), by = TELEFONO]
  #dt.intentos[NUM_INTENTOS == 0]
  dt.contactos    <- dt.data[CONTACTADO == 1, .(NUM_CONTACTOS = .N), by = TELEFONO]
  dt.aceptaciones <- dt.data[ACEPTA == 1, .(NUM_ACEPTACIONES = .N), by = TELEFONO]
  
  dt.channel.propensity <- dt.contactos[dt.intentos]
  dt.channel.propensity <- dt.aceptaciones[dt.channel.propensity]
  #nrow(dt.channel.propensity[is.na(NUM_CONTACTOS)])
  dt.channel.propensity[is.na(NUM_CONTACTOS), NUM_CONTACTOS := 0]
  #nrow(dt.channel.propensity[is.na(NUM_ACEPTACIONES)])
  dt.channel.propensity[is.na(NUM_ACEPTACIONES), NUM_ACEPTACIONES := 0]
  #head(dt.channel.propensity)
  
  # Trying rate
  dt.channel.propensity[, TASA_INTENTO := 1/NUM_INTENTOS - 1]
  #dt.channel.propensity[, TASA_INTENTO := -log(-1/NUM_INTENTOS + 2]
  dt.channel.propensity[TASA_INTENTO == 0, TASA_INTENTO := -0.1]
  summary(dt.channel.propensity$TASA_INTENTO)
  #dt.channel.propensity[is.na(TASA_INTENTO)]
  
  # Contact propensity
  dt.channel.propensity[, TASA_CONTACTO := NUM_CONTACTOS/NUM_INTENTOS]
  summary(dt.channel.propensity$TASA_CONTACTO)
  #dt.channel.propensity[is.na(TASA_CONTACTO)]
  
  # Acceptance propensity
  dt.channel.propensity[, TASA_ACEPTACION := NUM_ACEPTACIONES/NUM_CONTACTOS]
  dt.channel.propensity[is.na(TASA_ACEPTACION), TASA_ACEPTACION := 0]
  summary(dt.channel.propensity$TASA_ACEPTACION)
  #dt.channel.propensity[is.infinite(TASA_ACEPTACION)]
  
  # Telesales propensity
  dt.channel.propensity <- dt.channel.propensity[, PROP_CANAL_TELEVENTA := 0]
  dt.channel.propensity <- dt.channel.propensity[TASA_CONTACTO == 0, 
                                                 PROP_CANAL_TELEVENTA := TASA_INTENTO]
  summary(dt.channel.propensity$PROP_CANAL_TELEVENTA)
  dt.channel.propensity <- dt.channel.propensity[TASA_CONTACTO > 0, 
                                                 PROP_CANAL_TELEVENTA := TASA_CONTACTO]
  summary(dt.channel.propensity$PROP_CANAL_TELEVENTA)
  dt.channel.propensity <- dt.channel.propensity[TASA_ACEPTACION > 0, 
                                                 PROP_CANAL_TELEVENTA := 1 + TASA_ACEPTACION]
  summary(dt.channel.propensity$PROP_CANAL_TELEVENTA)
  
  # nrow(dt.channel.propensity[TASA_CONTACTO == 0])
  # nrow(dt.channel.propensity[(PROP_CANAL_TELEVENTA > 0) & (PROP_CANAL_TELEVENTA < 1)])
  # nrow(dt.channel.propensity[TASA_CONTACTO > 0])
  # nrow(dt.channel.propensity[(PROP_CANAL_TELEVENTA >= 1) & (PROP_CANAL_TELEVENTA <= 2)])
  # nrow(dt.channel.propensity[TASA_ACEPTACION > 0])
  # nrow(dt.channel.propensity[(PROP_CANAL_TELEVENTA > 2)])
  
  #dt.channel.propensity <- dt.channel.propensity[, PROP_CANAL_TELEVENTA := PROP_CANAL_TELEVENTA/max(PROP_CANAL_TELEVENTA)]
  #summary(dt.channel.propensity$PROP_CANAL_TELEVENTA)
  
  setnames(dt.channel.propensity, "TELEFONO", "x_id_red")
  setkey(dt.channel.propensity, x_id_red)
  
  # Join with ID
  
  #dt_EXTR_AC_FINAL_POSPAGO[x_num_ident == "XXX", .(NUMS = list(x_id_red)), by = x_num_ident]
  #nums_nifs <- dt_EXTR_AC_FINAL_POSPAGO[, .(x_num_ident, ARPU), by = x_id_red]
  nums_nifs <- dt_EXTR_AC_FINAL_POSPAGO[, .(x_num_ident), by = x_id_red]
  setkey(nums_nifs, x_id_red)
  head(nums_nifs)

  #dt.channel.propensity <- merge(nums_nifs, dt.data, all=TRUE)
  #dt.channel.propensity <- dt.data[nums_nifs]
  dt.channel.propensity <- nums_nifs[dt.channel.propensity]
  
  # TODO: Ademas de la AC_FINAL_POSPAGO se debería hacer join con otras tablas
  # en donde puedan figurar MSISDNs con sus respectivos NIFS, 
  # p.ej. PREPAGO, ONO y empresas
  
  # Just for proper ordering
  # dt.channel.propensity <- dt.channel.propensity[, INV_PROP_CONTACTO := 1-PROP_CANAL_TELEVENTA]
  # setkey(dt.channel.propensity, INV_PROP_CONTACTO)
  # dt.channel.propensity <- dt.channel.propensity[, INV_PROP_CONTACTO := NULL]
  
  return(dt.channel.propensity)
}

channelPropensityByID <- function(dt.data) {
  cat("[INFO]  channelPropensityByID() \n")
  
  dt.data <- dt.data[!is.na(x_num_ident)]
  setkey(dt.data, x_num_ident)
  nrow(dt.data[! is.na(x_id_red)])
  nrow(dt.data[is.na(PROP_CANAL_TELEVENTA)])
  head(dt.data)
  
  # dt.data <- dt.data[! is.na(PROP_CANAL_TELEVENTA)]
  # head(dt.data)
  
  #dt.data[!is.na(dt.data$x_num_ident)]
  
  dt.channel.propensity.bynif <- dt.data[, .(LIST_MSISDN = list(x_id_red),
                                                           SUM_INTENTOS        = sum(NUM_INTENTOS),
                                                           SUM_CONTACTOS       = sum(NUM_CONTACTOS),
                                                           SUM_ACEPTACIONES    = sum(NUM_ACEPTACIONES),
                                                           MAX_INTENTOS        = max(NUM_INTENTOS),
                                                           MAX_CONTACTOS       = max(NUM_CONTACTOS),
                                                           MAX_ACEPTACIONES    = max(NUM_ACEPTACIONES),
                                                           MAX_TASA_INTENTO    = max(TASA_INTENTO),
                                                           MAX_TASA_CONTACTO   = max(TASA_CONTACTO),
                                                           MAX_TASA_ACEPTACION = max(TASA_ACEPTACION),
                                                           MAX_PROP_CANAL_TELEVENTA = max(PROP_CANAL_TELEVENTA)),
                                            by = x_num_ident]
  dt.channel.propensity.bynif[, NUM_MSISDN_BY_ID := length(unlist(LIST_MSISDN)), by = x_num_ident]
  #dt.channel.propensity.bynif[, LIST_MSISDN2 := paste0(unlist(LIST_MSISDN), collapse = ","), by = x_num_ident]
  dt.channel.propensity.bynif[, TASA_INTENTO := 0]
  dt.channel.propensity.bynif[, TASA_CONTACTO := 0]
  dt.channel.propensity.bynif[, TASA_ACEPTACION := 0]
  dt.channel.propensity.bynif[SUM_INTENTOS  == 0, TASA_INTENTO    := -0.1,     by = x_num_ident]
  dt.channel.propensity.bynif[SUM_INTENTOS   > 0, TASA_INTENTO    := 1/SUM_INTENTOS - 1,     by = x_num_ident]
  dt.channel.propensity.bynif[SUM_INTENTOS   > 0, TASA_CONTACTO   := SUM_CONTACTOS/SUM_INTENTOS,     by = x_num_ident]
  dt.channel.propensity.bynif[SUM_CONTACTOS  > 0, TASA_ACEPTACION := SUM_ACEPTACIONES/SUM_CONTACTOS, by = x_num_ident]
  
  dt.channel.propensity.bynif[TASA_CONTACTO   == 0, PROP_CANAL_TELEVENTA := TASA_INTENTO, by = x_num_ident]
  dt.channel.propensity.bynif[TASA_CONTACTO    > 0, PROP_CANAL_TELEVENTA := TASA_CONTACTO, by = x_num_ident]
  dt.channel.propensity.bynif[TASA_ACEPTACION  > 0, PROP_CANAL_TELEVENTA := 1 + TASA_ACEPTACION, by = x_num_ident]
  
  setkey(dt.channel.propensity.bynif, x_num_ident)
  #summary(dt.channel.propensity.bynif[NUM_MSISDN_BY_ID >= 2, .(NUM_MSISDN_BY_ID)])
  #View(head(dt.channel.propensity.bynif, 100))
  
  # dt.channel.propensity.bynif <- dt.data[, .(PROP_CANAL_TELEVENTA_BY_ID = max(PROP_CANAL_TELEVENTA)), 
  #                                                              by = x_num_ident]
  # nrow(dt.channel.propensity.bynif)
  # nrow(dt.channel.propensity.bynif[is.na(PROP_CANAL_TELEVENTA_BY_ID)])
  # nrow(dt.channel.propensity.bynif[!is.na(PROP_CANAL_TELEVENTA_BY_ID)])
  # head(dt.channel.propensity.bynif)
  # 
  # # Just for proper ordering
  # dt.channel.propensity.bynif <- dt.channel.propensity.bynif[, INV_PROP_CANAL_TELEVENTA_BY_ID := 1-PROP_CANAL_TELEVENTA_BY_ID]
  # setkey(dt.channel.propensity.bynif, INV_PROP_CANAL_TELEVENTA_BY_ID)
  # dt.channel.propensity.bynif <- dt.channel.propensity.bynif[, INV_PROP_CANAL_TELEVENTA_BY_ID := NULL]
  
  return(dt.channel.propensity.bynif)
}

decisionMaker <- function(dt.data, dt_EXTR_AC_FINAL_POSPAGO) {
}

wdayPropensity <- function(dt.data) {
  # TODO
  # Weekday propensity
  #dt.data[TELEFONO == 626534361 & FINALIZ_TELEF == "Contactado"]
}


main_02_channel_propensity <- function(month, dt.BDC_GENESIS = NULL, dt_EXTR_AC_FINAL_POSPAGO = NULL) {
  #month <- "201612"
  cat ("\n[MAIN] 02_channel_propensity", month, "\n")
  
  # Get the first day of the month
  first.day.month <- paste0(month, "01")
  first.day.month.posix <- as.POSIXct(strptime(first.day.month, "%Y%m%d"))
  
  if (genesis.init.first.day.month.posix <= first.day.month.posix) {
    # First calculate metrics for the given month
    
    ifile <- file.path(data.folder, "BDC_GENESIS", month, paste0("dt.BDC_GENESIS_", month, ".RData"))
    if (!exists("dt.BDC_GENESIS") || is.null(dt.BDC_GENESIS)) {
      cat("[LOAD] ", ifile, "\n")
      load(ifile)
      #summary(dt.BDC_GENESIS)
      #str(as.factor(dt.BDC_GENESIS$FINALIZ_NEGO))
    } else {
      cat("[INFO] Skipping", ifile, ", already loaded\n")
    }
  
    ifile <- file.path(data.folder, "CVM", month, paste0("dt_EXTR_AC_FINAL_POSPAGO_", month, ".RData"))
    if (!exists("dt_EXTR_AC_FINAL_POSPAGO") || is.null(dt_EXTR_AC_FINAL_POSPAGO)) {
      cat("[LOAD] ", ifile, "\n")
      load(ifile)
    } else {
      cat("[INFO] Skipping", ifile, ", already loaded\n")
    }
    
    dt.data.clean <- copy(dt.BDC_GENESIS)
    dt.data.clean <- cleanData(dt.data.clean)
    head(dt.data.clean)
    #str(as.factor(dt.data.clean$FINALIZ_NEGO))
    #table(dt.data$FINALIZ_NEGO)
    
    dt.data.prepared <- copy(dt.data.clean)
    dt.data.prepared <- prepareData(dt.data.prepared)
    head(dt.data.prepared)
    
    
    # Aggregate by MSISDN
    
    dt.contacts.bymsisdn <- copy(dt.data.prepared)
    dt.contacts.bymsisdn <- channelPropensityByMSISDN(dt.contacts.bymsisdn, dt_EXTR_AC_FINAL_POSPAGO)
    nrow(dt.contacts.bymsisdn)
    nrow(dt.contacts.bymsisdn[is.na(x_num_ident)])
    nrow(dt.contacts.bymsisdn[!is.na(x_num_ident)])
    head(dt.contacts.bymsisdn[!is.na(x_num_ident)])
    tail(dt.contacts.bymsisdn[!is.na(x_num_ident)])
    
    ofolder <- file.path(data.folder, "Contacts", month)
    if (!file.exists(ofolder)) {
      cat("[INFO]  Creating Folder\n")
      dir.create(ofolder, recursive = T)
    }
    
    ofile <- file.path(ofolder, paste0("dt.contacts.bymsisdn-", month, ".RData"))
    cat("[SAVE] ", ofile, "-", nrow(dt.contacts.bymsisdn), "\n")
    save(dt.contacts.bymsisdn, file = ofile)
    
    
    # Aggregate by ID
    
    dt.contacts.byid <- copy(dt.contacts.bymsisdn)
    dt.contacts.byid <- dt.contacts.byid[!is.na(x_num_ident)]
    dt.contacts.byid <- channelPropensityByID(dt.contacts.byid)
    head(dt.contacts.byid)
    tail(dt.contacts.byid)
    
    ofile <- file.path(ofolder, paste0("dt.contacts.byid-", month, ".RData"))
    cat("[SAVE] ", ofile, "-", nrow(dt.contacts.byid), "\n")
    save(dt.contacts.byid, file = ofile)
    
    if (genesis.init.first.day.month.posix == first.day.month.posix) {
      # If it is the first month, historic metrics will be those of the first month
      
      dt.contacts.hist.byid <- dt.contacts.byid
      
      dt.contacts.hist.byid[, GENESIS_HIST_NUM_INTENTOS        := SUM_INTENTOS]
      dt.contacts.hist.byid[, GENESIS_HIST_NUM_CONTACTOS       := SUM_CONTACTOS]
      dt.contacts.hist.byid[, GENESIS_HIST_NUM_ACEPTACIONES    := SUM_ACEPTACIONES]
      dt.contacts.hist.byid[, GENESIS_HIST_TASA_INTENTO        := TASA_INTENTO]
      dt.contacts.hist.byid[, GENESIS_HIST_TASA_CONTACTO       := TASA_CONTACTO]
      dt.contacts.hist.byid[, GENESIS_HIST_TASA_ACEPTACION     := TASA_ACEPTACION]
      dt.contacts.hist.byid[, GENESIS_HIST_PROP_CANAL_TELEVENTA := PROP_CANAL_TELEVENTA]
      dt.contacts.hist.byid[, GENESIS_HIST_MAX_INTENTOS        := MAX_INTENTOS]
      dt.contacts.hist.byid[, GENESIS_HIST_MAX_CONTACTOS       := MAX_CONTACTOS]
      dt.contacts.hist.byid[, GENESIS_HIST_MAX_ACEPTACIONES    := MAX_ACEPTACIONES]
      dt.contacts.hist.byid[, GENESIS_HIST_MAX_TASA_INTENTO    := MAX_TASA_INTENTO]
      dt.contacts.hist.byid[, GENESIS_HIST_MAX_TASA_CONTACTO   := MAX_TASA_CONTACTO]
      dt.contacts.hist.byid[, GENESIS_HIST_MAX_TASA_ACEPTACION := MAX_TASA_ACEPTACION]
      dt.contacts.hist.byid[, GENESIS_HIST_MAX_PROP_CANAL_TELEVENTA := MAX_PROP_CANAL_TELEVENTA]
      
      dt.contacts.hist.byid <- dt.contacts.hist.byid[, .(x_num_ident,
                                                         GENESIS_HIST_NUM_INTENTOS,
                                                         GENESIS_HIST_NUM_CONTACTOS,
                                                         GENESIS_HIST_NUM_ACEPTACIONES,
                                                         GENESIS_HIST_TASA_INTENTO,
                                                         GENESIS_HIST_TASA_CONTACTO,
                                                         GENESIS_HIST_TASA_ACEPTACION,
                                                         GENESIS_HIST_PROP_CANAL_TELEVENTA,
                                                         GENESIS_HIST_MAX_INTENTOS,
                                                         GENESIS_HIST_MAX_CONTACTOS,
                                                         GENESIS_HIST_MAX_ACEPTACIONES,
                                                         GENESIS_HIST_MAX_TASA_INTENTO,
                                                         GENESIS_HIST_MAX_TASA_CONTACTO,
                                                         GENESIS_HIST_MAX_TASA_ACEPTACION,
                                                         GENESIS_HIST_MAX_PROP_CANAL_TELEVENTA)]
      
      ofile <- file.path(ofolder, paste0("dt.contacts.hist.byid-", month, ".RData"))
      cat("[SAVE] ", ofile, "-", nrow(dt.contacts.hist.byid), "\n")
      save(dt.contacts.hist.byid, file = ofile)
    } else {
      # If it is not the first month, calculate historic metrics
      
      # Get previous month
      first.day.prev.month.posix <- seq.POSIXt(first.day.month.posix, length=2, by="-1 months")[2]
      prev.month <- format(first.day.prev.month.posix, format = "%Y%m")
      
      ofolder <- file.path(data.folder, "Contacts", prev.month)
      ifile <- file.path(ofolder, paste0("dt.contacts.hist.byid-", prev.month, ".RData"))
      cat("[LOAD] ", ifile, "\n")
      load(ifile)
      head(dt.contacts.hist.byid)
      nrow(dt.contacts.hist.byid)
      
      dt.contacts.hist.byid <- merge(dt.contacts.hist.byid, dt.contacts.byid, all=TRUE)
      head(dt.contacts.hist.byid)
      nrow(dt.contacts.hist.byid)

      # dt.contacts.hist.byid[is.na(GENESIS_HIST_NUM_INTENTOS),        GENESIS_HIST_NUM_INTENTOS        := 0]
      # dt.contacts.hist.byid[is.na(GENESIS_HIST_NUM_CONTACTOS),       GENESIS_HIST_NUM_CONTACTOS       := 0]
      # dt.contacts.hist.byid[is.na(GENESIS_HIST_NUM_ACEPTACIONES),    GENESIS_HIST_NUM_ACEPTACIONES    := 0]
      # dt.contacts.hist.byid[is.na(GENESIS_HIST_TASA_INTENTO),        GENESIS_HIST_TASA_INTENTO        := 0]
      # dt.contacts.hist.byid[is.na(GENESIS_HIST_TASA_CONTACTO),       GENESIS_HIST_TASA_CONTACTO       := 0]
      # dt.contacts.hist.byid[is.na(GENESIS_HIST_TASA_ACEPTACION),     GENESIS_HIST_TASA_ACEPTACION     := 0]
      # dt.contacts.hist.byid[is.na(GENESIS_HIST_MAX_INTENTOS),        GENESIS_HIST_MAX_INTENTOS        := 0]
      # dt.contacts.hist.byid[is.na(GENESIS_HIST_MAX_CONTACTOS),       GENESIS_HIST_MAX_CONTACTOS       := 0]
      # dt.contacts.hist.byid[is.na(GENESIS_HIST_MAX_ACEPTACIONES),    GENESIS_HIST_MAX_ACEPTACIONES    := 0]
      # dt.contacts.hist.byid[is.na(GENESIS_HIST_MAX_TASA_INTENTO),    GENESIS_HIST_MAX_TASA_INTENTO    := 0]
      # dt.contacts.hist.byid[is.na(GENESIS_HIST_MAX_TASA_CONTACTO),   GENESIS_HIST_MAX_TASA_CONTACTO   := 0]
      # dt.contacts.hist.byid[is.na(GENESIS_HIST_MAX_TASA_ACEPTACION), GENESIS_HIST_MAX_TASA_ACEPTACION := 0]
      # dt.contacts.hist.byid[is.na(GENESIS_HIST_MAX_PROP_CANAL_TELEVENTA), GENESIS_HIST_MAX_PROP_CANAL_TELEVENTA := 0]
      
      dt.contacts.hist.byid[, GENESIS_HIST_NUM_INTENTOS        := sum(GENESIS_HIST_NUM_INTENTOS,        SUM_INTENTOS, na.rm = TRUE),     by = x_num_ident]
      dt.contacts.hist.byid[, GENESIS_HIST_NUM_CONTACTOS       := sum(GENESIS_HIST_NUM_CONTACTOS,       SUM_CONTACTOS, na.rm = TRUE),    by = x_num_ident]
      dt.contacts.hist.byid[, GENESIS_HIST_NUM_ACEPTACIONES    := sum(GENESIS_HIST_NUM_ACEPTACIONES,    SUM_ACEPTACIONES, na.rm = TRUE), by = x_num_ident]
      dt.contacts.hist.byid[, GENESIS_HIST_MAX_INTENTOS        := max(GENESIS_HIST_MAX_INTENTOS,        SUM_INTENTOS, na.rm = TRUE),     by = x_num_ident]
      dt.contacts.hist.byid[, GENESIS_HIST_MAX_CONTACTOS       := max(GENESIS_HIST_MAX_CONTACTOS,       SUM_CONTACTOS, na.rm = TRUE),    by = x_num_ident]
      dt.contacts.hist.byid[, GENESIS_HIST_MAX_ACEPTACIONES    := max(GENESIS_HIST_MAX_ACEPTACIONES,    SUM_ACEPTACIONES, na.rm = TRUE), by = x_num_ident]
      dt.contacts.hist.byid[, GENESIS_HIST_MAX_TASA_INTENTO    := max(GENESIS_HIST_MAX_TASA_INTENTO,    TASA_INTENTO, na.rm = TRUE),    by = x_num_ident]
      dt.contacts.hist.byid[, GENESIS_HIST_MAX_TASA_CONTACTO   := max(GENESIS_HIST_MAX_TASA_CONTACTO,   TASA_CONTACTO, na.rm = TRUE),    by = x_num_ident]
      dt.contacts.hist.byid[, GENESIS_HIST_MAX_TASA_ACEPTACION := max(GENESIS_HIST_MAX_TASA_ACEPTACION, TASA_ACEPTACION, na.rm = TRUE),  by = x_num_ident]
      dt.contacts.hist.byid[, GENESIS_HIST_MAX_PROP_CANAL_TELEVENTA := max(GENESIS_HIST_MAX_PROP_CANAL_TELEVENTA, MAX_PROP_CANAL_TELEVENTA, na.rm = TRUE),  by = x_num_ident]
      
      dt.contacts.hist.byid[, GENESIS_HIST_TASA_INTENTO    := 0]
      dt.contacts.hist.byid[, GENESIS_HIST_TASA_CONTACTO   := 0]
      dt.contacts.hist.byid[, GENESIS_HIST_TASA_ACEPTACION := 0]
      dt.contacts.hist.byid[GENESIS_HIST_NUM_INTENTOS  == 0, GENESIS_HIST_TASA_INTENTO    := -0.1,     by = x_num_ident]
      dt.contacts.hist.byid[GENESIS_HIST_NUM_INTENTOS   > 0, GENESIS_HIST_TASA_INTENTO    := 1/GENESIS_HIST_NUM_INTENTOS - 1,     by = x_num_ident]
      dt.contacts.hist.byid[GENESIS_HIST_NUM_INTENTOS   > 0, GENESIS_HIST_TASA_CONTACTO   := GENESIS_HIST_NUM_CONTACTOS/GENESIS_HIST_NUM_INTENTOS,     by = x_num_ident]
      dt.contacts.hist.byid[GENESIS_HIST_NUM_CONTACTOS  > 0, GENESIS_HIST_TASA_ACEPTACION := GENESIS_HIST_NUM_ACEPTACIONES/GENESIS_HIST_NUM_CONTACTOS, by = x_num_ident]
      
      dt.contacts.hist.byid[, GENESIS_HIST_PROP_CANAL_TELEVENTA := 0]
      dt.contacts.hist.byid[GENESIS_HIST_TASA_CONTACTO  == 0, GENESIS_HIST_PROP_CANAL_TELEVENTA := GENESIS_HIST_TASA_INTENTO, by = x_num_ident]
      dt.contacts.hist.byid[GENESIS_HIST_TASA_CONTACTO   > 0, GENESIS_HIST_PROP_CANAL_TELEVENTA := GENESIS_HIST_TASA_CONTACTO, by = x_num_ident]
      dt.contacts.hist.byid[GENESIS_HIST_TASA_ACEPTACION > 0, GENESIS_HIST_PROP_CANAL_TELEVENTA := 1 + GENESIS_HIST_TASA_ACEPTACION, by = x_num_ident]
      
      dt.contacts.hist.byid <- dt.contacts.hist.byid[, .(x_num_ident,
                                                         GENESIS_HIST_NUM_INTENTOS,
                                                         GENESIS_HIST_NUM_CONTACTOS,
                                                         GENESIS_HIST_NUM_ACEPTACIONES,
                                                         GENESIS_HIST_TASA_INTENTO,
                                                         GENESIS_HIST_TASA_CONTACTO,
                                                         GENESIS_HIST_TASA_ACEPTACION,
                                                         GENESIS_HIST_PROP_CANAL_TELEVENTA,
                                                         GENESIS_HIST_MAX_INTENTOS,
                                                         GENESIS_HIST_MAX_CONTACTOS,
                                                         GENESIS_HIST_MAX_ACEPTACIONES,
                                                         GENESIS_HIST_MAX_TASA_INTENTO,
                                                         GENESIS_HIST_MAX_TASA_CONTACTO,
                                                         GENESIS_HIST_MAX_TASA_ACEPTACION,
                                                         GENESIS_HIST_MAX_PROP_CANAL_TELEVENTA)]
      
      head(dt.contacts.hist.byid)
      nrow(dt.contacts.hist.byid)
      
      ofolder <- file.path(data.folder, "Contacts", month)
      ofile <- file.path(ofolder, paste0("dt.contacts.hist.byid-", month, ".RData"))
      cat("[SAVE] ", ofile, "-", nrow(dt.contacts.hist.byid), "\n")
      save(dt.contacts.hist.byid, file = ofile)
    }
  }
  
  # source("lift.R")
  # 
  # preds.201612 <- fread("~/fy17/predictions/h2orf.201611.pred.201612.0.831185666683882.csv")
  # setkey(preds.201612, x_num_ident)
  # cat("[INFO] Computing lift\n")
  # print(computeLift(preds.201612$PRED, preds.201612$CLASS))
  # p <- preds.201612[dt.contacts.byid]
  # p <- p[!is.na(CLASS)]
  # #p[is.na(PRED), PRED := 0]
  # head(p)
  # tail(p)
  # 
  # cat("[INFO] Computing lift\n")
  # setkey(p, PROP_CANAL_TELEVENTA_BY_ID)
  # print(computeLift(p$PRED, p$CLASS))
  # print(computeLift(p$PROP_CANAL_TELEVENTA_BY_ID, p$CLASS))
  # p2 <- p[PROP_CANAL_TELEVENTA_BY_ID >= 1]
  # p2 <- p[PROP_CANAL_TELEVENTA_BY_ID >= 0 & PROP_CANAL_TELEVENTA_BY_ID < 1]
  # p2 <- p[PROP_CANAL_TELEVENTA_BY_ID < 0]
  # print(computeLift(p2$PROP_CANAL_TELEVENTA_BY_ID, p2$CLASS))
  # print(computeLift(p$PRED + p$PROP_CANAL_TELEVENTA_BY_ID, p$CLASS))
  # #plot(computeLift(p$PROP_CANAL_TELEVENTA_BY_ID, p$CLASS))
  
  #invisible()
}
#----------------------------------------------------------------------------------------------------------
if (!exists("sourced")) {
  option_list <- list(
    make_option(c("-m", "--month"), type = "character", default = NULL, help = "input month (YYYYMM)", 
                metavar = "character")
  )
  
  opt_parser <- OptionParser(option_list = option_list)
  opt <- parse_args(opt_parser)
  
  if (is.null(opt$month)) {
    print_help(opt_parser)
    stop("At least one parameter must be supplied (input month: YYYYMM)", call. = FALSE)
  } else {
    main_02_channel_propensity(opt$month, NULL, NULL)
  }
}
