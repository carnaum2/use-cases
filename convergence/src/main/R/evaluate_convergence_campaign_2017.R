#!/usr/bin/env Rscript

suppressWarnings(suppressMessages(library(optparse)))
suppressWarnings(suppressMessages(library(data.table)))
suppressWarnings(suppressMessages(library(bit64)))
suppressWarnings(suppressMessages(library(getPass)))
suppressWarnings(suppressMessages(library(RODBC)))

getScriptPath <- function() {
  if (rstudioapi::isAvailable()) {
    script.dir <- dirname(rstudioapi::callFun("getActiveDocumentContext")$path)
  } else {
    cmd.args <- commandArgs()
    m <- regexpr("(?<=^--file=).+", cmd.args, perl=TRUE)
    script.dir <- dirname(regmatches(cmd.args, m))
    if(length(script.dir) == 0) stop("can't determine script dir: please call the script with Rscript")
    if(length(script.dir) > 1) stop("can't determine script dir: more than one '--file' argument detected")
  }
  return(script.dir)
}
#base.dir <- file.path(getScriptPath(), "Evolucion_campañas")
base.dir <- file.path("C:/cygwin/home/bbergua", "Evolucion_campañas")
#cat(base.dir)

query.planificados.campañas.template <- "SELECT
HOC.CIF_NIF,
HOC.CAMPAIGNCODE,
HOC.CELLCODE,
HOC.CANAL,
HOC.CREATIVIDAD,
HOC.contactdatetime DATEID
FROM
clientesp_iq_v.ac75_UA_NIFCONTACTHIST HOC
WHERE
HOC.contactdatetime >= '@INI_DAY@'
AND HOC.contactdatetime < '@END_DAY@'
AND HOC.CAMPAIGNCODE IN ('20160909_PXXXC_FIBRA_GRATIS', 'AUTOMMES_PXXXC_FIBRA_NEBA', 'AUTOMMES_PXXXC_NEBA_ZC', 'AUTOMMES_PXXXC_XSELL_ADSL')
--AND HOC.CAMPAIGNCODE IN ('20160909_PXXXC_FIBRA_GRATIS')
--AND HOC.CREATIVIDAD = 'PROPENSOS_FIBRA'
--AND HOC.CELLCODE LIKE '%BIGDATA'
AND HOC.CANAL NOT LIKE 'PO%'
AND HOC.CANAL NOT LIKE 'NBA%'
AND HOC.CANAL = 'TEL'
AND HOC.CELLCODE NOT LIKE 'CU%'
AND HOC.FLAG_BORRADO = 0"

query.provisionados.template <- "sel distinct ALS_PCS_CLIENTES_QUOTE.CIF_NIF,cast(PCS_QUOTE.SUBMIT_DT AS DATE FORMAT 'YYYYMMDD'),PCS_QUOTE.STATUS
       FROM
CLIENTESP_IQ_V.PCS_CLIENTES ALS_PCS_CLIENTES_QUOTE,
CLIENTESP_IQ_V.PCS_QUOTE_ITEM RIGHT JOIN CLIENTESP_IQ_V.PCS_QUOTE ON PCS_QUOTE_ITEM.ID_QUOTE=PCS_QUOTE.ID_PK,
CLIENTESP_IQ_V.PCS_CUENTAS ALS_PCS_CUENTAS_QUOTE
WHERE
ALS_PCS_CLIENTES_QUOTE.ID_PK = PCS_QUOTE.ID_CLIENTES
AND PCS_QUOTE.ID_CTA_FACTURACION=ALS_PCS_CUENTAS_QUOTE.ID_PK
AND ALS_PCS_CUENTAS_QUOTE.ESTADO_CUENTA<>'BORRADOR' AND ALS_PCS_CUENTAS_QUOTE.ESTADO_CUENTA<>'CANCELADO'
AND ALS_PCS_CLIENTES_QUOTE.ESTADO_CLIENTE<>'BORRADOR' AND ALS_PCS_CLIENTES_QUOTE.ESTADO_CLIENTE<>'CANCELADO'
AND (
PCS_QUOTE_ITEM.ID_PRODUCTOS IN ('DSLHOG', 'FIBHOG')   
AND (ALS_PCS_CUENTAS_QUOTE.TIPO_CUENTA IS NULL OR ALS_PCS_CUENTAS_QUOTE.TIPO_CUENTA NOT LIKE 'PRUEBA%')
AND (PCS_QUOTE.Tipo='Modificar' OR PCS_QUOTE.Tipo='Activación')  --Tipo de solicitud de Fibra: MIGRA O ALTA
AND cast(PCS_QUOTE.SUBMIT_DT AS DATE FORMAT 'YYYYMMDD')>= '@INI_DAY@'--current_date-34  --Último mes
AND cast(PCS_QUOTE.SUBMIT_DT AS DATE FORMAT 'YYYYMMDD')< '@END_DAY@'
-- AND (PCS_QUOTE.STATUS IN ('Cancelado', 'Completado', 'En Curso', 'Pendiente Acción Futura', 'Pendiente de Completar', 'Rechazado')
AND (PCS_QUOTE.STATUS IN ('Completado', 'En Curso', 'Pendiente Acción Futura', 'Pendiente de Completar')
OR  PCS_QUOTE.STATUS='Parcialmente Completado' 
AND PCS_QUOTE_ITEM.STATUS='Completado')
AND PCS_QUOTE_ITEM.ACTION_CODE='Nuevo')"

teradata <- function(query, pass) {
  username <- Sys.getenv("USERNAME")
  if (is.null(pass))
    pass <- getPass(paste0("Enter passoword for user <", username, ">:"))
  
  ch <- odbcDriverConnect(paste0("Driver=Teradata;DBCName=vodaes1-u;UID=", username, ";PWD=", pass))
  
  #res <- sqlQuery(ch,'select count(*) from ADS_TRACK_V.AC_FINAL_POSPAGO')
  res <- data.table(sqlQuery(ch, query))
  
  odbcClose(ch)
  #odbcCloseAll()
  
  res
}

measure_sub_campaign <- function(df.results, month, redeemers.TG, redeemers.CG, size.TG, size.CG, label) {
  if (is.null(df.results)) {
    df.results <- data.frame(Month = character(),
                             Propensity_list_after_filters = integer(),
                             Size_TG = integer(), Size_CG = integer(),
                             Redeemers_TG = integer(), Redeemers_CG = integer(),
                             Conversion_Rate_TG = numeric(), Conversion_Rate_CG = numeric(),
                             Relative_CR_Uplift_vs_CG = numeric(), Absolute_CR_Uplift_vs_CG = numeric(),
                             Campaign_Efficiency = numeric())
  }
  
  #nm <-deparse(substitute(redeemers.TG))
  #print(nm)
  
  propensity.list.after.filters <- size.TG + size.CG
  conversion.Rate.TG <- redeemers.TG/size.TG
  conversion.Rate.CG <- redeemers.CG/size.CG
  relative.CR.Uplift.vs.CG <- conversion.Rate.TG/conversion.Rate.CG
  campaign.Efficiency <- conversion.Rate.TG - conversion.Rate.CG
  absolute.CR.Uplift.vs.CG <- campaign.Efficiency/conversion.Rate.CG
  
  row <- list(Month=month,
              Propensity_list_after_filters=propensity.list.after.filters, 
              Size_TG=size.TG, Size_CG=size.CG, 
              Redeemers_TG=redeemers.TG, Redeemers_CG=redeemers.CG, 
              Conversion_Rate_TG=conversion.Rate.TG, Conversion_Rate_CG=conversion.Rate.CG, 
              Relative_CR_Uplift_vs_CG=relative.CR.Uplift.vs.CG, Absolute_CR_Uplift_vs_CG=absolute.CR.Uplift.vs.CG, 
              Campaign_Efficiency=campaign.Efficiency)
  
  df.results <- rbind(df.results, row)
  rownames(df.results)[nrow(df.results)] <- label
  
  cat("\n")
  cat("Total Redeemers (", label, "TG ): ", redeemers.TG, " out of ", size.TG, "\n")
  cat("Conversion Rate (", label, "TG ): ", conversion.Rate.TG, "\n")
  cat("Total Redeemers (", label, "CG ): ", redeemers.CG, " out of ", size.CG, "\n")
  cat("Conversion Rate (", label, "CG ): ", conversion.Rate.CG, "\n")
  cat("Uplift (", label, "):             ", relative.CR.Uplift.vs.CG, "\n")
  
  df.results
}

main <- function(month, provision.date = NULL, only.completed = TRUE, teradata.password) {
  #Sys.setlocale("LC_TIME", "en_UK.utf8")
  Sys.setlocale("LC_TIME", "English_United Kingdom")
  #Sys.setlocale("LC_TIME", "C")
  
  today <- format(Sys.Date(), format = "%Y%m%d")
  today.posix <- as.POSIXct(strptime(today, "%Y%m%d"))
  today.minus.lag.posix <- seq.POSIXt(today.posix, length=2, by="-2 days")[2]
  today.minus.lag.label <- format(today.minus.lag.posix, format = "%Y%m%d")
  today.minus.lag.next.day.posix <- seq.POSIXt(today.minus.lag.posix, length=2, by="+1 days")[2]
  today.minus.lag.next.day.label <- format(today.minus.lag.next.day.posix, format = "%Y%m%d")
  
  # Get the first day of the month
  first.day.month <- paste0(month, "01")
  first.day.month.posix <- as.POSIXct(strptime(first.day.month, "%Y%m%d"))
  month.label <- format(first.day.month.posix, "%b-%Y")
  
  # Get previous month
  first.day.prev.month.posix <- seq.POSIXt(first.day.month.posix, length=2, by="-1 months")[2]
  prev.month <- format(first.day.prev.month.posix, format = "%Y%m")
  
  # Get next month
  first.day.next.month.posix <- seq.POSIXt(first.day.month.posix, length=2, by="+1 months")[2]
  first.day.next.month.label <- format(first.day.next.month.posix, format = "%Y%m%d")
  next.month <- format(first.day.next.month.posix, format = "%Y%m")

  # Get -1 day of (month + 1) = last day of month
  last.day.month.posix <- seq.POSIXt(first.day.next.month.posix, length=2, by="-1 days")[2]
  last.day.month.label <- format(last.day.month.posix, format = "%Y%m%d")
  
  # Get the first day of (month + 2)
  first.day.month.two.posix <- seq.POSIXt(first.day.month.posix, length=2, by="+2 months")[2]
  first.day.month.two.label <- format(first.day.month.two.posix, format = "%Y%m%d")
  # Get -1 day of (month + 2) = last day of next month
  last.day.next.month.posix <- seq.POSIXt(first.day.month.two.posix, length=2, by="-1 days")[2]
  last.day.next.month.label <- format(last.day.next.month.posix, format = "%Y%m%d")
  # Get +2 days of (month + 2)
  first.day.month.two.lag.posix <- seq.POSIXt(first.day.month.two.posix, length=2, by="+2 days")[2] # Time lag in provisioned data is 2 days
  
  # TODO: test it
  if (!is.null(provision.date)) {
    provision.date.posix <- as.POSIXct(strptime(provision.date, "%Y%m%d"))
    provision.date.next.day.posix <- seq.POSIXt(provision.date.posix, length=2, by="+1 days")[2]
    provision.date.next.day.label <- format(provision.date.next.day.posix, format = "%Y%m%d")
    if (provision.date.posix <= today.minus.lag.posix) {
      provision.date.query <- provision.date.next.day.label
      provision.date.label <- provision.date
    } else {
      provision.date.query <- today.minus.lag.next.day.label
      provision.date.label <- today.minus.lag.label
    }
  } else {
    if (first.day.month.two.posix <= today.minus.lag.posix) {
      provision.date.query <- first.day.month.two.label
      provision.date.label <- last.day.next.month.label
    } else {
      provision.date.query <- today.minus.lag.next.day.label
      provision.date.label <- today.minus.lag.label
    }
  }
  provision.date.query.posix <- as.POSIXct(strptime(provision.date.query, "%Y%m%d"))
  
  
  # Write results to file
  ofile <- file.path(base.dir, paste0("Convergence_", month, "-", provision.date.label, ".tsv") )
  
  
  cat("\nEvaluating Convergence campaign for month", month, "using predictions made in", prev.month, "\n")
  
  cat("\nProvision_date: ", provision.date.label, "\n")
  cat(paste0("Provision_date\t", provision.date.label, "\n"), file = ofile, append = FALSE)
  
  ifile <- file.path(base.dir, paste0("convergence.predictions.", prev.month, ".csv"))
  if (file.exists(ifile)) {
    cat("\n[LOAD]", ifile, "\n")
    dt.preds <- fread(ifile)
    
    cat("Predictions list: ", nrow(dt.preds), "\n")
    cat(paste0("Predictions_list\t", nrow(dt.preds), "\n"), file = ofile, append = TRUE)
  } else {
    cat("\n[WARN]", ifile, "does not exist\n")
  }
  
  # If campaign contacts file exists, then read it; otherwise, run query against Teradata
  ifile <- file.path(base.dir, "FIBRA_GRATIS_CONTACTADOS_BIG_DATA", paste0("Campaigns-Contactos-", month, ".txt"))
  if (file.exists(ifile)) {
    cat("\n[LOAD]", ifile, "\n")
    dt.campaigns <- fread(ifile)
  } else {
    dt.campaigns <- data.table()
  }
  
  if (nrow(dt.campaigns) <= 0) {
    # Calculate init and end days for queries
    ini.day <- format(first.day.month.posix, format = "%Y-%m-%d %H:%M:%S")
    end.day <- format(first.day.next.month.posix, format = "%Y-%m-%d %H:%M:%S")
    
    query.planificados.campañas <- sub("@INI_DAY@", ini.day, query.planificados.campañas.template)
    query.planificados.campañas <- sub("@END_DAY@", end.day, query.planificados.campañas)
    
    ini <- Sys.time()
    cat(paste0("[", ini,"] ", "Campaign contacts file does not exist, so querying Teradata...\n"))
    
    dt.campaigns <- teradata(query.planificados.campañas, teradata.password)
    
    end <- Sys.time()
    time <- end-ini
    cat(paste0("[", end,"] ", "Query took ", time, " ", units(time), "\n"))
    
    # Save results for future use
    cat("\n[SAVE]", ifile, "\n")
    write.table(dt.campaigns, file = ifile, sep = "|", quote = F, row.names = F)
  }
  
  dt.campaigns[, DATEID := NULL]
  
  cat("Contacts list: ", nrow(dt.campaigns), "\n")
  cat(paste0("Contacts_list\t", nrow(dt.campaigns), "\n"), file = ofile, append = TRUE)
  
  cat('\nCampaignCode:')
  print(table(dt.campaigns$CampaignCode))
  cat('Creatividad:')
  print(table(dt.campaigns$Creatividad))
  cat('CellCode:')
  print(table(dt.campaigns$CellCode))
  cat('CampaignCode & Creatividad:\n')
  print(table(dt.campaigns[, .(CampaignCode, Creatividad)]))
  cat('CampaignCode & CellCode:\n')
  print(table(dt.campaigns[, .(CampaignCode, CellCode)]))
  
  cat("\n***** PROVISIONED *****\n")
  
  # FIXME
  # if (!is.null(provision.date)) {
  #   ifile <- file.path(base.dir, "FIBRA_GRATIS_CONTACTADOS_BIG_DATA", paste0("Provisionadas_", month, "-", provision.date.label, ".txt") )
  # } else {
  #   if (last.day.month.posix <= today.minus.lag.posix) {
  #     ifile <- file.path(base.dir, "FIBRA_GRATIS_CONTACTADOS_BIG_DATA", paste0("Provisionadas_", month, "-", last.day.month.label, ".txt") )
  #   } else {
  #     ifile <- file.path(base.dir, "FIBRA_GRATIS_CONTACTADOS_BIG_DATA", paste0("Provisionadas_", month, "-", today.minus.lag.label, ".txt") )
  #   }
  # }
  ifile <- file.path(base.dir, "FIBRA_GRATIS_CONTACTADOS_BIG_DATA", paste0("Provisionadas_", month, "-", provision.date.label, ".txt") )
  
  # If provisioned file exists, then read it; otherwise, run query against Teradata
  if (file.exists(ifile)) {
    cat("\n[LOAD]", ifile, "\n")
    dt.provisionados <- fread(ifile)
  } else {
    cat("\n[WARN]", ifile, "does not exist\n")
    query.provisionados <- sub("@INI_DAY@", first.day.month, query.provisionados.template)
    # FIXME: This does not work when a provision date is provided
    query.provisionados <- sub("@END_DAY@", provision.date.query, query.provisionados)
    
    ini <- Sys.time()
    cat(paste0("[", ini,"] ", "Provision file does not exist, so querying Teradata from ",
                              first.day.month, " to ", provision.date.query, " ...\n"))
    
    dt.provisionados <- teradata(query.provisionados, teradata.password)

    end <- Sys.time()
    time <- end-ini
    cat(paste0("[", end,"] ", "Query took ", time, " ", units(time), "\n"))
    
    # Save results for future use
    cat("\n[SAVE]", ifile, "\n")
    write.table(dt.provisionados, file = ifile, sep = "|", quote = F, row.names = F)
  }
  
  # Since there are duplicated CIF_NIF with different SUBMIT_DT/STATUS, we need to eliminate those duplicated
  # First, assign order to STATUS
  dt.provisionados[, STATUS := factor(STATUS, levels=c("Completado", "Parcialmente Completado", "Pendiente de Completar", "En Curso"))]
  # Second, sort by CIF_NIF, -SUBMIT_DT, STATUS
  dt.provisionados <- dt.provisionados[order(dt.provisionados[,CIF_NIF],-dt.provisionados[,SUBMIT_DT],dt.provisionados[,STATUS])]
  # Last, take the first CIF_NIF: take the one with more recent SUBMIT_DT, and in case of ties, take the one with most completed STATUS
  dt.provisionados <- dt.provisionados[!duplicated(dt.provisionados$CIF_NIF),]
  setkey(dt.provisionados, CIF_NIF)
  
  cat("\n")
  
  if (only.completed) {
    cat("Counting provisions with STATUS = Completed\n")
    cat("Status\tCompleted\n", file = ofile, append = TRUE)
    dt.provisionados <- dt.provisionados[dt.provisionados$STATUS == "Completado"]
  } else {
    cat("Counting provisions with STATUS = All provisioned\n")
    cat("Status\tProvisioned\n", file = ofile, append = TRUE)
  }
  
  # If provision date provided, provision data is filtered up to given day
  if (!is.null(provision.date)) {
    cat(paste0("Provision date provided, so calculating up to ", provision.date.label, "\n"))
    dt.provisionados$SUBMIT_DT <- as.POSIXct(dt.provisionados$SUBMIT_DT)
    provision.date.posix <- as.POSIXct(strptime(provision.date, "%Y%m%d"))
    dt.provisionados <- dt.provisionados[dt.provisionados$SUBMIT_DT <= provision.date.posix]
  }
  
  df.results <- NULL
  
  if (exists("dt.preds")) {
    convergidos.all <- dt.preds[x_num_ident %in% dt.provisionados$CIF_NIF]
    cat("\n")
    cat("Total BD Contacts (COMPLETE LIST): ",  nrow(dt.campaigns[CIF_NIF %in% dt.preds$x_num_ident]), " out of ", nrow(dt.campaigns), " contacts\n")
    cat("Total Redeemers   (COMPLETE LIST): ",  nrow(convergidos.all), " out of ", nrow(dt.preds), " predictions\n") # Pero no necesariamente han contactado a todos
    cat("Conversion Rate   (COMPLETE LIST): ", (nrow(convergidos.all)/nrow(dt.preds)), "\n")
  }
  
  #---------------
  # FIBRE (BD and CVM, PREVEN and NOPREVEN)
  #---------------
  
  dt.campaigns.FIBRE <- dt.campaigns[CampaignCode == "20160909_PXXXC_FIBRA_GRATIS"]
  
  # if (first.day.month.posix <= strptime("20170601", "%Y%m%d")) {
  #   # Up to Jun '17
  if (first.day.month.posix <= strptime("20170301", "%Y%m%d")) {
    # Up to Mar '17
    dt.campaigns.FIBRE.BD.PREVEN.TG    <- dt.campaigns.FIBRE[CellCode == "01_PREVEN_BIGDATA"]
    dt.campaigns.FIBRE.BD.PREVEN.CG    <- dt.campaigns.FIBRE[CellCode == "CC01_PREVEN_BIGDATA"]
    dt.campaigns.FIBRE.BD.NOPREVEN.TG  <- dt.campaigns.FIBRE[CellCode == "01_NOPREVEN_BIGDATA"]
    dt.campaigns.FIBRE.BD.NOPREVEN.CG  <- dt.campaigns.FIBRE[CellCode == "CC01_NOPREVEN_BIGDATA"]
    dt.campaigns.FIBRE.BD.ALL.TG       <- dt.campaigns.FIBRE[CellCode == "01_PREVEN_BIGDATA"   | CellCode == "01_NOPREVEN_BIGDATA"]
    dt.campaigns.FIBRE.BD.ALL.CG       <- dt.campaigns.FIBRE[CellCode == "CC01_PREVEN_BIGDATA" | CellCode == "CC01_NOPREVEN_BIGDATA"]
    
    dt.campaigns.FIBRE.CVM.PREVEN.TG   <- dt.campaigns.FIBRE[CellCode == "01_PREVEN_MODCVM"]
    dt.campaigns.FIBRE.CVM.PREVEN.CG   <- dt.campaigns.FIBRE[CellCode == "CC01_PREVEN_MODCVM"]
    dt.campaigns.FIBRE.CVM.NOPREVEN.TG <- dt.campaigns.FIBRE[CellCode == "01_NOPREVEN_MODCVM"]
    dt.campaigns.FIBRE.CVM.NOPREVEN.CG <- dt.campaigns.FIBRE[CellCode == "CC01_NOPREVEN_MODCVM"]
    dt.campaigns.FIBRE.CVM.ALL.TG      <- dt.campaigns.FIBRE[CellCode == "01_PREVEN_MODCVM"    | CellCode == "01_NOPREVEN_MODCVM"]
    dt.campaigns.FIBRE.CVM.ALL.CG      <- dt.campaigns.FIBRE[CellCode == "CC01_PREVEN_MODCVM"  | CellCode == "CC01_NOPREVEN_MODCVM"]
    
    dt.campaigns.FIBRE.CVMBD.ALL.TG    <- dt.campaigns.FIBRE[CellCode == "01_PREVEN_MODCVM"    | CellCode == "01_NOPREVEN_MODCVM" | 
                                                             CellCode == "01_PREVEN_BIGDATA"   | CellCode == "01_NOPREVEN_BIGDATA"]
    dt.campaigns.FIBRE.CVMBD.ALL.CG    <- dt.campaigns.FIBRE[CellCode == "CC01_PREVEN_MODCVM"  | CellCode == "CC01_NOPREVEN_MODCVM" | 
                                                             CellCode == "CC01_PREVEN_BIGDATA" | CellCode == "CC01_NOPREVEN_BIGDATA"]
  # } else if (first.day.month.posix == strptime("20170701", "%Y%m%d")) {
  #   # In July '17
  #   dt.campaigns.FIBRE.BD.ALL.TG       <- dt.campaigns.FIBRE[CellCode == "01_PROPENSO"]#   | CellCode == "01_NOPROPENSO"]
  #   dt.campaigns.FIBRE.BD.ALL.CG       <- dt.campaigns.FIBRE[CellCode == "CC01_PROPENSO"]# | CellCode == "CC01_NOPROPENSO"]
  } else { #if (first.day.month.posix >= strptime("20170801", "%Y%m%d")) {
    # From August '17
    dt.campaigns.FIBRE.BD.ALL.TG       <- dt.campaigns.FIBRE[startsWith(CellCode, "01")]
    dt.campaigns.FIBRE.BD.ALL.CG       <- dt.campaigns.FIBRE[startsWith(CellCode, "CC01")]
  }
  
  # if (first.day.month.posix <= strptime("20170601", "%Y%m%d")) {
  #   # Up to Jun '17
  if (first.day.month.posix <= strptime("20170301", "%Y%m%d")) {
    # Up to Mar '17
    convergidos.FIBRE.BD.PREVEN.TG <- dt.campaigns.FIBRE.BD.PREVEN.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    convergidos.FIBRE.BD.PREVEN.CG <- dt.campaigns.FIBRE.BD.PREVEN.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    
    redeemers.TG <- nrow(convergidos.FIBRE.BD.PREVEN.TG)
    redeemers.CG <- nrow(convergidos.FIBRE.BD.PREVEN.CG)
    size.TG      <- nrow(dt.campaigns.FIBRE.BD.PREVEN.TG)
    size.CG      <- nrow(dt.campaigns.FIBRE.BD.PREVEN.CG)
    label        <- "FIBRE_BD_PREVEN"
    
    df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
    #----------------
    convergidos.FIBRE.CVM.PREVEN.TG <- dt.campaigns.FIBRE.CVM.PREVEN.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    convergidos.FIBRE.CVM.PREVEN.CG <- dt.campaigns.FIBRE.CVM.PREVEN.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    
    redeemers.TG <- nrow(convergidos.FIBRE.CVM.PREVEN.TG)
    redeemers.CG <- nrow(convergidos.FIBRE.CVM.PREVEN.CG)
    size.TG      <- nrow(dt.campaigns.FIBRE.CVM.PREVEN.TG)
    size.CG      <- nrow(dt.campaigns.FIBRE.CVM.PREVEN.CG)
    label        <- "FIBRE_CVM_PREVEN"
    
    df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
    #-----------------
    convergidos.FIBRE.BD.NOPREVEN.TG <- dt.campaigns.FIBRE.BD.NOPREVEN.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    convergidos.FIBRE.BD.NOPREVEN.CG <- dt.campaigns.FIBRE.BD.NOPREVEN.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    
    redeemers.TG <- nrow(convergidos.FIBRE.BD.NOPREVEN.TG)
    redeemers.CG <- nrow(convergidos.FIBRE.BD.NOPREVEN.CG)
    size.TG      <- nrow(dt.campaigns.FIBRE.BD.NOPREVEN.TG)
    size.CG      <- nrow(dt.campaigns.FIBRE.BD.NOPREVEN.CG)
    label        <- "FIBRE_BD_NOPREVEN"
    
    df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
    #--------------------
    convergidos.FIBRE.CVM.NOPREVEN.TG <- dt.campaigns.FIBRE.CVM.NOPREVEN.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    convergidos.FIBRE.CVM.NOPREVEN.CG <- dt.campaigns.FIBRE.CVM.NOPREVEN.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    
    redeemers.TG <- nrow(convergidos.FIBRE.CVM.NOPREVEN.TG)
    redeemers.CG <- nrow(convergidos.FIBRE.CVM.NOPREVEN.CG)
    size.TG      <- nrow(dt.campaigns.FIBRE.CVM.NOPREVEN.TG)
    size.CG      <- nrow(dt.campaigns.FIBRE.CVM.NOPREVEN.CG)
    label        <- "FIBRE_CVM_NOPREVEN"
    
    df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
  }

  #---------------
  # FIBRE
  #---------------
  
  convergidos.FIBRE.BD.ALL.TG <- dt.campaigns.FIBRE.BD.ALL.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
  convergidos.FIBRE.BD.ALL.CG <- dt.campaigns.FIBRE.BD.ALL.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
  
  redeemers.TG <- nrow(convergidos.FIBRE.BD.ALL.TG)
  redeemers.CG <- nrow(convergidos.FIBRE.BD.ALL.CG)
  size.TG      <- nrow(dt.campaigns.FIBRE.BD.ALL.TG)
  size.CG      <- nrow(dt.campaigns.FIBRE.BD.ALL.CG)
  #label        <- "FIBRE_BD"
  label        <- "FIBRE"
  
  df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
  
  #--------------------
  # if (first.day.month.posix <= strptime("20170601", "%Y%m%d")) {
  #   # Up to Jun '17
  if (first.day.month.posix <= strptime("20170301", "%Y%m%d")) {
    # Up to Mar '17
    convergidos.FIBRE.CVM.ALL.TG <- dt.campaigns.FIBRE.CVM.ALL.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    convergidos.FIBRE.CVM.ALL.CG <- dt.campaigns.FIBRE.CVM.ALL.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    
    redeemers.TG <- nrow(convergidos.FIBRE.CVM.ALL.TG)
    redeemers.CG <- nrow(convergidos.FIBRE.CVM.ALL.CG)
    size.TG      <- nrow(dt.campaigns.FIBRE.CVM.ALL.TG)
    size.CG      <- nrow(dt.campaigns.FIBRE.CVM.ALL.CG)
    label        <- "FIBRE_CVM"
    
    df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
    #--------------------
    convergidos.FIBRE.CVMBD.ALL.TG <- dt.campaigns.FIBRE.CVMBD.ALL.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    convergidos.FIBRE.CVMBD.ALL.CG <- dt.campaigns.FIBRE.CVMBD.ALL.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    
    redeemers.TG <- nrow(convergidos.FIBRE.CVMBD.ALL.TG)
    redeemers.CG <- nrow(convergidos.FIBRE.CVMBD.ALL.CG)
    size.TG      <- nrow(dt.campaigns.FIBRE.CVMBD.ALL.TG)
    size.CG      <- nrow(dt.campaigns.FIBRE.CVMBD.ALL.CG)
    label        <- "FIBRE_CVM+BD"
    
    df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
  }

  #---------------
  # NEBA
  #---------------
  dt.campaigns.NEBA <- dt.campaigns[CampaignCode == "AUTOMMES_PXXXC_FIBRA_NEBA"]
  
  # if (first.day.month.posix <= strptime("20170601", "%Y%m%d")) {
  #   # Up to Jun '17
  if (first.day.month.posix <= strptime("20170301", "%Y%m%d")) {
    # Up to Mar '17
    dt.campaigns.NEBA.BD  <-  dt.campaigns.NEBA[CIF_NIF %in% dt.preds$x_num_ident]
    dt.campaigns.NEBA.CVM <-  dt.campaigns.NEBA[!CIF_NIF %in% dt.preds$x_num_ident]
    
    if (month == "201612") {
      dt.campaigns.NEBA.ALL.TG     <- dt.campaigns.NEBA[CellCode == "01"]
      dt.campaigns.NEBA.ALL.CG     <- dt.campaigns.NEBA[CellCode == "CC01"]
      dt.campaigns.NEBA.BD.ALL.TG  <- dt.campaigns.NEBA.BD[CellCode == "01"]
      dt.campaigns.NEBA.BD.ALL.CG  <- dt.campaigns.NEBA.BD[CellCode == "CC01"]
      dt.campaigns.NEBA.CVM.ALL.TG <- dt.campaigns.NEBA.CVM[CellCode == "01"]
      dt.campaigns.NEBA.CVM.ALL.CG <- dt.campaigns.NEBA.CVM[CellCode == "CC01"]
    } else {
      dt.campaigns.NEBA.ALL.TG     <- dt.campaigns.NEBA[CellCode == "01_PREVEN" | CellCode == "01_NOPREVEN"]
      dt.campaigns.NEBA.ALL.CG     <- dt.campaigns.NEBA[CellCode == "CC01_PREVEN" | CellCode == "CC01_NOPREVEN"]
      dt.campaigns.NEBA.BD.ALL.TG  <- dt.campaigns.NEBA.BD[CellCode == "01_PREVEN" | CellCode == "01_NOPREVEN"]
      dt.campaigns.NEBA.BD.ALL.CG  <- dt.campaigns.NEBA.BD[CellCode == "CC01_PREVEN" | CellCode == "CC01_NOPREVEN"]
      dt.campaigns.NEBA.CVM.ALL.TG <- dt.campaigns.NEBA.CVM[CellCode == "01_PREVEN" | CellCode == "01_NOPREVEN"]
      dt.campaigns.NEBA.CVM.ALL.CG <- dt.campaigns.NEBA.CVM[CellCode == "CC01_PREVEN" | CellCode == "CC01_NOPREVEN"]
    }
  # } else if (first.day.month.posix == strptime("20170701", "%Y%m%d")) {
  #   # In July '17
  #   dt.campaigns.NEBA.ALL.TG     <- dt.campaigns.NEBA[CellCode == "01_PROPENSO"]
  #   dt.campaigns.NEBA.ALL.CG     <- dt.campaigns.NEBA[CellCode == "CC01_PROPENSO"]
  #   dt.campaigns.NEBA.BD.ALL.TG  <- dt.campaigns.NEBA.BD[CellCode == "01_PROPENSO"]
  #   dt.campaigns.NEBA.BD.ALL.CG  <- dt.campaigns.NEBA.BD[CellCode == "CC01_PROPENSO"]
  #   dt.campaigns.NEBA.CVM.ALL.TG <- dt.campaigns.NEBA.CVM[CellCode == "01_PROPENSO"]
  #   dt.campaigns.NEBA.CVM.ALL.CG <- dt.campaigns.NEBA.CVM[CellCode == "CC01_PROPENSO"]
  } else { #if (first.day.month.posix >= strptime("20170801", "%Y%m%d")) {
    # From August '17
    dt.campaigns.NEBA.ALL.TG     <- dt.campaigns.NEBA[startsWith(CellCode, "01")]
    dt.campaigns.NEBA.ALL.CG     <- dt.campaigns.NEBA[startsWith(CellCode, "CC01")]
    # dt.campaigns.NEBA.BD.ALL.TG  <- dt.campaigns.NEBA.BD[startsWith(CellCode, "01")]
    # dt.campaigns.NEBA.BD.ALL.CG  <- dt.campaigns.NEBA.BD[startsWith(CellCode, "CC01")]
    # dt.campaigns.NEBA.CVM.ALL.TG <- dt.campaigns.NEBA.CVM[startsWith(CellCode, "01")]
    # dt.campaigns.NEBA.CVM.ALL.CG <- dt.campaigns.NEBA.CVM[startsWith(CellCode, "CC01")]
  }
  
  #--------------------
  # convergidos.NEBA.BD.ALL.TG <- dt.campaigns.NEBA.BD.ALL.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
  # convergidos.NEBA.BD.ALL.CG <- dt.campaigns.NEBA.BD.ALL.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
  # 
  # redeemers.TG <- nrow(convergidos.NEBA.BD.ALL.TG)
  # redeemers.CG <- nrow(convergidos.NEBA.BD.ALL.CG)
  # size.TG      <- nrow(dt.campaigns.NEBA.BD.ALL.TG)
  # size.CG      <- nrow(dt.campaigns.NEBA.BD.ALL.CG)
  # label        <- "NEBA_BD"
  # 
  # df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
  #--------------------
  # convergidos.NEBA.CVM.ALL.TG <- dt.campaigns.NEBA.CVM.ALL.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
  # convergidos.NEBA.CVM.ALL.CG <- dt.campaigns.NEBA.CVM.ALL.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
  # 
  # redeemers.TG <- nrow(convergidos.NEBA.CVM.ALL.TG)
  # redeemers.CG <- nrow(convergidos.NEBA.CVM.ALL.CG)
  # size.TG      <- nrow(dt.campaigns.NEBA.CVM.ALL.TG)
  # size.CG      <- nrow(dt.campaigns.NEBA.CVM.ALL.CG)
  # label        <- "NEBA_CVM"
  # 
  # df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
  #--------------------
  convergidos.NEBA.ALL.TG <- dt.campaigns.NEBA.ALL.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
  convergidos.NEBA.ALL.CG <- dt.campaigns.NEBA.ALL.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
  
  redeemers.TG <- nrow(convergidos.NEBA.ALL.TG)
  redeemers.CG <- nrow(convergidos.NEBA.ALL.CG)
  size.TG      <- nrow(dt.campaigns.NEBA.ALL.TG)
  size.CG      <- nrow(dt.campaigns.NEBA.ALL.CG)
  #label        <- "NEBA_CVM+BD"
  label        <- "NEBA"
  
  df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
  #--------------------
  
  #---------------
  # NEBA ZC (ARIEL)
  #---------------
  if ((first.day.month.posix >= as.POSIXct(strptime("20170401", "%Y%m%d"))) &&
      (first.day.month.posix <= as.POSIXct(strptime("20170901", "%Y%m%d")))) {
        
    dt.campaigns.NEBA_ZC <- dt.campaigns[CampaignCode == "AUTOMMES_PXXXC_NEBA_ZC"]
  
    # dt.campaigns.NEBA_ZC.BD  <-  dt.campaigns.NEBA_ZC[CIF_NIF %in% dt.preds$x_num_ident]
    # dt.campaigns.NEBA_ZC.CVM <-  dt.campaigns.NEBA_ZC[!CIF_NIF %in% dt.preds$x_num_ident]
    # 
    # if ((as.POSIXct(strptime("20170401", "%Y%m%d")) <= first.day.month.posix) &
    #     (first.day.month.posix <= strptime("20170701", "%Y%m%d"))) {
    #   # From April '17 to July '17
    #   dt.campaigns.NEBA_ZC.ALL.TG     <- dt.campaigns.NEBA_ZC[CellCode == "01"]
    #   dt.campaigns.NEBA_ZC.ALL.CG     <- dt.campaigns.NEBA_ZC[CellCode == "CC01"]
    #   dt.campaigns.NEBA_ZC.BD.ALL.TG  <- dt.campaigns.NEBA_ZC.BD[CellCode == "01"]
    #   dt.campaigns.NEBA_ZC.BD.ALL.CG  <- dt.campaigns.NEBA_ZC.BD[CellCode == "CC01"]
    #   dt.campaigns.NEBA_ZC.CVM.ALL.TG <- dt.campaigns.NEBA_ZC.CVM[CellCode == "01"]
    #   dt.campaigns.NEBA_ZC.CVM.ALL.CG <- dt.campaigns.NEBA_ZC.CVM[CellCode == "CC01"]
    # } else { #if (first.day.month.posix >= strptime("20170801", "%Y%m%d")) {
    # From August '17
    # if (first.day.month.posix >= as.POSIXct(strptime("20170401", "%Y%m%d"))) {
      dt.campaigns.NEBA_ZC.ALL.TG     <- dt.campaigns.NEBA_ZC[startsWith(CellCode, "01")]
      dt.campaigns.NEBA_ZC.ALL.CG     <- dt.campaigns.NEBA_ZC[startsWith(CellCode, "CC01")]
      # dt.campaigns.NEBA_ZC.BD.ALL.TG  <- dt.campaigns.NEBA_ZC.BD[startsWith(CellCode, "01")]
      # dt.campaigns.NEBA_ZC.BD.ALL.CG  <- dt.campaigns.NEBA_ZC.BD[startsWith(CellCode, "CC01")]
      # dt.campaigns.NEBA_ZC.CVM.ALL.TG <- dt.campaigns.NEBA_ZC.CVM[startsWith(CellCode, "01")]
      # dt.campaigns.NEBA_ZC.CVM.ALL.CG <- dt.campaigns.NEBA_ZC.CVM[startsWith(CellCode, "CC01")]
    # }
  
    #--------------------
    # convergidos.NEBA_ZC.BD.ALL.TG <- dt.campaigns.NEBA_ZC.BD.ALL.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    # convergidos.NEBA_ZC.BD.ALL.CG <- dt.campaigns.NEBA_ZC.BD.ALL.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    # 
    # redeemers.TG <- nrow(convergidos.NEBA_ZC.BD.ALL.TG)
    # redeemers.CG <- nrow(convergidos.NEBA_ZC.BD.ALL.CG)
    # size.TG      <- nrow(dt.campaigns.NEBA_ZC.BD.ALL.TG)
    # size.CG      <- nrow(dt.campaigns.NEBA_ZC.BD.ALL.CG)
    # label        <- "NEBA_ZC_BD"
    # 
    # df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
    #--------------------
    # convergidos.NEBA_ZC.CVM.ALL.TG <- dt.campaigns.NEBA_ZC.CVM.ALL.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    # convergidos.NEBA_ZC.CVM.ALL.CG <- dt.campaigns.NEBA_ZC.CVM.ALL.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    # 
    # redeemers.TG <- nrow(convergidos.NEBA_ZC.CVM.ALL.TG)
    # redeemers.CG <- nrow(convergidos.NEBA_ZC.CVM.ALL.CG)
    # size.TG      <- nrow(dt.campaigns.NEBA_ZC.CVM.ALL.TG)
    # size.CG      <- nrow(dt.campaigns.NEBA_ZC.CVM.ALL.CG)
    # label        <- "NEBA_ZC_CVM"
    # 
    # df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
    #--------------------
    convergidos.NEBA_ZC.ALL.TG <- dt.campaigns.NEBA_ZC.ALL.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    convergidos.NEBA_ZC.ALL.CG <- dt.campaigns.NEBA_ZC.ALL.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
    
    redeemers.TG <- nrow(convergidos.NEBA_ZC.ALL.TG)
    redeemers.CG <- nrow(convergidos.NEBA_ZC.ALL.CG)
    size.TG      <- nrow(dt.campaigns.NEBA_ZC.ALL.TG)
    size.CG      <- nrow(dt.campaigns.NEBA_ZC.ALL.CG)
    #label        <- "NEBA_ZC_CVM+BD"
    label        <- "NEBA_ZC"
    
    df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
  }
  #--------------------
  
  #----------
  # DSL
  #-----------
  
  dt.campaigns.DSL <- dt.campaigns[CampaignCode == "AUTOMMES_PXXXC_XSELL_ADSL"] # & Creatividad == "DSL_PROPENSOS_TEL"]
  
  # if (first.day.month.posix <= strptime("20170601", "%Y%m%d")) {
  #   # Up to Jun '17
  if (first.day.month.posix <= strptime("20170301", "%Y%m%d")) {
    # Up to Mar '17
    dt.campaigns.DSL.BD  <-  dt.campaigns.DSL[CIF_NIF %in% dt.preds$x_num_ident]
    dt.campaigns.DSL.CVM <-  dt.campaigns.DSL[!CIF_NIF %in% dt.preds$x_num_ident]
    
    dt.campaigns.DSL.ALL.TG     <- dt.campaigns.DSL[Creatividad == "DSL_PROPENSOS_TEL" & (CellCode == "01_NOPREVEN" | CellCode == "01_PREVEN")]
    dt.campaigns.DSL.ALL.CG     <- dt.campaigns.DSL[Creatividad == "DSL_PROPENSOS_TEL" & (CellCode == "CC01_NOPREVEN" | CellCode == "CC01_PREVEN")]
    dt.campaigns.DSL.BD.ALL.TG  <- dt.campaigns.DSL.BD[Creatividad == "DSL_PROPENSOS_TEL" & (CellCode == "01_NOPREVEN" | CellCode == "01_PREVEN")]
    dt.campaigns.DSL.BD.ALL.CG  <- dt.campaigns.DSL.BD[Creatividad == "DSL_PROPENSOS_TEL" & (CellCode == "CC01_NOPREVEN" | CellCode == "CC01_PREVEN")]
    dt.campaigns.DSL.CVM.ALL.TG <- dt.campaigns.DSL.CVM[Creatividad == "DSL_PROPENSOS_TEL" & (CellCode == "01_NOPREVEN" | CellCode == "01_PREVEN")]
    dt.campaigns.DSL.CVM.ALL.CG <- dt.campaigns.DSL.CVM[Creatividad == "DSL_PROPENSOS_TEL" & (CellCode == "CC01_NOPREVEN" | CellCode == "CC01_PREVEN")]
  # } else if (first.day.month.posix == strptime("20170701", "%Y%m%d")) {
  #   # In July '17
  #   dt.campaigns.DSL.ALL.TG     <- dt.campaigns.DSL[CellCode == "01_PROPENSO"]
  #   dt.campaigns.DSL.ALL.CG     <- dt.campaigns.DSL[CellCode == "CC01_PROPENSO"]
  #   dt.campaigns.DSL.BD.ALL.TG  <- dt.campaigns.DSL.BD[CellCode == "01_PROPENSO"]
  #   dt.campaigns.DSL.BD.ALL.CG  <- dt.campaigns.DSL.BD[CellCode == "CC01_PROPENSO"]
  #   dt.campaigns.DSL.CVM.ALL.TG <- dt.campaigns.DSL.CVM[CellCode == "01_PROPENSO"]
  #   dt.campaigns.DSL.CVM.ALL.CG <- dt.campaigns.DSL.CVM[CellCode == "CC01_PROPENSO"]
  } else { #if (first.day.month.posix >= strptime("20170801", "%Y%m%d")) {
    # From August '17
    dt.campaigns.DSL.ALL.TG     <- dt.campaigns.DSL[startsWith(CellCode, "01")]
    dt.campaigns.DSL.ALL.CG     <- dt.campaigns.DSL[startsWith(CellCode, "CC01")]
    # dt.campaigns.DSL.BD.ALL.TG  <- dt.campaigns.DSL.BD[startsWith(CellCode, "01")]
    # dt.campaigns.DSL.BD.ALL.CG  <- dt.campaigns.DSL.BD[startsWith(CellCode, "CC01")]
    # dt.campaigns.DSL.CVM.ALL.TG <- dt.campaigns.DSL.CVM[startsWith(CellCode, "01")]
    # dt.campaigns.DSL.CVM.ALL.CG <- dt.campaigns.DSL.CVM[startsWith(CellCode, "CC01")]
  }
  
  #--------------------
  # convergidos.DSL.BD.ALL.TG <- dt.campaigns.DSL.BD.ALL.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
  # convergidos.DSL.BD.ALL.CG <- dt.campaigns.DSL.BD.ALL.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
  # 
  # redeemers.TG <- nrow(convergidos.DSL.BD.ALL.TG)
  # redeemers.CG <- nrow(convergidos.DSL.BD.ALL.CG)
  # size.TG      <- nrow(dt.campaigns.DSL.BD.ALL.TG)
  # size.CG      <- nrow(dt.campaigns.DSL.BD.ALL.CG)
  # label        <- "DSL_BD"
  # 
  # df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
  #--------------------
  # convergidos.DSL.CVM.ALL.TG <- dt.campaigns.DSL.CVM.ALL.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
  # convergidos.DSL.CVM.ALL.CG <- dt.campaigns.DSL.CVM.ALL.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
  # 
  # redeemers.TG <- nrow(convergidos.DSL.CVM.ALL.TG)
  # redeemers.CG <- nrow(convergidos.DSL.CVM.ALL.CG)
  # size.TG      <- nrow(dt.campaigns.DSL.CVM.ALL.TG)
  # size.CG      <- nrow(dt.campaigns.DSL.CVM.ALL.CG)
  # label        <- "DSL_CVM"
  # 
  # df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
  #--------------------
  convergidos.DSL.ALL.TG <- dt.campaigns.DSL.ALL.TG[CIF_NIF %in% dt.provisionados$CIF_NIF]
  convergidos.DSL.ALL.CG <- dt.campaigns.DSL.ALL.CG[CIF_NIF %in% dt.provisionados$CIF_NIF]
  
  redeemers.TG <- nrow(convergidos.DSL.ALL.TG)
  redeemers.CG <- nrow(convergidos.DSL.ALL.CG)
  size.TG      <- nrow(dt.campaigns.DSL.ALL.TG)
  size.CG      <- nrow(dt.campaigns.DSL.ALL.CG)
  #label        <- "DSL_CVM+BD"
  label        <- "DSL"
  
  df.results <- measure_sub_campaign(df.results, month.label, redeemers.TG, redeemers.CG, size.TG, size.CG, label)
  #--------------------
  
  cat("Convergence_campaign\t", file = ofile, append = TRUE)
  suppressWarnings( write.table(df.results, file = ofile, sep = "\t", dec = ".", quote = FALSE, append = TRUE) )
  
  #warnings()
}


#--------------------------------------------------------------------------------------------------------
option_list <- list(
  make_option(c("-m", "--month"), type = "character", default = NULL, help = "Month to evaluate the campaign (YYYYMM)", 
              metavar = "character"),
  make_option(c("-p", "--prov"), type = "character", default = NULL, help = "(Optional) Provision Date (YYYYMMDD). Calculations are done up to this date. Default: month + 2", 
              metavar = "character"),
  make_option(c("-c", "--completed"), type = "logical", action="store_true", default = TRUE, help = "(Optional) Count provisions only with STATUS = Completed", 
              metavar = "character"),
  make_option(c("-a", "--pass"), type = "character", default = NULL, help = "(Optional) Password for Teradata", 
              metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

if (is.null(opt$month)) {
  print_help(opt_parser)
  stop("At least the month must be supplied (--month YYYYMM)", call.=FALSE)
} else {
  main(opt$month, opt$prov, opt$completed, opt$pass)
}
