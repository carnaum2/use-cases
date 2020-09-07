#!/usr/bin/env Rscript

suppressWarnings(suppressMessages(library(optparse)))
suppressWarnings(suppressMessages(library(data.table)))
suppressWarnings(suppressMessages(library(bit64)))
suppressWarnings(suppressMessages(library(getPass)))
suppressWarnings(suppressMessages(library(RODBC)))


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
    AND (PCS_QUOTE.Tipo='Modificar' OR PCS_QUOTE.Tipo LIKE 'Activaci%n')  --Tipo de solicitud de Fibra: MIGRA O ALTA
    AND cast(PCS_QUOTE.SUBMIT_DT AS DATE FORMAT 'YYYYMMDD')>= '@INI_DAY@'
    AND cast(PCS_QUOTE.SUBMIT_DT AS DATE FORMAT 'YYYYMMDD')< '@END_DAY@'
    AND (PCS_QUOTE.STATUS IN ('Completado', 'En Curso', 'Parcialmente Completado', 'Pendiente de Completar') -- 'Cancelado', 'Rechazado'
        OR  PCS_QUOTE.STATUS LIKE 'Pendiente Acci%n Futura'
        AND PCS_QUOTE_ITEM.STATUS='Completado')
    AND PCS_QUOTE_ITEM.ACTION_CODE='Nuevo')"

main <- function(pass) {
  #username <- Sys.getenv("USERNAME") # Windows
  username <- Sys.getenv("USER") # Mac OS X
  
  # driver <- 'Teradata' # Windows
  driver <- '{Teradata Database ODBC Driver 16.20}' # Mac OS X
  
  ch <- odbcDriverConnect(paste0("Driver=", driver, ";DBCName=vodaes1-u;UID=", username, ";PWD=", pass)) # FIXME: , DBMSencoding = ""

	# Get dates

	today <- format(Sys.Date(), format = "%Y%m%d")
	today.posix <- as.POSIXct(strptime(today, "%Y%m%d"))
	month <- format(Sys.Date(), format = "%Y%m")
	  
	# Get the first day of the month
	first.day.month <- paste0(month, "01")
	first.day.month.posix <- as.POSIXct(strptime(first.day.month, "%Y%m%d"))

	# Get previous month
	first.day.prev.month.posix <- seq.POSIXt(first.day.month.posix, length=2, by="-1 months")[2]
	first.day.prev.month <- format(first.day.prev.month.posix, format = "%Y%m%d")


	# Create output folder
	ofolder <- file.path(today, "INPUT")
	if (!file.exists(ofolder)) {
	  cat("[INFO] Creating folder", ofolder, "\n\n")
	  dir.create(ofolder, recursive = T)
	}


	# Provisioned

	query.provisionados <- sub("@INI_DAY@", first.day.prev.month, query.provisionados.template)
	query.provisionados <- sub("@END_DAY@", today, query.provisionados)

	cat("[INFO] Querying Provisioned ...\n")
	dt.provisioned <- data.table(sqlQuery(ch, query.provisionados))
	ofile <- file.path(ofolder, paste0("Provisionadas_", today, ".txt") )
	cat("[SAVE] Saving Provisioned ...\n")
	write.table(dt.provisioned, file = ofile, sep = "|", quote = F, row.names = F)
	#rm(list = c("query.provisionados.template", "query.provisionados"))
	rm(list = c("query.provisionados"))


	# NIFS_COMPARTIDOS

	#a <- sqlQuery(ch, 'select * from dbc.tables  where upper(tablename) like \'%NIFS_COMPARTIDOS%\'')
	cat("\n[INFO] Querying NIFS_COMPARTIDOS ...\n")
	dt.extr_nifs_compartidos <- data.table(sqlQuery(ch, 'select * from ADS_TRACK_V.EXTR_NIFS_COMPARTIDOS_YYYYMMDD'))
	ofile <- file.path(ofolder, paste0("EXTR_NIFS_COMPARTIDOS_", today, ".txt") )
	cat("[SAVE] Saving NIFS_COMPARTIDOS ...\n")
	write.table(dt.extr_nifs_compartidos, file = ofile, sep = "|", quote = F, row.names = F)
	#odbcClose(ch); quit()


	# SUN_INFO_CRUCE
	
	#a <- sqlQuery(ch, 'select * from dbc.tables  where upper(tablename) like \'%SUN_INFO_CRUCE%\'')
	cat("\n[INFO] Querying SUN_INFO_CRUCE ...\n")
	dt.sun_info_cruce <- data.table(sqlQuery(ch, 'sel distinct NIF from ADS_TRACK_V.SUN_INFO_CRUCE where ID_CLIENTE is NOT NULL AND NIF is NOT NULL'))
	ofile <- file.path(ofolder, paste0("SUN_INFO_CRUCE_", today, ".txt") )
	cat("[SAVE] Saving SUN_INFO_CRUCE ...\n")
	write.table(dt.sun_info_cruce, file = ofile, sep = "|", quote = F, row.names = F)
	
	odbcClose(ch); quit()
	
	
	# AC_FINAL_POSPAGO

	#a <- sqlQuery(ch, 'select * from dbc.tables  where upper(tablename) like \'%AC_FINAL_POSPAGO%\'')
	#a <- sqlQuery(ch, 'select count(*) from ADS_TRACK_V.AC_FINAL_POSPAGO')
	cat("\n[INFO] Querying and Saving AC_FINAL_POSPAGO ...\n")
	#dt.ac_final_pospago <- data.table(sqlQuery(ch, 'select * from ADS_TRACK_V.AC_FINAL_POSPAGO', believeNRows = FALSE))

	ini <- Sys.time()
	ofile <- file.path(ofolder, paste0("EXTR_AC_FINAL_POSPAGO_", today, ".txt") )
	m <- 250000
	res <- sqlFetch(ch, "ADS_TRACK_V.AC_FINAL_POSPAGO", max = m, as.is = T)
	num.rows <- nrow(res)
	cat("[INFO] Fetched", num.rows, "rows\n")
	write.table(res, file = ofile, sep = "|", quote = F, row.names = F)
	while (nrow(res) == m) {
	  rm(res); dev.null <- gc()
	  res <- sqlFetchMore(ch, max = m, as.is = T)
	  cat("[INFO] Fetched", nrow(res), "rows. ")
	  if ((res == -1) || (res == "-1") || (nrow(res) <= 0)) {
	    cat("No more rows\n")
		  break
	  } else {
  		num.rows <- num.rows + nrow(res)
  		cat ("Total rows:", num.rows, "\n")
  		write.table(res, file = ofile, sep = "|", quote = F, row.names = F, col.names = FALSE, append = TRUE)
	  }
	}
	end <- Sys.time()
	time <- end-ini
	cat(paste0("[", end,"] ", "Query & Saving took ", time, " ", units(time), "\n"))

	#ofile <- file.path(ofolder, paste0("EXTR_AC_FINAL_POSPAGO_", today, ".txt") )
	#cat("[SAVE] Saving AC_FINAL_POSPAGO ...\n")
	#write.table(dt.ac_final_pospago, file = ofile, sep = "|", quote = F, row.names = F)


	odbcClose(ch)
}

#--------------------------------------------------------------------------------------------------------
option_list <- list(
  make_option(c("-a", "--pass"), type = "character", default = NULL, help = "Password for Teradata", 
              metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

if (is.null(opt$pass)) {
  print_help(opt_parser)
  stop("At least the pass must be supplied (--pass <password>)", call.=FALSE)
} else {
  main(opt$pass)
}
