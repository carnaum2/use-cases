source("configuration.R")

init.month <- "201605" # FIXME
# FIXME: No sobreescribir el último mes, usar un nombre diferente

main_02_compute_historical_campaign_variables <- function(curr.month) {
  cat ("\n[MAIN] 02_compute_historical_campaign_variables", curr.month, "\n")
  
  if (curr.month == init.month) {
    ofolder <- paste0(data.folder,"/Campaign/", curr.month)
    
    if (! "dt.Campaigns" %in% ls()) {
      ifile <- paste0(ofolder, "/dt.Campaigns.", curr.month, ".RData")
      cat("[LOAD] ", ifile, "\n")
      load(ifile)
    }
    
    if ("HIST_number_of_contacts" %in% colnames(dt.Campaigns))
      dt.Campaigns[, HIST_number_of_contacts := NULL]
    
    if ("i.HIST_number_of_contacts" %in% colnames(dt.Campaigns))
      dt.Campaigns[, i.HIST_number_of_contacts := NULL]
    
    if ("HIST_number_of_responses" %in% colnames(dt.Campaigns))
      dt.Campaigns[, HIST_number_of_responses := NULL]
    
    if ("i.HIST_number_of_responses" %in% colnames(dt.Campaigns))
      dt.Campaigns[, i.HIST_number_of_responses := NULL]
    
    dt.Campaigns[, HIST_number_of_contacts := number_of_contacts]
    dt.Campaigns[, HIST_number_of_responses := number_of_responses]
   
    ofile <- ifile
    cat("[SAVE] ", ofile, "-", nrow(dt.Campaigns), "\n")
    save(dt.Campaigns, file = ofile) # FIXME: Overwrites dt.Campaigns.<month>.RData
  }
  else {
    # Load Previous month
    # FIXME: Contemplar el caso de que te bajes una extracción de un mes en medio del mes con formato YYYYMMDD, en lugar de YYYYMM
    prev.month <- paste0(curr.month, "01")
    prev.month <- as.POSIXct(strptime(prev.month, "%Y%m%d"))
    prev.month <- prev.month - 2
    prev.month <- as.character(prev.month)
    prev.month <- substr(prev.month, 1, 7)
    prev.month <- gsub("-", "", prev.month)
    
    ifolder <- paste0(data.folder,"/Campaign/", prev.month)
    ifile <- paste0(ifolder, "/dt.Campaigns.", prev.month, ".RData")
    cat("[LOAD] ", ifile, "\n")
    load(ifile)

    if (prev.month == init.month) {
      dt.Campaigns.prev <- dt.Campaigns[, .(CIF_NIF, HIST_number_of_contacts, HIST_number_of_responses)]

    } else {
      dt.Campaigns.prev <- dt.Campaigns[, .(CIF_NIF, HIST_number_of_contacts, HIST_number_of_responses, number_of_contacts, 
                                            number_of_responses)]
      dt.Campaigns.prev[, HIST_number_of_contacts := HIST_number_of_contacts + number_of_contacts]
      dt.Campaigns.prev[, HIST_number_of_responses := HIST_number_of_responses + number_of_responses]
      dt.Campaigns.prev[, number_of_contacts := NULL]
      dt.Campaigns.prev[, number_of_responses := NULL]
    }
    
    setkey(dt.Campaigns.prev, CIF_NIF)
    
    # Load Current Month
    ifolder <- paste0(data.folder,"/Campaign/", curr.month)
    ifile <- paste0(ifolder, "/dt.Campaigns.", curr.month, ".RData")
    cat("[LOAD] ", ifile, "\n")
    load(ifile)
    setkey(dt.Campaigns, CIF_NIF)
    
    if ("HIST_number_of_contacts" %in% colnames(dt.Campaigns))
      dt.Campaigns[, HIST_number_of_contacts := NULL]
    
    if ("HIST_number_of_responses" %in% colnames(dt.Campaigns))
      dt.Campaigns[, HIST_number_of_responses := NULL]
    
    if ("i.HIST_number_of_contacts" %in% colnames(dt.Campaigns))
      dt.Campaigns[, i.HIST_number_of_contacts := NULL]
    
    if ("i.HIST_number_of_responses" %in% colnames(dt.Campaigns))
      dt.Campaigns[, i.HIST_number_of_responses := NULL]
    
    cat("[INFO] Joining....\n")
    dt.Campaigns <- dt.Campaigns.prev[dt.Campaigns]
    dt.Campaigns[is.na(HIST_number_of_contacts), HIST_number_of_contacts := 0]
    dt.Campaigns[is.na(HIST_number_of_responses), HIST_number_of_responses := 0]
    
    ofile <- ifile
    cat("[SAVE] ", ofile, "-", nrow(dt.Campaigns), "\n")
    save(dt.Campaigns, file = ofile) # FIXME: Overwrites dt.Campaigns.<month>.RData
  }
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
    main_02_compute_historical_campaign_variables(opt$month)
  }
}
