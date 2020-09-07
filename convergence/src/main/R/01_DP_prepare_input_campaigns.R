source("configuration.R")

main_01_DP_prepare_input_campaigns <- function(month) {
  ifile <- file.path(input.folder.campaigns, month, 
                     paste0(month, "__campaign_data_by_customer__number_of_responses_by_channel_and_campaign_type" ,".csv"))
  if (!file.exists(ifile))
      cat("[WARN] File", ifile, "does not exist\n")
      
  ofolder <- file.path(data.folder, "Campaigns", month)
  ofile <- file.path(ofolder, paste0("dt.Campaigns.", month, ".RData"))
  
  ifile.ctime <- file.info(ifile)$ctime
  ofile.ctime <- file.info(ofile)$ctime
  
  if ( (!file.exists(ofile)) || (file.exists(ifile) && (ofile.ctime < ifile.ctime)) ) {
    cat("[INFO] Preparing Input Data for Month: ", month, "\n")
    # FIXME: Copiado manualmente 20161222__campaign_data_by_customer__number_of_responses_by_channel_and_campaign_type.csv a 201612
    cat("[LOAD] ", ifile, "\n")
    dt.Campaigns <- fread(ifile)
    
    if (!file.exists(ofolder)) {
      cat("[INFO] Creating Folder\n")
      dir.create(ofolder, recursive = T)
    }
    
    cat("[SAVE] ", ofile)
    save(dt.Campaigns, file = ofile) 
  } else {
    cat("[INFO] Skipping", ifile, "\n")
    cat("[LOAD] ", ofile)
    load(ofile)
  }
  cat(" -", nrow(dt.Campaigns), "\n")
  
  invisible(dt.Campaigns)
}

#--------

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
    main_01_DP_prepare_input_campaigns(opt$month)
  }
}
