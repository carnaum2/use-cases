suppressMessages(library(optparse))

# initial.options <- commandArgs(trailingOnly = FALSE)
# file.arg.name <- "--file="
# script.name <- sub(file.arg.name, "", initial.options[grep(file.arg.name, initial.options)])
# script.basename <- dirname(script.name)
# other.name <- paste(sep="/", script.basename, "01_DP_prepare_input_cvm_pospago.R")
# print(paste("Sourcing",other.name,"from",script.name))
# quit()
source("01_DP_prepare_input_cvm_pospago.R")

main_01_DP_prepare_input <- function(month) {
  cat ("\n[MAIN] 01_DP_prepare_input", month, "\n")
  
  res <- list()
  
  # Required files:
  #   <month>/INPUT/EXTR_AC_FINAL_POSPAGO_<month>.TXT
  source("01_DP_prepare_input_cvm_pospago.R")
  res$dt_EXTR_AC_FINAL_POSPAGO <- main_01_DP_prepare_input_cvm_pospago(month)
  
  # Required files:
  #   <month>/INPUT/EXTR_NIFS_COMPARTIDOS_<month>.TXT
  source("01_DP_prepare_input_cvm_nifs_compartidos.R")
  res$dt_EXTR_NIFS_COMPARTIDOS <- main_01_DP_prepare_input_cvm_nifs_compartidos(month)

  # Required files:
  #   <month>/INPUT/SUN_INFO_CRUCE_<month>.TXT
  source("01_DP_prepare_input_cvm_sun_info_cruce.R")
  res$dt_SUN_INFO_CRUCE <- main_01_DP_prepare_input_cvm_sun_info_cruce(month)
  
  # Required files:
  #   <month>/OUTPUT/par_propensos_cross_nif_<month>.txt
  source("01_DP_prepare_input_acc_prop.R")
  res$dt_par_propensos_cross_nif <- main_01_DP_prepare_input_acc_prop(month)
  
  # Required files:
  #   <month>/INPUT/Provisionadas_<month>.txt
  # source("01_DP_prepare_input_provisioned.R")
  # res$dt.provisioned <- main_01_DP_prepare_input_provisioned(month)
  
  # Required files:
  #   <month>__campaign_data_by_customer__number_of_responses_by_channel_and_campaign_type
  #source("01_DP_prepare_input_campaigns.R")
  #res$dt.Campaigns <- main_01_DP_prepare_input_campaigns(month)
  
  #source("01_DP_prepare_input_tarificador.R")
  #res$dt_EXTR_TARIFICADOR_POS <- main_01_DP_prepare_input_tarificador(month)
  
  #source("01_DP_prepare_input_acc_car.R")
  #res$dt.PAR_EXPLIC_CLI_6M <- main_01_DP_prepare_input_acc_car(month)
  
  # Required file:
  #   BDC_GENESIS/BDC_GENESIS_<month>.TXT
  # source("01_DP_prepare_input_bdc_genesis.R")
  # res$dt.BDC_GENESIS <- main_01_DP_prepare_input_bdc_genesis(month)
  
  invisible(res)
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
    main_01_DP_prepare_input(opt$month)
  }
}
