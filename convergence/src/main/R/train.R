source("configuration.R")

option_list <- list(
  make_option(c("-m", "--month"), type = "character", default = NULL, help = "initial month (YYYYMM)", 
              metavar = "character"),
  make_option(c("-e", "--end"), type = "character", default = NULL, help = "end month (YYYYMM)", 
              metavar = "character"),
  make_option(c("-p", "--provision"), type = "character", default = NULL, help = "(Optional) provision date (YYYYMMDD)", 
              metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

if (is.null(opt$month) | is.null(opt$end)) {
  print_help(opt_parser)
  stop("Please provide initial month (YYYYMM) and end month (YYYYMM)", call.=FALSE)
}

# Required files:
#   CVM/<month>/INPUT/EXTR_AC_FINAL_POSPAGO_<month>.TXT
#   CVM/<month>/INPUT/EXTR_NIFS_COMPARTIDOS_<month>.TXT
#   CVM/<month>/OUTPUT/par_propensos_cross_nif_<month>.txt
#   CVM/<provision>/INPUT/Provisionadas_<provision>.txt
#   BDC_GENESIS/BDC_GENESIS_<month>.TXT

ini.month <- opt$month#"201612"
end.month <- opt$end#"201701"
prov.date <- opt$provision#"20170124"

sourced <- NULL


############
# Training #
############

source("01_DP_prepare_input.R")
input.ini <- main_01_DP_prepare_input(ini.month)
input.end <- main_01_DP_prepare_input(end.month)

# source("02_channel_propensity.R")
# main_02_channel_propensity(ini.month, input.ini$dt.BDC_GENESIS, input.ini$dt_EXTR_AC_FINAL_POSPAGO)

source("02_get_mobile_only_postpaid.R")
dt.MobileOnly <- main_02_get_mobile_only_postpaid(ini.month, input.ini$dt_EXTR_AC_FINAL_POSPAGO, input.ini$dt_EXTR_NIFS_COMPARTIDOS)
rm(input.ini); dev.null <- gc()

source("03_get_convergent_postpaid.R")
dt.Convergentes <- main_03_get_convergent_postpaid(end.month, prov.date,
                                                   input.end$dt_EXTR_AC_FINAL_POSPAGO, 
                                                   input.end$dt_EXTR_NIFS_COMPARTIDOS, 
                                                   input.end$dt.provisioned)#—-month 20170124 —-prov 20170124
rm(input.end); dev.null <- gc()

source("04_get_convergidos_and_not_convergidos.R")
convergidos <- main_04_get_convergidos_and_not_convergidos(ini.month, end.month, dt.MobileOnly, dt.Convergentes)
rm(dt.MobileOnly, dt.Convergentes); dev.null <- gc()

source("05_clean_convergidos_and_not_convergidos.R")
convergidos.clean <- main_05_clean_convergidos_and_not_convergidos(ini.month, convergidos$dt.Convergidos, convergidos$dt.NoConvergidos)
rm(convergidos); dev.null <- gc()

source("06_train_convergence_model.R")
main_06_train_convergence_model(ini.month, 1, 500, "h2orf", "all", "none",
                                convergidos.clean$dt.Convergidos, convergidos.clean$dt.NoConvergidos)#--month 201612 --samples 1 --trees 500 --model h2orf --type all --valid none
rm(convergidos.clean); dev.null <- gc()

# source("07_evaluate_convergence_model.R")
# Evaluation should be done with end.month != ini.month + 1
# Required files:
#   dt.Convergidos-train-<testmonth>.RData
#   dt.NoConvergidos-train-<testmonth>.RData
#   So, EXTR_AC_FINAL_POSPAGO_<testmonth + 1>.TXT is needed
# main_07_evaluate_convergence_model(ini.month, end.month, "h2orf")#--month 201610 --testmonth 201611 --model h2orf
#print("\nEvaluate with: Rscript 07_evaluate_convergence_model.R --month", ini.month, "--testmonth",  end.month, "--model h2orf\n")
