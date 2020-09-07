source("configuration.R")

option_list <- list(
  make_option(c("-m", "--month"), type = "character", default = NULL, help = "model month (YYYYMM)", 
              metavar = "character"),
  make_option(c("-d", "--day"), type = "character", default = NULL, help = "prediction day (YYYYMMDD)", 
              metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

if (is.null(opt$month) | is.null(opt$day)) {
  print_help(opt_parser)
  stop("Please provide month month (YYYYMM) and prediction day (YYYYMMDD)", call.=FALSE)
}

# Required files:
#   models/<month>/rf.model.h2o/h2orf
#   CVM/<day>/INPUT/EXTR_AC_FINAL_POSPAGO_<day>.TXT
#   CVM/<day>/INPUT/EXTR_NIFS_COMPARTIDOS_<day>.TXT
#   CVM/<day>/INPUT/Provisionadas_<day>.txt"
#   CVM/<month-1 of day>/INPUT/EXTR_AC_FINAL_POSPAGO_<month-1 of day>.TXT
#   CVM/<month-1 of day>/OUTPUT/par_propensos_cross_nif_<month-1 of day>.txt
#   BDC_GENESIS/BDC_GENESIS_<month-1 of day>.txt

model.month <- opt$month#"201612"
pred.day    <- opt$day#"20170124"

pred.day.posix <- as.POSIXct(strptime(pred.day, "%Y%m%d"))
pred.day.month <- format(pred.day.posix, format = "%Y%m")
first.day.pred.day.month <- as.POSIXct(strptime(paste0(pred.day.month, "01"), "%Y%m%d"))
pred.day.prev.month.posix <- seq.POSIXt(first.day.pred.day.month, length=2, by="-1 months")[2]
pred.day.prev.month <- format(pred.day.prev.month.posix, format = "%Y%m")

sourced <- NULL


##############
# Prediction #
##############

# source("01_DP_prepare_input.R")
# input.pred.day.prev.month <- main_01_DP_prepare_input(pred.day.prev.month)

source("01_DP_prepare_input_acc_prop.R")
main_01_DP_prepare_input_acc_prop(pred.day.prev.month)

source("01_DP_prepare_input.R")
input <- main_01_DP_prepare_input(pred.day)

# source("02_channel_propensity.R")
# main_02_channel_propensity(pred.day.prev.month, input.pred.day.prev.month$dt.BDC_GENESIS, input.pred.day.prev.month$dt_EXTR_AC_FINAL_POSPAGO)

source("02_get_mobile_only_postpaid.R")
dt.MobileOnly <- main_02_get_mobile_only_postpaid(pred.day, input$dt_EXTR_AC_FINAL_POSPAGO, input$dt_EXTR_NIFS_COMPARTIDOS)
input$dt_EXTR_AC_FINAL_POSPAGO <- NULL; dev.null <- gc()

source("07_prepare_prediction_data.R")
#main_07_prepare_prediction_data(model.month, pred.day, dt.MobileOnly, input$dt_EXTR_NIFS_COMPARTIDOS, input$dt_SUN_INFO_CRUCE, input.pred.day.prev.month$dt_EXTR_AC_FINAL_POSPAGO)#--day 20170124
main_07_prepare_prediction_data(model.month, pred.day, dt.MobileOnly, input$dt_EXTR_NIFS_COMPARTIDOS, input$dt_SUN_INFO_CRUCE, NULL)#--day 20170124
#rm(input, input.pred.day.prev.month, dt.MobileOnly); dev.null <- gc()
rm(input, dt.MobileOnly); dev.null <- gc()
