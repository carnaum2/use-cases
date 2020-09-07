source("configuration.R")

option_list <- list(
  make_option(c("-m", "--modelmonth"), type = "character", default = NULL, help = "model month (YYYYMM)", 
              metavar = "character"),
  make_option(c("-t", "--testmonth"), type = "character", default = NULL, help = "(optional) test month (YYYYMM). If not provided, then it is assumed to be <model month> + 2 (f.e. if model month is 201612, then test month is assumed to be 201702)", 
              metavar = "character"),
  make_option(c("-o", "--model"), type = "character", default = NULL, help = "model (f.e.: h2orf, ...)", 
              metavar = "character")
)

opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

if (is.null(opt$modelmonth) | is.null(opt$model)) {
  print_help(opt_parser)
  stop("Please provide, at least, model month (YYYYMM), and model (f.e.: h2orf, ...)", call.=FALSE)
}

# Required files:
#   CVM/<day>/INPUT/EXTR_AC_FINAL_POSPAGO_<test.month+1>.TXT
#   CVM/<day>/INPUT/EXTR_NIFS_COMPARTIDOS_<test.month+1>.TXT

model.month <- opt$modelmonth#"201612"
test.month  <- opt$testmonth#"201702"
model       <- opt$model#"h2orf"

if (is.null(test.month)) {
  first.day.month <- paste0(model.month, "01")
  month.posix <- as.POSIXct(strptime(first.day.month, "%Y%m%d"))
  next.day <- seq.POSIXt(month.posix, length=2, by="+2 months")[2]
  test.month <- format(next.day, format = "%Y%m")
}

# Get the first day of the month
first.day.month <- paste0(test.month, "01")
month.posix <- as.POSIXct(strptime(first.day.month, "%Y%m%d"))
next.day <- seq.POSIXt(month.posix, length=2, by="+1 months")[2]
test.month.next <- format(next.day, format = "%Y%m")


sourced <- NULL


##############
# Evaluation #
##############

source("01_DP_prepare_input.R")
input <- main_01_DP_prepare_input(test.month.next)

source("03_get_convergent_postpaid.R")
dt.Convergentes <- main_03_get_convergent_postpaid(test.month.next, NULL,
                                                   input$dt_EXTR_AC_FINAL_POSPAGO, 
                                                   input$dt_EXTR_NIFS_COMPARTIDOS, 
                                                   NULL)#—-month 201702
rm(input); dev.null <- gc()

source("04_get_convergidos_and_not_convergidos.R")
convergidos <- main_04_get_convergidos_and_not_convergidos(test.month, test.month.next, NULL, dt.Convergentes)
rm(dt.Convergentes); dev.null <- gc()

source("05_clean_convergidos_and_not_convergidos.R")
convergidos.clean <- main_05_clean_convergidos_and_not_convergidos(test.month, convergidos$dt.Convergidos, convergidos$dt.NoConvergidos)
rm(convergidos, convergidos.clean); dev.null <- gc()

source("07_evaluate_convergence_model.R")
main_07_evaluate_convergence_model(model.month, test.month, model)#--modelmonth 201612 --testmonth 201702 --model h2orf
