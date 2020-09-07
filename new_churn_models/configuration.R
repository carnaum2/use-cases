suppressMessages(library(data.table))
suppressMessages(library(randomForest))
suppressMessages(library(h2o))
suppressMessages(library(optparse))
suppressMessages(library(ROCR))
suppressMessages(library(caret))

set.seed(1234)

base.folder <- "/Users/fcastanf/Desktop/CVM-Fede/"

cvm.input.folder       <- paste0(base.folder, "input/CVM/")
cvm.data.folder        <- paste0(base.folder, "data/CVM/")

acc.input.folder       <- paste0(base.folder, "input/Accenture_CAR/")
acc.output.folder      <- paste0(base.folder, "input/Accenture_Out/")
acc.data.folder        <- paste0(base.folder, "data/ACC/")

datasets.folder        <- paste0(base.folder, "datasets/")
churn.datasets.folder  <- paste0(base.folder, "datasets/churn/")

models.folder          <- paste0(base.folder, "/models/churn/")

preds.folder           <- paste0(base.folder, "/predictions/churn/")

valid.tariffs <- c("TARXX", "TREXL",
                   "TARRX","UNLIL", "TRLPL","RXLTA", "TREDL", "TARRL",
                   "RLTAR", "UNLIM", "R2TAR","TRMPL","R1TAR","RSTAR",
                   "TARRM", "TREDM", "UNLIS","2L4RE", "2LIRL", "2L3RM",
                   "2L3RE", "RCTAR", "UNIXL","B4TAR", "B3TAR", "SMTAR",
                   "TARSM","TSMAS", "UNIXS","B2TAR","SSTAR", "TARSS",
                   "TARXS", "2L2SM", "2LINS","BMTAR","TARMS","TMINS",
                   "UIXS2","V15TA","2L1PM","2LMIN","2LMXS", "V6TAR",
                   "SYUTA","MEYTA","YUTAR","TYU15",
                   "PCV01", "PCV02", "PCV03", "PCV10", "PCV11", "PCV12", "PCV13", "PCV14"
                   )
