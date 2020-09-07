suppressMessages(library(optparse))
suppressMessages(library(data.table))
suppressMessages(library(h2o))
suppressMessages(library(bit64))
suppressMessages(library(ROCR))

set.seed(1234)

base.folder <- "/Users/fede/Desktop/CVM-Fede/"

input.folder.cvm     <- paste0(base.folder, "input/CVM/")
input.folder.acc.car <- paste0(base.folder, "input/Accenture_CAR/")
input.folder.acc.out <- paste0(base.folder, "input/Accenture_Out/")
input.folder.prov    <- paste0(base.folder, "input/Provisionadas/")

input.folder.campaigns <- paste0(base.folder, "input/Campaign/")
 
data.folder        <- paste0(base.folder, "data/")
datasets.folder    <- paste0(base.folder, "datasets/")
models.folder      <- paste0(base.folder, "models/")
predictions.folder <- paste0(base.folder, "predictions/EBU/")
