suppressMessages(library(optparse))
suppressMessages(library(data.table))
suppressMessages(library(bit64))

main <- function(current.date, provision.date) {
  dt.preds <- fread("/Users/fede/Desktop/CVM-Fede/predictions/201611/pred.convergence.20161124.csv")
  cat("\nListado predicciones:                     ", nrow(dt.preds), "\n")
  
  load("/Users/fede/Desktop/CVM-Fede/data/CVM/201611/dt_EXTR_NIFS_COMPARTIDOS_201611.RData")
  dt.preds <- dt.preds[!x_num_ident %in% dt_EXTR_NIFS_COMPARTIDOS$x_num_ident]
  cat("Listado predicciones sin NIFs compartidos: ", nrow(dt.preds), "\n")
  
  #dt.listado <- fread("/Users/fede/Desktop/CVM-Fede/FIBRA_GRATIS_CONTACTADOS_BIG_DATA/Hoja1-Tabla 1.csv")
  dt.listado.all <- fread("/Users/fede/Downloads/Contactos_convergencia_diciembre.txt")
  dt.listado.all[,CellCode := as.factor(CellCode)]
  dt.listado <- dt.listado.all[CellCode %in% c("01_NOPREVEN_BIGDATA", "01_PREVEN_BIGDATA")]
  dt.listado.CC <- dt.listado.all[CellCode %in% c("CC01_NOPREVEN_BIGDATA", "CC01_PREVEN_BIGDATA")]
  
  cat("Listado NIFS en Campaña (Big Data):        ", nrow(dt.listado), "\n")
  
  ifile <- paste0("/Users/fede/Desktop/CVM-Fede/datasets/", current.date, "/dt.Convergentes-NIFS-", current.date, ".RData")
  cat("\n[LOAD] ", ifile, "\n\n")
  load(ifile)
  cat("Total NIFS Cartera Convergentes a Fecha:           ", current.date, " ", nrow(dt.Convergentes), "\n\n")
  
  # Convergidos del listado completo
  dt.preds.convergidos <- dt.preds[x_num_ident %in% dt.Convergentes$x_num_ident]
  cat("Total NIFS convergidos a fecha (LISTADO COMPLETO): ", current.date, " ", nrow(dt.preds.convergidos), "\n")
  cat("Conversion Rate (LISTADO COMPLETO) a Fecha:        ", current.date, " ", (nrow(dt.preds.convergidos)/nrow(dt.preds)), "\n")
  
  # Convergidos del listado completo menos los contactados en el mes anterior
  # dt.campaign.contactos <- fread("/Users/fede/Desktop/CVM-Fede/FIBRA_GRATIS_CONTACTADOS_BIG_DATA/Campaigns-Contactos-201611.txt")
  
  # Convergidos del listado de la campaña
  convergidos.listado <- dt.listado[CIF_NIF %in% dt.Convergentes$x_num_ident]
  convergidos.listado <- convergidos.listado[, .(CIF_NIF)]
  convergidos.listado.CC <- dt.listado.CC[CIF_NIF %in% dt.Convergentes$x_num_ident]
  convergidos.listado.CC <- convergidos.listado.CC[, .(CIF_NIF)]
  
  cat("\nTotal NIFS convergidos a fecha (LISTADO CAMPAÑA):  ", current.date, " ", nrow(convergidos.listado), " de ", nrow(dt.listado), "\n")
  cat("Conversion Rate (LISTADO CAMPAÑA) a Fecha:         ", current.date, " ", (nrow(convergidos.listado)/nrow(dt.listado)), "\n")

  dt.listado.NEBA <- fread("/Users/fede/Desktop/CVM-Fede/FIBRA_GRATIS_CONTACTADOS_BIG_DATA/Campaigns-NEBA_201612.txt")
  dt.listado.NEBA[, CellCode := as.factor(CellCode)]
  dt.listado.NEBA.CC <- dt.listado.NEBA[CellCode == "CC01"]
  dt.listado.NEBA <- dt.listado.NEBA[CellCode == "01"]
  
  # Convergidos del listado de la Campaña de NEBA
  dt.preds.NEBA <- dt.preds[x_num_ident %in% dt.listado.NEBA$CIF_NIF]
  convergidos.listado.NEBA <- dt.preds.NEBA[x_num_ident %in% dt.Convergentes$x_num_ident]
  cat("\nTotal NIFS convergidos a fecha (LISTADO NEBA):     ", current.date, " ", nrow(convergidos.listado.NEBA), " de ", nrow(dt.preds.NEBA), "\n")
  cat("Conversion Rate (LISTADO NEBA) a Fecha:            ", current.date, " ", (nrow(convergidos.listado.NEBA)/nrow(dt.preds.NEBA)), "\n")
  
  # Convergidos CVM
  #dt.listado.cvm <- dt.listado.all[CellCode %in% c("01_NOPREVEN_MODCVM", "01_PREVEN_MODCVM", "CC01_NOPREVEN_MODCVM", "CC01_PREVEN_MODCVM")]
  dt.listado.cvm <- dt.listado.all[CellCode %in% c("01_NOPREVEN_MODCVM", "01_PREVEN_MODCVM")]
  dt.listado.cvm.CC <- dt.listado.all[CellCode %in% c("CC01_NOPREVEN_MODCVM", "CC01_PREVEN_MODCVM")]
  
  convergidos.listado.cvm <- dt.listado.cvm[CIF_NIF %in% dt.Convergentes$x_num_ident]
  convergidos.listado.cvm <- convergidos.listado.cvm[, .(CIF_NIF)]
  convergidos.listado.cvm.CC <- dt.listado.cvm.CC[CIF_NIF %in% dt.Convergentes$x_num_ident]
  convergidos.listado.cvm.CC <- convergidos.listado.cvm.CC[, .(CIF_NIF)]
  
  cat("\nTotal NIFS convergidos a fecha (LISTADO CVM):      ", current.date, " ", nrow(convergidos.listado.cvm), " de ", nrow(dt.listado.cvm), "\n")
  cat("Conversion Rate (LISTADO CVM) a Fecha:             ", current.date, " ", (nrow(convergidos.listado.cvm)/nrow(dt.listado.cvm)), "\n")
  
  #dt.preds.convergidos.solos <- dt.preds.convergidos[!x_num_ident %in% convergidos.listado$CIF_NIF]
  #cat("\nTotal NIFS convergidos a fecha (LISTADO FULL):     ", current.date, " ", nrow(dt.preds.convergidos.solos), " de ", nrow(dt.preds.convergidos), "\n")
  #cat("Conversion Rate (LISTADO FULL) a Fecha:            ", current.date, " ", (nrow(dt.preds.convergidos.solos)/nrow(dt.preds.convergidos)), "\n\n")
 
  # PROVISIONADOS
  cat("\n\n***** PROVISIONADOS *****\n\n")
  dt.provisionados <- fread("/Users/fede/Desktop/CVM-Fede/FIBRA_GRATIS_CONTACTADOS_BIG_DATA/Provisionadas_20170110.txt")
  #dt.provisionados <- dt.provisionados[STATUS == "Completado" | STATUS == "Parcialmente Completado" | STATUS == "Pendiente de Completar"]
  #dt.provisionados <- dt.provisionados[STATUS == "Completado"]
  
  # Convergidos del listado completo
  dt.preds.convergidos <- dt.preds[x_num_ident %in% dt.provisionados$CIF_NIF]
  #cat("Total NIFS convergidos a fecha (LISTADO COMPLETO): ", current.date, " ", nrow(dt.preds.convergidos), "\n")
  #cat("Conversion Rate (LISTADO COMPLETO) a Fecha:        ", current.date, " ", (nrow(dt.preds.convergidos)/nrow(dt.preds)), "\n")
  
  # Convergidos del listado de la campaña
  convergidos.listado.prov <- dt.listado[CIF_NIF %in% dt.provisionados$CIF_NIF]
  convergidos.listado.prov <- convergidos.listado.prov[, .(CIF_NIF)]
  convergidos.all <- rbind(convergidos.listado.prov, convergidos.listado)
  convergidos.all <- convergidos.all[!duplicated(CIF_NIF)]
  
  cat("\nTotal NIFS convergidos a fecha (LISTADO CAMPAÑA):  ", current.date, " ", nrow(convergidos.all), " de ", nrow(dt.listado), "\n")
  cat("Conversion Rate (LISTADO CAMPAÑA) a Fecha:         ", current.date, " ", (nrow(convergidos.all)/nrow(dt.listado)), "\n")
  
  # Control
  convergidos.listado.prov.CC <- dt.listado.CC[CIF_NIF %in% dt.provisionados$CIF_NIF]
  convergidos.listado.prov.CC <- convergidos.listado.prov.CC[, .(CIF_NIF)]
  convergidos.all.CC <- rbind(convergidos.listado.prov.CC, convergidos.listado.CC)
  convergidos.all.CC <- convergidos.all.CC[!duplicated(CIF_NIF)]
  
  cat("\nTotal NIFS convergidos a fecha (LISTADO CAMPAÑA CC):", current.date, " ",nrow(convergidos.all.CC), " de ", nrow(dt.listado.CC), "\n")
  cat("Conversion Rate (LISTADO CAMPAÑA CC) a Fecha:       ", current.date, " ", (nrow(convergidos.all.CC)/nrow(dt.listado.CC)), "\n")
  
  CR.Fibra.BD <- (nrow(convergidos.all)/nrow(dt.listado))
  CR.Fibra.BD.CC <- (nrow(convergidos.all.CC)/nrow(dt.listado.CC))
  uplift <- CR.Fibra.BD/CR.Fibra.BD.CC
  
  cat("Uplift                               a Fecha:       ", current.date," ", uplift, "\n")
  
  
  
  
  # Convergidos del listado de la Campaña de NEBA
  dt.preds.NEBA <- dt.preds[x_num_ident %in% dt.listado.NEBA$CIF_NIF]
  convergidos.listado.NEBA <- dt.preds.NEBA[x_num_ident %in% dt.provisionados$CIF_NIF]
  cat("\nTotal NIFS convergidos a fecha (LISTADO NEBA):     ", current.date, " ", nrow(convergidos.listado.NEBA), " de ", nrow(dt.preds.NEBA), "\n")
  cat("Conversion Rate (LISTADO NEBA) a Fecha:            ", current.date, " ", (nrow(convergidos.listado.NEBA)/nrow(dt.preds.NEBA)), "\n")
  # Control 
  dt.preds.NEBA.CC <- dt.preds[x_num_ident %in% dt.listado.NEBA.CC$CIF_NIF]
  convergidos.listado.NEBA.CC <- dt.preds.NEBA.CC[x_num_ident %in% dt.Convergentes$x_num_ident]
  cat("\nTotal NIFS convergidos a fecha (LISTADO NEBA CC):  ", current.date, " ", nrow(convergidos.listado.NEBA.CC), " de ", nrow(dt.preds.NEBA.CC), "\n")
  cat("Conversion Rate (LISTADO NEBA CC) a Fecha:         ", current.date, " ", (nrow(convergidos.listado.NEBA.CC)/nrow(dt.preds.NEBA.CC)), "\n")
  
  CR.NEBA.BD <- (nrow(convergidos.listado.NEBA)/nrow(dt.preds.NEBA))
  CR.NEBA.BD.CC <- (nrow(convergidos.listado.NEBA.CC)/nrow(dt.preds.NEBA.CC))
  uplift.NEBA <- CR.NEBA.BD/CR.NEBA.BD.CC
  
  cat("Uplift                           a Fecha:          ", current.date," ", uplift.NEBA, "\n")
   
  # Convergidos listado CVM
  convergidos.listado.cvm.prov <- dt.listado.cvm[CIF_NIF %in% dt.provisionados$CIF_NIF]
  convergidos.listado.cvm.prov <- convergidos.listado.cvm.prov[, .(CIF_NIF)]
  convergidos.cvm.all <- rbind(convergidos.listado.cvm.prov, convergidos.listado.cvm)
  convergidos.cvm.all <- convergidos.cvm.all[!duplicated(CIF_NIF)]
  cat("\nTotal NIFS convergidos a fecha (LISTADO CVM):      ", current.date, " ", nrow(convergidos.cvm.all), " de ", nrow(dt.listado.cvm), "\n")
  cat("Conversion Rate (LISTADO CVM) a Fecha:             ", current.date, " ", (nrow(convergidos.cvm.all)/nrow(dt.listado.cvm)), "\n")
  
  # Control
  convergidos.listado.cvm.prov.CC <- dt.listado.cvm.CC[CIF_NIF %in% dt.provisionados$CIF_NIF]
  convergidos.listado.cvm.prov.CC <- convergidos.listado.cvm.prov.CC[, .(CIF_NIF)]
  convergidos.cvm.all.CC <- rbind(convergidos.listado.cvm.prov.CC, convergidos.listado.cvm.CC)
  convergidos.cvm.all.CC <- convergidos.cvm.all.CC[!duplicated(CIF_NIF)]
  cat("\nTotal NIFS convergidos a fecha (LISTADO CVM CC):   ", current.date, " ", nrow(convergidos.cvm.all.CC), " de ", nrow(dt.listado.cvm.CC), "\n")
  cat("Conversion Rate (LISTADO CVM CC) a Fecha:          ", current.date, " ", (nrow(convergidos.cvm.all.CC)/nrow(dt.listado.cvm.CC)), "\n")
  
  CR.Fibra.CVM <- (nrow(convergidos.cvm.all)/nrow(dt.listado.cvm))
  CR.Fibra.CVM.CC <- (nrow(convergidos.cvm.all.CC)/nrow(dt.listado.cvm.CC))
  uplift.cvm <- CR.Fibra.CVM/CR.Fibra.CVM.CC
  
  cat("Uplift                           a Fecha:          ", current.date," ", uplift.cvm, "\n")
  
  # NEBA CVM
  dt.preds.NEBA.cvm <- dt.listado.NEBA[!CIF_NIF %in% dt.preds$x_num_ident]
  convergidos.listado.NEBA.cvm <- dt.preds.NEBA.cvm[CIF_NIF %in% dt.provisionados$CIF_NIF]
  cat("\nTotal NIFS convergidos a fecha (LISTADO NEBA CVM): ", current.date, " ", nrow(convergidos.listado.NEBA.cvm), " de ", nrow(dt.preds.NEBA.cvm), "\n")
  cat("Conversion Rate (LISTADO NEBA CVM) a Fecha:        ", current.date, " ", (nrow(convergidos.listado.NEBA.cvm)/nrow(dt.preds.NEBA.cvm)), "\n")
  # Control 
  dt.preds.NEBA.cvm.CC <- dt.listado.NEBA.CC[!CIF_NIF %in% dt.preds$x_num_ident]
  convergidos.listado.NEBA.cvm.CC <- dt.preds.NEBA.cvm.CC[CIF_NIF %in% dt.Convergentes$x_num_ident]
  cat("\nTotal NIFS convergidos a fecha (LISTADO NEBA CVM CC):  ", current.date, " ", nrow(convergidos.listado.NEBA.cvm.CC), " de ", nrow(dt.preds.NEBA.cvm.CC), "\n")
  cat("Conversion Rate (LISTADO NEBA CVM CC) a Fecha:         ", current.date, " ", (nrow(convergidos.listado.NEBA.cvm.CC)/nrow(dt.preds.NEBA.cvm.CC)), "\n")
  
  CR.NEBA.CVM<- (nrow(convergidos.listado.NEBA.cvm)/nrow(dt.preds.NEBA.cvm))
  CR.NEBA.CVM.CC <- (nrow(convergidos.listado.NEBA.cvm.CC)/nrow(dt.preds.NEBA.cvm.CC))
  uplift.NEBA.cvm <- CR.NEBA.CVM/CR.NEBA.CVM.CC
  
  cat("Uplift                                a Fecha:         ", current.date," ", uplift.NEBA.cvm, "\n")
  
  # Listado sin los que han contactado en el 201511
  cat("\n\n***** SIN CONTACTAR EL MES ANTERIOR *****\n")
  
  load("/Users/fede/Desktop/CVM-Fede/data/Campaign/201611/dt.Campaigns.201611.RData")
  dt.preds.new <- dt.preds[!x_num_ident %in% dt.Campaigns$CIF_NIF]
  cat("Tamaño listado sin contactar mes anterior: ", nrow(dt.preds.new), "\n")
  dt.preds.convergidos.prov <- dt.preds.new[x_num_ident %in% dt.provisionados$CIF_NIF]
  
  cat("Total NIFS convergidos a fecha (LISTADO SIN CONTACTAR): ", current.date, " ", nrow(dt.preds.convergidos.prov), "\n")
  cat("Conversion Rate (LISTADO SIN CONTACTAR) a Fecha:        ", current.date, " ", (nrow(dt.preds.convergidos.prov)/nrow(dt.preds.new)), "\n")
  
  }


#convergidos.listado.score <- dt.preds[x_num_ident %in% convergidos.listado]

#dt.listado.all <- fread("/Users/fede/Downloads/Contactos_convergencia_diciembre.txt")
#dt.listado.all[,CellCode := as.factor(CellCode)]
#dt.listado.cvm <- dt.listado.all[CellCode %in% c("01_NOPREVEN_MODCVM", "01_PREVEN_MODCVM", "CC01_NOPREVEN_MODCVM", "CC01_PREVEN_MODCVM")]

#convergidos.listado.cvm <- dt.listado.cvm[CIF_NIF %in% dt.Convergentes$x_num_ident]$CIF_NIF

#dt.preds.convergidos.duda <- dt.preds.convergidos[!x_num_ident %in% convergidos.listado]

#--------------------------------------------------------------------------------------------------------
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
  main(opt$month)
}
