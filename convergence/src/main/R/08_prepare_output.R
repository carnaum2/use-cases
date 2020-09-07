library(data.table)

# TODO:
# (function() {
# print(getSrcFilename(sys.call(sys.nframe())))
# })()

# load("~/Downloads/dt.preds.adsl.RData")
# head(dt.preds.adsl)
# 
# dt.preds.adsl.bycp <- dt.preds.adsl[, .N, by=CODIGO_POSTAL]
# head(dt.preds.adsl.bycp)
# 
# setkey(dt.preds.adsl.bycp, CODIGO_POSTAL)
# head(dt.preds.adsl.bycp)
# 
# dt.preds.adsl.bycp[, LOG_N := log(N)]
# head(dt.preds.adsl.bycp)
# 
# write.table(dt.preds.adsl.bycp, file="~/Downloads/dt.preds.adsl.bycp.csv", sep = "\t", quote = FALSE, dec = ",", row.names = FALSE)


fix_gender <- function(x) {
  x[x_sexo == "", x_sexo := NA]
  x[x_sexo == "...", x_sexo := NA]
  x[x_sexo == "M", x_sexo := "Female"]
  x[x_sexo == "H", x_sexo := "Male"]
  x[x_sexo == "Var\177n", x_sexo := "Male"]
  x[x_sexo == "Var\xf3n", x_sexo := "Male"]
  x[, x_sexo := as.factor(x_sexo)]
  nrow(x[is.na(x_sexo)])
  unique(x$x_sexo)
  setnames(x, "x_sexo", "GENDER")
}

generate_age <- function (x, month) {
  # Get the last day of the month
  #month <- "201611"
  month.posix <- as.POSIXct(strptime(paste0(month, "01"), "%Y%m%d"))
  last.day <- seq.POSIXt(month.posix, length=2, by="months")[2]
  
  x[, x_fecha_nacimiento := as.POSIXct(strptime(x_fecha_nacimiento, "%Y-%m-%d %H:%M:%S"))]
  #x[is.na(x_fecha_nacimiento), x_fecha_nacimiento := as.POSIXct(strptime("1970-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))]
  x[, EDAD := floor(as.numeric(last.day - x_fecha_nacimiento)/365)]
  nrow(x[is.na(EDAD)])
  x[EDAD < 18, EDAD := NA]
  x[EDAD > 99, EDAD := NA]
  nrow(x[is.na(EDAD)])
  x[, x_fecha_nacimiento := NULL]
}

cp_to_char.default <- function(x) {
  #print(class(x))
  #print(typeof(x))
  #print(x)
  
  #options(warn = 0)
  x <- as.numeric(x)
  #print(x)
  
  if (is.na(x))
    return(NA)
  
  #if (is.numeric(x)) {
  if (x < 1000)
    return(NA)
  
  cp <- ""
  if (x < 10000)
    cp <- "0"
  cp <- paste0(cp, as.character(x))
  #}
  #else if (is.character(x)) {
  #  if (nchar(x) == 4) {
  #    cp <- "0"
  #  }
  #  cp <- paste0(cp, as.character(x))
  #}
  
  return(cp)
}
#cp_to_char(NA)
#cp_to_char(1001)
#cp_to_char(01001)
#cp_to_char(12345)
#cp_to_char("1001")
#cp_to_char("01001")
#cp_to_char("12345")
#cp_to_char("12345a")
cp_to_char <- function(x) {
  unlist(lapply(x, cp_to_char.default))
}
#cp_to_char(c(NA, 0, 00000, "00000", 1001, 01001, 12345, "1001", "01001", "12345", "12345a"))


pred.201607 <- fread("~/fy17/predictions/h2orf.201606.pred.201607.0.84678410316806.csv")
pred.201608 <- fread("~/fy17/predictions/h2orf.201607.pred.201608.0.830104043414842.csv")
pred.201609 <- fread("~/fy17/predictions/h2orf.201608.pred.201609.0.857453645634465.csv")
pred.201610 <- fread("~/fy17/predictions/h2orf.201609.pred.201610.0.833846848855561.csv")
pred.201611 <- fread("~/fy17/predictions/h2orf.201610.pred.201611.0.876403877499133.csv")
pred.201612 <- fread("~/fy17/predictions/h2orf.201611.pred.201612.0.831185666683882.csv")
pred.201701 <- fread("~/fy17/predictions/h2orf.201612.pred.201701.0.875716144222271.csv")
#pred.201702 <- fread("~/fy17/predictions/h2orf.201701.pred.201702.")

# pred.201702 <- fread("fy17/predictions/201701/convergence.predictions.20170124.csv")
# ###
# setnames(pred.201702, "SCORE_PROPENSO_CONV", "PRED")
# pred.201702[, month := "201701"]
# setkey(pred.201702, x_num_ident)
# # load("fy17/datasets/201701/dt.Convergidos-201701.RData") # FIXME: Provisionados
# setkey(dt.Convergidos, x_num_ident)
# dt.Convergidos[, CLASS := 1]
# pred.201702.prueba <- dt.Convergidos[pred.201702]
# pred.201702.prueba <- pred.201702.prueba[, .(x_num_ident, PRED, CLASS, month)]
# pred.201702.prueba[is.na(CLASS), CLASS := 0]
# pred.201702 <- pred.201702.prueba
# nrow(pred.201702[CLASS == 0])
# nrow(pred.201702[CLASS == 1])
# #View(head(pred.201702.prueba, 100))
# ###

# pred.201703 <- fread("fy17/predictions/201702/convergence.predictions.20170222.csv")
# ###
# setnames(pred.201703, "SCORE_PROPENSO_CONV", "PRED")
# pred.201703[, month := "201702"]
# setkey(pred.201703, x_num_ident)
# load("fy17/datasets/201702/dt.Convergidos-201702.RData")
# setkey(dt.Convergidos, x_num_ident)
# dt.Convergidos[, CLASS := 1]
# pred.201703.prueba <- dt.Convergidos[pred.201702]
# pred.201703.prueba <- pred.201702.prueba[, .(x_num_ident, PRED, CLASS, month)]
# pred.201703.prueba[is.na(CLASS), CLASS := 0]
# pred.201703 <- pred.201703.prueba
# nrow(pred.201703[CLASS == 0])
# nrow(pred.201703[CLASS == 1])
# #View(head(pred.201703.prueba, 100))
# ###

predictions <- data.table()
for (m in c(#"201607", "201608", "201609", "201610", "201611",
            "201612", "201701"
            #, "201702"
            )) {
  #m <- "201612"
  pred.m <- switch(m,
                   "201607" = pred.201607,
                   "201608" = pred.201608,
                   "201609" = pred.201609,
                   "201610" = pred.201610,
                   "201611" = pred.201611,
                   "201612" = pred.201612,
                   "201701" = pred.201701,
                   "201702" = pred.201702
                   #"201703" = pred.201703
                   )
  pred.m[, month := m]
  setkey(pred.m, x_num_ident)
  setorder(pred.m, -PRED)
  
  #ifile <- paste0(data.folder,"/CVM/", month, "/dt_EXTR_AC_FINAL_POSPAGO_", month, ".RData")
  ifile <- paste0("fy17/data/CVM/", m, "/dt_EXTR_AC_FINAL_POSPAGO_", m, ".RData")
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  
  # This is a way to get only one row per customer
  setkey(dt_EXTR_AC_FINAL_POSPAGO, x_num_ident)
  dt_EXTR_AC_FINAL_POSPAGO <- unique(dt_EXTR_AC_FINAL_POSPAGO) # FIXME: WARNING: if multiple rows, unique takes the first one!
  
  pred.ac <- dt_EXTR_AC_FINAL_POSPAGO[pred.m]
  if (! "FLAG_COBERTURA_ADSL" %in% names(pred.ac))
    pred.ac[, FLAG_COBERTURA_ADSL := NA]
  #pred.ac[345, V5]
  #unique(pred.ac[, .(ARPU)])
  pred <- pred.ac[, .(x_num_ident, x_sexo, x_fecha_nacimiento, ARPU,
                      FLAG_HUELLA_MOVISTAR,
                      FLAG_HUELLA_JAZZTEL,
                      FLAG_HUELLA_EUSKALTEL,
                      FLAG_HUELLA_ONO,
                      FLAG_HUELLA_VF,
                      FLAG_HUELLA_NEBA,
                      FLAG_COBERTURA_ADSL,
                      CODIGO_POSTAL,
                      PRED,
                      CLASS)]
  #head(pred)
  #tail(pred)
  
  fix_gender(pred)
  generate_age(pred, m)
  pred[CODIGO_POSTAL == "", CODIGO_POSTAL := NA]
  #nrow(pred[is.na(CODIGO_POSTAL)])
  
  pred[, MONTH := m]
  
  predictions <- rbind(predictions, pred)
}

head(predictions)
tail(predictions)

unique(predictions[is.na(as.numeric(CODIGO_POSTAL)),.(CODIGO_POSTAL)])
predictions[is.na(as.numeric(CODIGO_POSTAL)), CODIGO_POSTAL := NA]
unique(predictions[is.na(as.numeric(CODIGO_POSTAL)),.(CODIGO_POSTAL)])

#kk <- predictions[!is.numeric(as.numeric(CODIGO_POSTAL)),]
#unique(kk$CODIGO_POSTAL)
#is.na(as.numeric("01001"))

load("/Users/bbergua/Documents/Codigo/bdaes/data/dt.CodPostalLongLat.rda")
setkey(dt.CodPostalLongLat, CODIGO_POSTAL)

setkey(predictions, CODIGO_POSTAL)
predictions <- dt.CodPostalLongLat[predictions]
#unique(kk$CODIGO_POSTAL)
#cods <- unique(cp_to_char(kk$CODIGO_POSTAL))
#unique(cods)
predictions[, CODIGO_POSTAL := cp_to_char(CODIGO_POSTAL)]

head(predictions)
tail(predictions)

setnames(predictions, "CODIGO_POSTAL", "ZIP_CODE")
setnames(predictions, "long", "LONG")
setnames(predictions, "lat", "LAT")
setnames(predictions, "Municipio", "TOWN")
setnames(predictions, "Provincia", "PROVINCE")
setnames(predictions, "ComAutonom", "STATE")
setnames(predictions, "x_num_ident", "ID")
#setnames(predictions, "x_sexo", "GENDER")
setnames(predictions, "FLAG_HUELLA_MOVISTAR", "MOVISTAR_FINGERPRINT")
setnames(predictions, "FLAG_HUELLA_JAZZTEL", "JAZZTEL_FINGERPRINT")
setnames(predictions, "FLAG_HUELLA_EUSKALTEL", "EUSKALTEL_FINGERPRINT")
setnames(predictions, "FLAG_HUELLA_ONO", "ONO_FINGERPRINT")
setnames(predictions, "FLAG_HUELLA_VF", "VF_FINGERPRINT")
setnames(predictions, "FLAG_HUELLA_NEBA", "NEBA_FINGERPRINT")
setnames(predictions, "FLAG_COBERTURA_ADSL", "ADSL_FINGERPRINT")
setnames(predictions, "EDAD", "AGE")

write.table(predictions, file="~/Downloads/predictions.201612-201701.csv", sep = "\t", quote = FALSE, dec = ",", row.names = FALSE)


### By Zip code

predictions <- fread("~/Downloads/predictions.201612-201701.csv")
predictions[, PRED := gsub(',', '.', predictions$PRED)]
predictions[, PRED := as.numeric(predictions$PRED)]
#predictions$CLASS

th <- .8

#predictions <- predictions[CLASS == 1,]
unique(predictions$CLASS)
summary(predictions)

#predictions <- predictions[, DISCREPANCY_RATE := abs(sum(PRED>=th) - sum(CLASS))/sum(CLASS), by = ZIP_CODE]
#summary(predictions$DISCREPANCY_RATE)

predictions[, PREDICTIONS_ABOVE_THRESHOLD := 0]
predictions[PRED >= th, PREDICTIONS_ABOVE_THRESHOLD := 1]
predictions[, HIT := 0]
predictions[CLASS == 1 & PREDICTIONS_ABOVE_THRESHOLD == 1, HIT := 1]

#preds.by.zip <- predictions[, .(PREDICTIONS_ABOVE_THRESHOLD_COUNT = sum(PREDICTIONS_ABOVE_THRESHOLD)), by = ZIP_CODE]
#tot <- predictions[, .(TOTAL_COUNT = .N), by = ZIP_CODE]
predictions.cod.postal <- predictions[, .(TOT_CLASS = sum(CLASS),
                                          TOT_PRED = sum(PREDICTIONS_ABOVE_THRESHOLD),
                                          TOT_HIT = sum(HIT),
                                          DISCREPANCY = sum(PRED>=th) - sum(CLASS),
                                          #DISCREPANCY_RATE = abs(sum(PRED>=th) - sum(CLASS))/sum(CLASS), 
                                          #PREDICTIONS_ABOVE_THRESHOLD_COUNT = sum(PREDICTIONS_ABOVE_THRESHOLD), 
                                          TOTAL_COUNT = .N), 
                                      by = .(ZIP_CODE, TOWN, PROVINCE, STATE, MONTH)]
predictions.cod.postal <- predictions.cod.postal[! is.na(ZIP_CODE)]
predictions.cod.postal[, DISCREPANCY_RATE := abs(DISCREPANCY)/TOTAL_COUNT]
predictions.cod.postal[, PREDICTION_RATE := TOT_PRED/TOTAL_COUNT]
predictions.cod.postal[, HIT_RATE := TOT_HIT/TOTAL_COUNT]

#predictions.cod.postal <- predictions
setkey(predictions.cod.postal, ZIP_CODE, MONTH)
predictions.cod.postal <- unique(predictions.cod.postal)
#predictions.cod.postal[, x_num_ident := NULL]
nrow(predictions.cod.postal)

head(predictions.cod.postal)
tail(predictions.cod.postal)
#View(head(predictions.cod.postal, 200))

write.table(predictions.cod.postal, file="~/Downloads/predictions.by.cp.201612-201701.csv", sep = "\t", quote = FALSE, dec = ",", row.names = FALSE)
