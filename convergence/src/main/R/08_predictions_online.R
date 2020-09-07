library(data.table)


# Output for Online channel

pred.convergence.20161124 <- fread("~/Downloads/Convergencia Dic 16/pred.convergence.20161124.csv")
head(pred.convergence.20161124)

convergence.predictions.ADSL <- fread("~/Downloads/Convergencia Dic 16/convergence.predictions.ADSL.csv")
head(convergence.predictions.ADSL)

convergence.december <- rbind(pred.convergence.20161124, convergence.predictions.ADSL)
setkey(convergence.december, x_num_ident)

#load("fy17/data/CVM/201611/dt_EXTR_NIFS_COMPARTIDOS_201611.RData")
dt_EXTR_NIFS_COMPARTIDOS <- fread("~/Downloads/Convergencia Dic 16/EXTR_NIFS_COMPARTIDOS_20161213.txt")
names(dt_EXTR_NIFS_COMPARTIDOS)[1] <- "x_num_ident"
head(dt_EXTR_NIFS_COMPARTIDOS)

convergence.no.compartidos <- convergence.december[! x_num_ident %in% dt_EXTR_NIFS_COMPARTIDOS$x_num_ident]
setkey(convergence.no.compartidos, x_num_ident)

setorder(convergence.no.compartidos, -SCORE_PROPENSO_CONV)
head(convergence.no.compartidos)
dim(convergence.no.compartidos)

load("fy17/data/CVM/201611/dt_EXTR_AC_FINAL_POSPAGO_201611.RData")
dt_EXTR_AC_FINAL_POSPAGO.orig <- copy(dt_EXTR_AC_FINAL_POSPAGO)

dt_EXTR_AC_FINAL_POSPAGO <- copy(dt_EXTR_AC_FINAL_POSPAGO.orig)
dt_EXTR_AC_FINAL_POSPAGO <- dt_EXTR_AC_FINAL_POSPAGO[,.(x_num_ident, x_sexo, x_fecha_nacimiento, CODIGO_POSTAL, EMAIL_CLIENTE)]
setkey(dt_EXTR_AC_FINAL_POSPAGO, x_num_ident)
dt_EXTR_AC_FINAL_POSPAGO <- unique(dt_EXTR_AC_FINAL_POSPAGO) # FIXME: WARNING: if multiple rows unique takes the first one!
head(dt_EXTR_AC_FINAL_POSPAGO)

convergence <- dt_EXTR_AC_FINAL_POSPAGO[convergence.no.compartidos]
head(convergence)

Encoding(convergence$x_sexo) <- "latin1"
head(convergence)

# This does not work!
#convergence[, x_sexo             := paste(unique(x_sexo), sep="-", collapse=","),  by = x_num_ident]
#convergence[, x_fecha_nacimiento := paste(unique(x_fecha_nacimiento), sep="-", collapse=","),  by = x_num_ident]
#convergence[, EMAIL_CLIENTE      := paste(unique(EMAIL_CLIENTE), sep="-", collapse=","),  by = x_num_ident]
#head(convergence)

#fix_gender(convergence)

#generate_age(convergence, "201611")

convergence[CODIGO_POSTAL == "", CODIGO_POSTAL := NA]
nrow(convergence[is.na(CODIGO_POSTAL)])

convergence[EMAIL_CLIENTE == "", EMAIL_CLIENTE := NA]
nrow(convergence[is.na(EMAIL_CLIENTE)])

head(convergence)

convergence <- convergence[,.(x_num_ident, x_sexo, EDAD, CODIGO_POSTAL, EMAIL_CLIENTE, SCORE_PROPENSO_CONV)]
head(convergence)

write.table(convergence, file="~/Downloads/convergence.december.csv", sep = "\t", quote = FALSE, dec = ",", row.names = FALSE)
