suppressMessages(library(optparse))
suppressMessages(library(data.table))
suppressMessages(library(bit64))

base.input.folder <- "/Volumes/Data"

input.folder.cvm     <- file.path(base.input.folder, "CVM")
input.folder.acc.in  <- "INPUT"
input.folder.acc.car <- "INTERMEDIOS"
input.folder.acc.out <- "OUTPUT"

input.folder.genesis <- file.path(base.input.folder, "BDC_GENESIS")

input.folder.campaigns <- file.path(base.input.folder, "Campaigns")
 
base.folder <- file.path(Sys.getenv("HOME"), "fy17")

data.folder        <- file.path(base.folder, "data")
datasets.folder    <- file.path(base.folder, "datasets")
models.folder      <- file.path(base.folder, "models")
predictions.folder <- file.path(base.folder, "predictions")

# The first month with ACC data is 201511
first.month.with.data <- "201511"
# The first month with ACC par_propensos_cross_nif data is 201512
first.month.with.data.prop <- "201512"

# PRODVOZ Tarifa de Voz 
convergidos.feats <- c("x_num_ident",
                       "part_status",
                       "x_tipo_ident",
                       "x_fecha_nacimiento",
                       "x_nacionalidad",
                       "SEG_CLIENTE",
                       "COD_SEGFID",
                       "x_fecha_activacion",
                       "X_FECHA_CREACION_CUENTA",
                       "X_FECHA_CREACION_SERVICIO",
                       "x_fecha_ini_prov",
                       "x_plan",
                       "PLANDATOS",
                       "Flag_4G",
                       "COBERTURA_4G",
                       "FLAG_4G_APERTURAS",
                       "FLAG_4G_NODOS",
                       "FLAG_EBILLING",
                       "LORTAD",
                       "DEUDA",
                       "EMAIL_CLIENTE",
                       "PROMOCION_VF",
                       "FECHA_FIN_CP_VF",
                       "MESES_FIN_CP_VF",
                       "PROMOCION_TARIFA",
                       "FECHA_FIN_CP_TARIFA",
                       "MESES_FIN_CP_TARIFA",
                       "modelo",
                       "sistema_operativo",
                       "TERMINAL_4G",
                       
                       "LAST_UPD",
                       "NUM_POSPAGO",
                       "NUM_PREPAGO",
                       "NUM_TOTAL",
                       "PPRECIOS_DESTINO",
                       "ppid_destino",
                       "PUNTOS",
                       "ARPU",
                       "COBERTURA_4G_PLUS",
                       
                       "CODIGO_POSTAL",
                       "VFSMARTPHONE",
                       "FLAG_NH_REAL",
                       "FLAG_NH_PREVISTA",
                       "FLAG_FINANCIA",
                       "FLAG_FINANCIA_SIMO",
                       "CUOTAS_PENDIENTES",
                       "CANTIDAD_PENDIENTE",
                       
                       "FLAG_HUELLA_ONO",
                       "FLAG_HUELLA_VF",
                       "FLAG_HUELLA_NEBA",
                       # "FLAG_HUELLA_VON",
                       "FLAG_HUELLA_MOVISTAR",
                       "FLAG_HUELLA_JAZZTEL",
                       # "FLAG_COBERTURA_ADSL",
                       
                       "SUM_VOZ_M1",
                       "SUM_ADSL_M1",
                       "SUM_HZ_M1", 
                       "SUM_FTTH_M1",
                       "SUM_LPD_M1",
                       "SUM_TIVO_M1",
                       "SUM_FLAGFUTBOL_M1")

noconvergidos.feats <- c("x_num_ident",
                       "part_status",
                       "x_tipo_ident",
                       "x_fecha_nacimiento",
                       "x_nacionalidad",
                       "SEG_CLIENTE",
                       "COD_SEGFID",
                       "x_fecha_activacion",
                       "X_FECHA_CREACION_CUENTA",
                       "X_FECHA_CREACION_SERVICIO",
                       "x_fecha_ini_prov",
                       "x_plan",
                       "PLANDATOS",
                       "Flag_4G",
                       "COBERTURA_4G",
                       "FLAG_4G_APERTURAS",
                       "FLAG_4G_NODOS",
                       "FLAG_EBILLING",
                       "LORTAD",
                       "DEUDA",
                       "EMAIL_CLIENTE",
                       "PROMOCION_VF",
                       "FECHA_FIN_CP_VF",
                       "MESES_FIN_CP_VF",
                       "PROMOCION_TARIFA",
                       "FECHA_FIN_CP_TARIFA",
                       "MESES_FIN_CP_TARIFA",
                       "modelo",
                       "sistema_operativo",
                       "TERMINAL_4G",
                       
                       "LAST_UPD",
                       "NUM_POSPAGO",
                       "NUM_PREPAGO",
                       "NUM_TOTAL",
                       "PPRECIOS_DESTINO",
                       "ppid_destino",
                       "PUNTOS",
                       "ARPU",
                       "COBERTURA_4G_PLUS",
                       
                       "CODIGO_POSTAL",
                       "VFSMARTPHONE",
                       "FLAG_NH_REAL",
                       "FLAG_NH_PREVISTA",
                       "FLAG_FINANCIA",
                       "FLAG_FINANCIA_SIMO",
                       "CUOTAS_PENDIENTES",
                       "CANTIDAD_PENDIENTE",
                       
                       "FLAG_HUELLA_ONO",
                       "FLAG_HUELLA_VF",
                       "FLAG_HUELLA_NEBA",
                       # "FLAG_HUELLA_VON",
                       "FLAG_HUELLA_MOVISTAR",
                       "FLAG_HUELLA_JAZZTEL"
                       #"FLAG_COBERTURA_ADSL"
                      )

# convergidos.feats[! convergidos.feats %in% noconvergidos.feats]
