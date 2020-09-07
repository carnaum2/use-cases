library(data.table)
library(reshape2)
library(RColorBrewer)
library(gplots)

analisis_contactados <- function(month, first.month = "201609") {
  first.month.posix <- as.POSIXct(strptime(paste0(first.month, "01"), "%Y%m%d"))
  month.posix <- as.POSIXct(strptime(paste0(month, "01"), "%Y%m%d"))
  prev.month.posix <- seq.POSIXt(month.posix, length=2, by="-1 months")[2]
  prev.month <- format(prev.month.posix, format = "%Y%m")
  sequence <- seq.POSIXt(from = first.month.posix, to = prev.month.posix, by = "months")
  period <- unlist(lapply(sequence, format, "%Y%m"))
  
  
  # Cargamos los planificados para contactar de meses anteriores
  
  contactos <- NULL
  for (m in period) {
    ifile <- file.path("~/Downloads/planificados_convergencia", paste0("Campaigns-Contactos-", m, ".txt"))
    if (file.exists(ifile)) {
      cat("[LOAD]", ifile, "\n")
      contactos_m <- fread(ifile)
      contactos <- rbind(contactos, contactos_m)
    } else {
      cat("[WARN] File", ifile, "does not exist\n")
    }
  }
  rm(contactos_m)
  #View(head(contactos, 200))
  
  # Calculamos el número de contactos de meses anterires
  
  contactos_by_nif <- contactos[, .N, by = CIF_NIF]
  #View(contactos_by_nif)
  setnames(contactos_by_nif, "CIF_NIF", "x_num_ident")
  setkey(contactos_by_nif, x_num_ident)
  
  #hist(contactos_by_nif$N)
  
  # Cargamos los mobile-only a cierre del mes pasado
  
  ifile <- file.path("~/fy17/datasets", prev.month, paste0("dt.MobileOnly-NIFS-", prev.month, ".RData"))
  if (file.exists(ifile)) {
    cat("[LOAD]", ifile, "\n")
    load(ifile)
  } else {
    setwd("~/fy17/convergence/src/main/R/")
    source("02_get_mobile_only_postpaid.R")
    dt.MobileOnly <- main_02_get_mobile_only_postpaid(prev.month, NULL, NULL)
    dt.MobileOnly.nifs <- dt.MobileOnly[, .(x_num_ident)]
  }
  setkey(dt.MobileOnly.nifs, x_num_ident)
  
  # Calculamos el número de contactos pasados de los mobile-only
  
  dt.MobileOnly.nifs.contactos <- contactos_by_nif[dt.MobileOnly.nifs]
  dt.MobileOnly.nifs.contactos[is.na(N), N:= 0]
  setkey(dt.MobileOnly.nifs.contactos, x_num_ident)
  
  cat("Num. contactos de los mobile-only:\n")
  #hist(dt.MobileOnly.nifs.contactos$N)
  print(table(dt.MobileOnly.nifs.contactos$N))
  
  # Cargamos los planificados para contactar este mes
  
  files <- list()
  while (length(files) <= 0) {
    files <- list.files("~/Downloads/planificados_convergencia", pattern = "Campaigns-*", full.names = TRUE, recursive = FALSE)
    cat("Files", length(files), "\n")
  }
  files <- files[files != ""]
  #files <- gsub("CARTOCIUDAD_CALLEJERO_", "", files)
  files <- sort(files)
  ifile <- files[length(files)]
  
  cat("[LOAD]", ifile, "\n")
  contactos_m <- fread(ifile)
  setnames(contactos_m, "CIF_NIF", "x_num_ident")
  setkey(contactos_m, x_num_ident)
  
  ###
  #contactos_m <- contactos_m[CellCode == "01_PROPENSO"]
  ###
  
  print(levels(as.factor(contactos_m$CampaignCode)))
  #contactos_m <- contactos_m[CampaignCode == ""]
  
  # Calculamos el número de contactos pasados de los planificados para contactar este mes
  
  #planif_cur_month <- dt.MobileOnly.nifs.contactos[contactos_m]
  planif_cur_month <- contactos_by_nif[contactos_m]
  planif_cur_month <- planif_cur_month[, .(x_num_ident, N)]
  planif_cur_month[is.na(N), N:= 0]
  setkey(planif_cur_month, x_num_ident)
  cat("Num. contactos de los planificados para contactar este mes:\n")
  print(table(planif_cur_month$N))
  
  # Cargamos los provisionados este mes
  
  files <- list()
  while (length(files) <= 0) {
    files <- list.files("~/Downloads/planificados_convergencia", pattern = paste0("Provisionadas_", month, "-*"), full.names = TRUE, recursive = FALSE)
    cat("Files", length(files), "\n")
  }
  files <- files[files != ""]
  #files <- gsub("CARTOCIUDAD_CALLEJERO_", "", files)
  files <- sort(files)
  ifile <- files[length(files)]
  
  cat("[LOAD]", ifile, "\n")
  provisionados <- fread(ifile)
  provisionados <- provisionados[, .(CIF_NIF)]
  setnames(provisionados, "CIF_NIF", "x_num_ident")
  setkey(provisionados, x_num_ident)
  
  # Calculamos el conversion rate de los planificados este mes por num. de contactos
  
  # planif_cur_month[, CLASS := 0]
  # planif_cur_month[x_num_ident %in% provisionados$x_num_ident, CLASS := 1]
  # setkey(planif_cur_month, N)
  # cr_by_contacts <- planif_cur_month[, .(REDEEMERS = sum(CLASS), TG = .N), by = N]
  # cr_by_contacts[, CONVERSION_RATE := 100*REDEEMERS/TG]
  # print(cr_by_contacts)
  
  # Cargamos las predicciones
  
  files <- list()
  while (length(files) <= 0) {
    files <- list.files(file.path("~/fy17/predictions", prev.month), pattern = paste0("convergence.predictions.", prev.month, ".*.RData"), full.names = TRUE, recursive = FALSE)
    cat("Files", length(files), "\n")
  }
  files <- files[files != ""]
  #files <- gsub("CARTOCIUDAD_CALLEJERO_", "", files)
  files <- sort(files)
  ifile <- files[length(files)]
  
  cat("[LOAD]", ifile, "\n")
  load(ifile)
  setkey(preds.new, x_num_ident)
  
  # Calculamos el conversion rate de los planificados este mes por decil de scoring y por num. contactos
  
  preds.new$decile <- cut(preds.new$SCORE_PROPENSO_CONV, seq(0, 1, 0.1))
  contactos_m_preds <- preds.new[contactos_m]
  contactos_m_preds <- contactos_m_preds[, .(x_num_ident, decile)]

  planif_cur_month[, CLASS := 0]
  planif_cur_month[x_num_ident %in% provisionados$x_num_ident, CLASS := 1]
  setkey(planif_cur_month, x_num_ident)
  planif_cur_month <- contactos_m_preds[planif_cur_month]
  cr_by_decile_contacts <- planif_cur_month[, .(REDEEMERS = sum(CLASS), TG = .N), by = list(N, decile)]
  cr_by_decile_contacts[, CONVERSION_RATE := 100*REDEEMERS/TG]
  setorder(cr_by_decile_contacts, N, -decile)
  print(cr_by_decile_contacts)
  
  # Total results
  
  cr_by_contacts <- cr_by_decile_contacts[, .(REDEEMERS=sum(REDEEMERS), TG=sum(TG)), by = N]
  cr_by_contacts[, CONVERSION_RATE := 100*REDEEMERS/TG]
  print(cr_by_contacts)
  
  cr_by_decile <- cr_by_decile_contacts[, .(REDEEMERS=sum(REDEEMERS), TG=sum(TG)), by = decile]
  cr_by_decile[, CONVERSION_RATE := 100*REDEEMERS/TG]
  print(cr_by_decile)
  
  cat("Total Redeemers:", sum(cr_by_contacts$REDEEMERS), "\n")
  cat("Total TG:", sum(cr_by_contacts$TG), "\n")
  total_cr <- 100*sum(cr_by_contacts$REDEEMERS)/sum(cr_by_contacts$TG)
  cat("Total CR:", total_cr, "\n")
  
  # Visualizations
  
  # By contacts
  bp <- barplot(cr_by_contacts$CONVERSION_RATE, names.arg = cr_by_contacts$N, 
                main = paste0("Conversion Rate in ", month, " by Number of contacts"), 
                sub = paste0("Contact period: ", first.month, "-", prev.month), 
                xlab = "# of contacts in past months", ylab = "Conversion Rate")
  abline(h = total_cr)
  text(bp, cr_by_contacts$CONVERSION_RATE/2, round(cr_by_contacts$CONVERSION_RATE,2))
  
  # By decile
  def.par <- par(no.readonly = TRUE)
  par(mar=c(7,6,4,2) + 0.1)
  par(mgp=c(5,1,0))
  par(las=2)
  bp <- barplot(cr_by_decile$CONVERSION_RATE, names.arg = as.character(cr_by_decile$decile), 
                main = paste0("Conversion Rate in ", month, " by decile"), 
                sub = paste0("Contact period: ", first.month, "-", prev.month), 
                xlab = "# of contacts in past months", ylab = "Conversion Rate")
  abline(h = total_cr)
  text(bp, cr_by_decile$CONVERSION_RATE/2, round(cr_by_decile$CONVERSION_RATE,2))
  par(def.par)  # reset to default
  
  # Heatmap by number of contacts and decile
  
  mat_data <- acast(cr_by_decile_contacts, N~decile, value.var="CONVERSION_RATE")#, fill = 0)
  
  # creates a own color palette from red to green
  my_palette <- colorRampPalette(c("red", "yellow", "green"))(n = 299)
  
  # (optional) defines the color breaks manually for a "skewed" color transition
  col_breaks = c(seq(0,1,length=100),    # for red
                 seq(1.01,3,length=100), # for yellow
                 seq(3.01,6,length=100)) # for green
  
  heatmap.2(mat_data,
            main = "Conversion Rate", # heat map title
            xlab = "Decile",
            ylab = "Number of contacts",
            #cellnote = mat_data,      # same data set for cell labels
            #notecol="black",          # change font color of cell labels to black
            density.info="none",      # turns off density plot inside color legend
            trace="none",             # turns off trace lines inside the heat map
            margins =c(8,5),          # widens margins around plot
            col=my_palette,           # use on color palette defined earlier
            breaks=col_breaks,        # enable color transition at specified limits
            dendrogram="none",        # turn off dendrogram
            Rowv="NA",                # turn off row clustering
            Colv="NA")                # turn off column clustering
  
  
  cr_by_decile_contacts
}
sourced <- NULL

#cr_by_decile_contacts <- analisis_contactados("201706")
#cr_by_decile_contacts <- analisis_contactados("201706", first.month = "201612")
#cr_by_decile_contacts <- analisis_contactados("201706", first.month = "201703")
cr_by_decile_contacts <- analisis_contactados("201707")


# Compute new Scoring

randata <- data.table(SCORE_PROPENSO_CONV = runif(1000), N = floor(7*runif(1000)))
randata$decile <- cut(randata$SCORE_PROPENSO_CONV, seq(0, 1, 0.1))
#barplot(table(randata$N))

randata[, NEW_SCORE_PROPENSO_CONV := 1-0.75*sqrt(1.5*(1-SCORE_PROPENSO_CONV)^2 + (N/6)^2)]
ranby <- randata[, .(MIN = min(NEW_SCORE_PROPENSO_CONV), MEAN = mean(NEW_SCORE_PROPENSO_CONV), MAX = max(NEW_SCORE_PROPENSO_CONV)), by = list(N, decile)]

# ranmin <- min(randata$NEW_SCORE_PROPENSO_CONV)
# ranmax <- max(randata$NEW_SCORE_PROPENSO_CONV)
# randata[, NEW_SCORE_PROPENSO_CONV := NEW_SCORE_PROPENSO_CONV - ranmin]
# randata[, NEW_SCORE_PROPENSO_CONV := NEW_SCORE_PROPENSO_CONV/ranmax]
randata[NEW_SCORE_PROPENSO_CONV < 0, NEW_SCORE_PROPENSO_CONV := 0]
randata[NEW_SCORE_PROPENSO_CONV > 1, NEW_SCORE_PROPENSO_CONV := 1]
ranby <- randata[, .(MIN = min(NEW_SCORE_PROPENSO_CONV), MEAN = mean(NEW_SCORE_PROPENSO_CONV), MAX = max(NEW_SCORE_PROPENSO_CONV)), by = list(N, decile)]

mat_data <- acast(randata, N~decile, fun.aggregate = mean, value.var="NEW_SCORE_PROPENSO_CONV")

# creates a own color palette from red to green
my_palette <- colorRampPalette(c("red", "yellow", "green"))(n = 299)

# (optional) defines the color breaks manually for a "skewed" color transition
col_breaks = c(seq(0,0.33,length=100),    # for red
               seq(0.34,0.67,length=100), # for yellow
               seq(0.68,1,length=100)) # for green

heatmap.2(mat_data,
          main = "Conversion Rate", # heat map title
          xlab = "Decile",
          ylab = "Number of contacts",
          #cellnote = mat_data,      # same data set for cell labels
          #notecol="black",          # change font color of cell labels to black
          density.info="none",      # turns off density plot inside color legend
          trace="none",             # turns off trace lines inside the heat map
          margins =c(8,5),          # widens margins around plot
          col=my_palette,           # use on color palette defined earlier
          breaks=col_breaks,        # enable color transition at specified limits
          dendrogram="none",        # turn off dendrogram
          Rowv="NA",                # turn off row clustering
          Colv="NA")                # turn off column clustering
