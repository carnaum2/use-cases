setwd("~/fy17/convergence/src")
source("configuration.R")
#options(run.main=FALSE)
#source("04_train_convergence_model.R")

# months <- c(
#   "201605"
#   ,"201606"
#   ,"201607"
#   ,"201608"
#   ,"201609"
#   #,"201610"
# )

months <- list.dirs(models.folder, full.names = FALSE)
months <- months[months != ""]
months <- sort(months)

df <- NULL
#aux <- 0
for (month in months) {
  #month <- 201607
  ifile <- paste0(models.folder, "/", month, "/rf.model.caret-", month, ".RData")
  cat("[LOAD] ", ifile, "\n")
  load(ifile)
  
  vi  <- varImp(rf.fit)$importance
  colnames(vi) <- paste0(colnames(vi), "_", month)
  #rownames(vi)
  
  if (is.null(df))
    df <- vi
  else {
    df <- merge(df, vi, by=0, all=TRUE)  # merge by row names (by=0 or by="row.names")
    df[is.na(df)] <- 0                   # replace NA values
    row.names(df) <- df$Row.names
    df <- df[-1]
  }
  #aux <- aux + vi["sistema_operativo",]
  
  rm(rf.fit)
}

dfs <- data.frame(importance=rowSums(df))
varimp <- dfs[with(dfs, order(-importance)), , drop = FALSE]
head(varimp, n=10)
#cat(aux)

ofile <- paste0(models.folder, "/rf.model.caret.varimp", ".RData")
save(varimp, file = ofile)
