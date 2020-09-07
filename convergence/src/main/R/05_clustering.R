source("configuration.R")

sourced <- NULL
source("06_train_convergence_model.R")


clustering.feats <- c(
  "DIAS_DESDE_ACTIVACION",
  "ARPU",
  #"CODIGO_POSTAL",
  "FLAG_HUELLA_MOVISTAR",
  "EDAD",
  #"modelo",
  #"PLANDATOS",
  #"sistema_operativo",
  #"COD_SEGFID",
  #"x_plan",
  #"SEG_CLIENTE",
  #"DIAS_HASTA_FIN_CP_VF",
  #"FLAG_HUELLA_JAZZTEL",
  #"PPRECIOS_DESTINO",
  #"COBERTURA_4G_PLUS",
  #"ppid_destino",
  #"CANTIDAD_PENDIENTE",
  #"NUM_POSPAGO",
  #"NUM_TOTAL",
  #"DIAS_DESDE_FECHA_FIN_CP_TARIFA",
  #"MESES_FIN_CP_VF",
  #"PROMOCION_VF",
  #"IS_NA_ARPU",
  #"IS_NA_SEG_CLIENTE",
  #"MESES_FIN_CP_TARIFA",
  #"CUOTAS_PENDIENTES",
  #"PUNTOS",
  #"WITH_EMAIL",
  #"x_nacionalidad",
  #"COBERTURA_4G",
  #"TERMINAL_4G",
  #"FLAG_EBILLING",
  #"LORTAD",
  #"part_status",
  #"DEUDA",
  #"FLAG_FINANCIA_SIMO",
  #"NUM_PREPAGO",
  #"VFSMARTPHONE",
  #"FLAG_FINANCIA",
  #"IS_NA_CUOTAS_PENDIENTES",
  #"IS_NA_FECHA_NACIMIENTO",
  #"IS_NA_FECHA_FIN_CP_TARIFA",
  #"IS_NA_MESES_FIN_CP_TARIFA",
  #"IS_NA_CANTIDAD_PENDIENTE",
  #"IS_NA_MESES_FIN_CP_VF",
  #"FLAG_4G_NODOS",
  #"Flag_4G",
  #"IS_NA_VFSMARTPHONE",
  #"IS_NA_COD_SEGFID",
  #"IS_NA_PUNTOS",
  #"FLAG_NH_REAL",
  #"FLAG_4G_APERTURAS",
  #"FLAG_NH_PREVISTA",
  "CLASS"
)


#source("04_train_convergence_model.R")
month <- 201605
dt.train.orig <- generateTrain(month)

dt.train <- copy(dt.train.orig)
dt.train <- prepareData(dt.train)
dt.train <- dt.train[, colnames(dt.train) %in% clustering.feats, with = F]
dt.train <- dt.train[CLASS == 1]
train.class <- dt.train[["CLASS"]]
dt.train[, CLASS := NULL]
#dt.train[, part_status := NULL]
#dt.train <- model.matrix(~.+0, data=dt.train) # dt.train becomes a matrix!
dt.train <- scale(dt.train)
#head(dt.train)

# Determine number of clusters
wss <- (nrow(dt.train)-1)*sum(apply(dt.train,2,var))
for (i in 2:15) { wss[i] <- sum(kmeans(dt.train,
                                     centers=i)$withinss)
                  #pr4 <- pam(dt.train, 4)
                  #si <- silhouette(pr4)
                }
plot(1:15, wss, type="b", xlab="Number of Clusters",
     ylab="Within groups sum of squares")

# K-Means Cluster Analysis
km <- kmeans(dt.train, centers = 12, nstart = 25)
table(km$cluster, train.class)

c <- clara(dt.train, 12); plot(s <- silhouette(c))
 #ggplot(dt.train, aes(ARPU, CLASS, color = km$cluster)) + geom_point()

#######

dt.train <- dt.train[DIAS_DESDE_ACTIVACION < 9000]

logARPU <- log10(dt.train$ARPU)
logARPU <- logARPU[!is.na(logARPU)]
logARPU <- logARPU[!is.infinite(logARPU)]
dt.train <- cbind(dt.train, logARPU)

dt.train <- dt.train[EDAD > -1500]
dt.train <- dt.train[EDAD < 200]
#dt.train$EDAD <- log10(dt.train$EDAD)


# DIAS_DESDE_ACTIVACION vs ARPU
ggplot(dt.train, aes(logARPU, DIAS_DESDE_ACTIVACION, color = CLASS)) + geom_point()
ggplot(dt.train[CLASS == 0], aes(logARPU, DIAS_DESDE_ACTIVACION, color = CLASS)) + geom_point()
ggplot(dt.train[CLASS == 1], aes(logARPU, DIAS_DESDE_ACTIVACION, color = CLASS)) + geom_point()

# DIAS_DESDE_ACTIVACION vs EDAD
ggplot(dt.train, aes(EDAD, DIAS_DESDE_ACTIVACION, color = CLASS)) + geom_point()
ggplot(dt.train[CLASS == 0], aes(EDAD, DIAS_DESDE_ACTIVACION, color = CLASS)) + geom_point()
ggplot(dt.train[CLASS == 1], aes(EDAD, DIAS_DESDE_ACTIVACION, color = CLASS)) + geom_point()

# DIAS_DESDE_ACTIVACION vs FLAG_HUELLA_MOVISTAR
ggplot(dt.train, aes(FLAG_HUELLA_MOVISTAR, DIAS_DESDE_ACTIVACION, color = CLASS)) + geom_point()
ggplot(dt.train[CLASS == 0], aes(FLAG_HUELLA_MOVISTAR, DIAS_DESDE_ACTIVACION, color = CLASS)) + geom_point()
ggplot(dt.train[CLASS == 1], aes(FLAG_HUELLA_MOVISTAR, DIAS_DESDE_ACTIVACION, color = CLASS)) + geom_point()


library(scatterplot3d)


# Install devtools, if you haven't already.
install.packages("devtools")

library(devtools)
install_github("shinyRGL", "trestletech")

# OPTIONAL (see below)
install_github("rgl", "trestletech", "js-class")


# brew install Caskroom/cask/xquartz
#options(rgl.useNULL=TRUE)
#.rs.restartR()
# Start XQuartz
# Run R from a shell
library(rgl)

colors <- unlist(lapply(dt.train$CLASS, function(x) if (x==0) "red" else "green"))

plot3d(dt.train$DIAS_DESDE_ACTIVACION, dt.train$logARPU, dt.train$EDAD, col=colors, size=3) 
plot3d(dt.train[CLASS == 0]$DIAS_DESDE_ACTIVACION, dt.train[CLASS == 0]$logARPU, dt.train[CLASS == 0]$EDAD, col="red", size=3) 
plot3d(dt.train[CLASS == 1]$DIAS_DESDE_ACTIVACION, dt.train[CLASS == 1]$logARPU, dt.train[CLASS == 1]$EDAD, col="green", size=3) 


#######

library(fpc)
library(cluster)
pamk.best <- pamk(dt.train, krange = 2:20, criterion = "asw", usepam = FALSE)
#pamk.best <- pamk(dt.train, 2)
cat("number of clusters estimated by optimum average silhouette width:", pamk.best$nc, "\n")
#pr <- pam(dt.train, pamk.best$nc)
#plot(pr)
#silhouette(pr)

km$cluster <- as.factor(km$cluster)
#ggplot(dt.train, aes(ARPU, CLASS, color = km$cluster)) + geom_point()

pr <- pam(dt.train, 12)
si <- silhouette(pamk.best)


#######

#Silhouette analysis for determining the number of clusters
library(fpc)
asw <- numeric(20)
for (k in 2:20)
  asw[[k]] <- pam(dt.train, k) $ silinfo $ avg.width
k.best <- which.max(asw)

#######

library(NbClust)
nb <- NbClust(dt.train, diss="NULL", distance = "euclidean", 
              min.nc=2, max.nc=15, method = "kmeans", 
              index = "alllong", alphaBeale = 0.1)
hist(nb$Best.nc[1,], breaks = max(na.omit(nb$Best.nc[1,])))

#######

# Cluster Plot against 1st 2 principal components

# vary parameters for most readable graph
library(cluster)
clusplot(dt.train, km$cluster, color=TRUE, shade=TRUE, labels=2, lines=0)

# Centroid Plot against 1st 2 discriminant functions
library(fpc)
plotcluster(dt.train, km$cluster)
