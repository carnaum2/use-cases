Convergence Model
-----------------

Training
--------

Steps required to train a convergence propensity model and predict new users for a specific month.

To train a model for a specific month:

1. fy17/convergence/src $ RScript train.R --month <month> --end <end>

The train.R script requires the following input files:

/Volumes/Data/CVM/<month>/INPUT/EXTR_AC_FINAL_POSPAGO_<month>.TXT
/Volumes/Data/CVM/<month>/INPUT/EXTR_NIFS_COMPARTIDOS_<month>.TXT
/Volumes/Data/CVM/<month>/OUTPUT/par_propensos_cross_nif_<month>.txt
/Volumes/Data/CVM/<end>/INPUT/EXTR_AC_FINAL_POSPAGO_<end>.TXT
/Volumes/Data/CVM/<end>/INPUT/EXTR_NIFS_COMPARTIDOS_<end>.TXT

and stores them in RData format into the data folder, located in $HOME/fy17/data/.

F.e.:
fy17/convergence/src $ RScript train.R --month 201611 --end 201612


          201611       201612       201701       201702
      |------------|------------|------------|------------|
                   ^            ^   ^
                   |            |   |
                   |            |   today (20170110)

dt.MobileOnly-201611            dt.Convergentes-201612
                                  (convergents)
                   |          /
                   |        /
                   |      /
                   |    /
                   |  /
                   |/
                   |
                   \/

            dt.Convergidos-201611   (converged)
            dt.NoConvergidos-201611 (not converged)

                   |
                   \/

$HOME/fy17/models/201611/rf.model.h2o


Prediction
----------

Steps required to predict new users for a specific month.

To generate predictions at a specific day:

1. fy17/convergence/src $ RScript daily_teradata_extractions.R --pass <password>

   If run on <day> (YYYYMMDD), this will generate a directory $PWD/<day>/INPUT/ with the required files to make predictions:

   /Volumes/Data/CVM/<day>/INPUT/EXTR_AC_FINAL_POSPAGO_<day>.TXT
   /Volumes/Data/CVM/<day>/INPUT/SUN_INFO_CRUCE_<day>.TXT

2. fy17/convergence/src $ RScript predict.R --month <model_month> --day <day>

The predict.R script requires the following input files:

/Volumes/Data/CVM/<day>/INPUT/EXTR_AC_FINAL_POSPAGO_<day>.TXT
/Volumes/Data/CVM/<day>/INPUT/SUN_INFO_CRUCE_<day>.TXT
/Volumes/Data/CVM/<month(day)-1>/OUTPUT/par_propensos_cross_nif_<month(day)-1>.txt

and stores them in RData format into the data folder, located in $HOME/fy17/data/.

BEWARE: Sometimes par_propensos_cross_nif_YYYYMM.txt comes without header. In such cases, take the header of previous month.

Also, it requires the model to use for the predictions:

$HOME/fy17/models/<model_month>/rf.model.h2o

F.e.:
fy17/convergence/src $ RScript predict.R --month 201611 --day 20170124

          201611       201612       201701       201702
      |------------|------------|------------|------------|
                   ^            ^          ^  XXXXXXXXXXXX  <-------------------------------------\
                   |            |          |                                                      |
                   |            |         today (20170124) ---\                                   |
                                                              |                                   |
dt.MobileOnly-201611            dt.Convergentes-201612        |                                   | Actioned by telesales
                                  (convergents)               |                                   | in 201702
                   |          /                               |                                   |
                   |        /                                 |                                   |
                   |      /                                   |                                   
                   |    /                                     |---> $HOME/fy17/predictions/201701/convergence.predictions.20170124.csv
                   |  /                                       |     (to be actioned by telesales in 201702)
                   |/                                         |
                   |                                          |
                   \/                                         |
                                                              |
            dt.Convergidos-201611   (converged)               |
            dt.NoConvergidos-201611 (not converged)           |
                                                              |
                   |                                          |
                   \/                                         |
                                                              |
$HOME/fy17/models/201611/rf.model.h2o  -----------------------/


Evaluation
----------

To accurately evaluate the predictions for 201702 made above, we need to be, at least, in 201703 (to get actual convergents of 201702):

F.e.:
fy17/convergence/src $ RScript 07_evaluate_convergence_model.R --modelmonth 201611 --testmonth 201701 --model h2orf



          201611       201612       201701       201702       201703
      |------------|------------|------------|------------|------------|
                   |                         |            | ^
                   |                         |            | |
                   |                         |            | today (20170310)
                   |                         |            |
                   |                         |            |
                   |                         |            |
                   |                         |            |
                   |       dt.MobileOnly-201701          dt.Convergentes-201702
                   |                         |          /  (convergents)
                   |                         |        /
                   |                         |      /
                   |                         |    /
                   |                         |  /
                   |                         |/
                   |                         |
                   |                         \/
                   |                dt.Convergidos-201701 ----\
                   \/                 (converged)             |---> h2orf.201611.pred.201701.<AUC>.csv
            201611/rf.model.h2o ------------------------------/




Alternative Approach
--------------------

Another possibility is to use the ongoing month to train the model:

F.e.:
fy17/convergence/src $ RScript train.R --month 201612 --end 20170124 --provision 20170124 ; RScript predict.R --month 201612 --day 20170124


          201611       201612       201701       201702
      |------------|------------|------------|------------|
                                ^          ^
                                |          |
                                |         today (20170124) -----------\
                                |          |                          |
                                |          |                          |
                                                                      |
             dt.MobileOnly-201612          dt.Convergentes-20170124   |
                                             (convergents)            |
                                |          /                          |
                                |        /                            |
                                |      /                              |
                                |    /                                |---> convergence.predictions.20170124.csv
                                |  /                                  |     (to be actioned by telesales in 201702)
                                |/                                    |
                                |                                     |
                                \/                                    |
                                                                      |
                         dt.Convergidos-201612   (converged)          |
                         dt.NoConvergidos-201612 (not converged)      |
                                                                      |
                                |                                     |
                                \/                                    |
                                                                      |
                         201612/rf.model.h2o  ------------------------/



To accurately evaluate the predictions for 201702 made above, we need to be, at least, in 201703 (to get actual convergents of 201702):

F.e.:
fy17/convergence/src $ RScript 07_evaluate_convergence_model.R --modelmonth 201612 --testmonth 201701 --model h2orf



          201611       201612       201701       201702       201703
      |------------|------------|------------|------------|------------|
                                |            |            | ^
                                |            |            | |
                                |            |            | today (20170310)
                                |            |            |
                                |            |            |
                                |            |            |
                                |            |            |
                                | dt.MobileOnly-201701    dt.Convergentes-201702
                                |            |          /   (convergents)
                                |            |        /
                                |            |      /
                                |            |    /
                                |            |  /
                                |            |/
                                |            |
                                |            \/
                                |    dt.Convergidos-201701 ----\
                                \/     (converged)             |---> h2orf.201612.pred.201701.<AUC>.csv
                         201612/rf.model.h2o ------------------/
