
import os


CMD_CHURN_REASONS = "spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=16 --executor-cores 4 --executor-memory 32G --driver-memory 32G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 {dir}use-cases/churn/models/churn_reasons/churn_reasons_improved.py -s {segment} -f {dir}use-cases/churn/models/churn_reasons/kernel_20.yaml -p {pred_name} -d {date}"
# CMD_REASONS_ONLYMOB      = "spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.low --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=16 --executor-cores 4 --executor-memory 32G --driver-memory 32G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 {dir}use-cases/churn/models/churn_reasons/churn_reasons_improved.py -s onlymob -f {dir}use-cases/churn/models/churn_reasons/kernel_20.yaml -p {pred_name} -d {date}"
# CMD_REASONS_MOBILEANDFBB = "spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.low --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=16 --executor-cores 4 --executor-memory 32G --driver-memory 32G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 {dir}use-cases/churn/models/churn_reasons/churn_reasons_improved.py -s mobileandfbb -f {dir}use-cases/churn/models/churn_reasons/kernel_20.yaml -p {pred_name} -d {date}"
CMD_CHURN_REASONS_FULL         = "spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=16 --executor-cores 4 --executor-memory 32G --driver-memory 32G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 {dir}use-cases/churn/models/churn_reasons/churn_reasons_final.py -d {date}"


CMD_SCORES_FBB = "spark2-submit --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=15 --executor-cores 4 --executor-memory 60G --driver-memory 8G --conf spark.driver.maxResultSize=16G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 --conf spark.yarn.executor.memoryOverhead=16G --conf spark.shuffle.service.enabled=true --conf spark.driver.extraJavaOptions=-Xss10m latest/use-cases/churn/models/fbb_churn_amdocs/fbb_churn_prod.py -p {date}"
CMD_CHURN_REASONS_FBB = "spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=4 --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.maxExecutors=16 --executor-cores 4 --executor-memory 16G --driver-memory 16G --conf spark.driver.maxResultSize=20G --conf spark.yarn.am.waitTime=800000s --conf spark.network.timeout=10000001 --conf spark.executor.heartbeatInterval=10000000 --conf spark.port.maxRetries=100 {dir}use-cases/churn/models/churn_reasons/churn_reasons_improved_fbb.py -f {dir}use-cases/churn/models/churn_reasons/kernel_20.yaml -p {pred_name} -d {date}"


# CMD_REASONS_DICT = {"onlymob" :      CMD_REASONS_ONLYMOB,
#                     "mobileandfbb" : CMD_REASONS_MOBILEANDFBB,
#                     "full" :         CMD_REASONS_FULL}
CMD_MODEL_EVALUATION = 'spark2-submit --master yarn --deploy-mode client --queue root.BDPtenants.es.medium --conf spark.dynamicAllocation.initialExecutors=8 \
--conf spark.dynamicAllocation.maxExecutors=16 --conf spark.driver.memory=16G --conf spark.executor.memory=25G --conf spark.port.maxRetries=500 \
--conf spark.executor.cores=4 --conf spark.executor.heartbeatInterval=119 --conf spark.sql.shuffle.partitions=20 \
--driver-java-options="-Droot.logger=WARN,console" {} {} {} {}'
CMD_MODEL_EVALUATION_PATH_FILE = "use-cases/churn/models/EvaluarModelosChurn/python_code/churn_Preds_quality.py"


DECILE_COL = "comb_decile"

COLS_EXTENDED = ['msisdn', 'comb_score', 'comb_decile', 'IND_AVERIAS', 'IND_SOPORTE', 'IND_RECLAMACIONES',
                                      'IND_DEGRAD_ADSL', 'IND_TIPIF_UCI', 'IND_PBMA_SRV', 'DETALLE_PBMA_SRV', 'palanca',
                                      'top0_reason', 'top1_reason', 'top2_reason', 'top3_reason', 'top4_reason',
                                      'top5_reason', 'Incertidumbre']
COLS_CLASSIC = ['msisdn', 'comb_score', 'comb_decile', 'IND_AVERIAS', 'IND_SOPORTE', 'IND_RECLAMACIONES',
                                      'IND_DEGRAD_ADSL', 'IND_TIPIF_UCI', 'IND_PBMA_SRV', 'DETALLE_PBMA_SRV', 'palanca']

COLS_REASONS = ['msisdn', 'comb_score', 'comb_decile', 'IND_TIPIF_UCI', 'top0_reason', 'top1_reason', 'top2_reason', 'top3_reason', 'top4_reason',
                                      'top5_reason', 'Incertidumbre']

DELIVERY_TABLE_PREPARED = 'tests_es.churn_team_delivery_{}_{}_prepared'


MODEL_OUTPUTS_NULL_TAG = "null"
MODEL_NAME_DELIV_CHURN = "delivery_churn"
EXTRA_INFO_COLS = ["comb_decile", "IND_TIPIF_UCI", "top0_reason", "top1_reason", "top2_reason", "top3_reason", "top4_reason", "top5_reason", "Incertidumbre"]

MODEL_EVALUATION_NB_DAYS = 30

ALL_SEGMENTS = ["onlymob", "mobileandfbb", "others", "fbb"]




# Labels for delivery script to show the step status
SECTION_CAR = "CAR"
SECTION_CAR_PREPARADO = "CAR PREPARADO"
SECTION_JOIN_CAR = "JOIN PREVIOUS CAR"
SECTION_EXTRA_FEATS = "EXTRA FEATS"
SECTION_SCORES_NEW = "SCORES"
SECTION_DP_LEVERS = "DP LEVERS"
SECTION_DP_CHURN_REASONS = "CHURN REASONS"
SECTION_DELIV_TABLE = "DELIVERY TABLE"
SECTION_DELIV_FILE = "DELIVERY FILE"
SECTION_MODEL_EVALUATION = "MODEL EVAL"
SECTION_CHURN_DELIV_ENTRY = "DELIVERY ENTRY"
FIELD_LENGTH = 19
PADDING_RES = 6


