# -*- coding: utf-8 -*-
from pykhaos.utils.constants import WHOAMI
import pykhaos.utils.custom_logger as clogger
logger = clogger.get_custom_logger()
import datetime as dt
from subprocess import Popen, PIPE
import os, time
from pyspark.sql.functions import (length, col)
from pyspark.sql.types import StringType, DoubleType, StructType, StructField



# class DeliveryManager():
#
#     def __init__(self, spark, project_name, campaign_date_label, df_predictions, local_dir_deliverables):
#         '''
#
#         :param spark:
#         :param campaign_date_label: label for the campaign date
#         :param df_predictions: two columns: "score" + "msisdn_hidden"
#         '''
#         self.spark = spark
#         self.project_name = project_name
#         self.campaign_date_label = campaign_date_label
#         self.df_predictions = df_predictions
#         self.local_dir_deliverables = local_dir_deliverables


def create_delivery(spark, project_name, campaign_date_label, df_predictions):

    dir_download = os.path.join(os.environ.get('BDA_USER_HOME', ''), "data", "download", project_name, "")
    dir_delivery = os.path.join(os.environ.get('BDA_USER_HOME', ''), "data", "delivery", project_name, "")
    name_table_anonymized = 'tests_es.{}_tmp_{}_{}_notprepared'.format(WHOAMI, project_name, campaign_date_label )
    name_table_deanonymized = 'tests_es.{}_tmp_{}_{}_prepared'.format(WHOAMI, project_name, campaign_date_label )
    name_file_delivery = '{}_delivery_{}_{}'.format(project_name, campaign_date_label ,
                                                    dt.datetime.now().strftime("%Y%m%d_%H%M%S"))

    print(name_table_anonymized,name_table_deanonymized,name_file_delivery)
    save_pred_as_table(spark, df_predictions, name_table_anonymized)
    logger.info("df_predictions saved as table '{}'".format(name_table_anonymized))
    spark.sql("refresh table {}".format(name_table_anonymized))


    desanonimizar(name_table_anonymized,name_table_deanonymized)
    deliver_table(spark, name_table_deanonymized,name_file_delivery,dir_download,dir_delivery,local_dir_deliverables=None)

def desanonimizar(name_table_anonymized,name_table_deanonymized):

    # sh /home/jsotovi2/desanonimizar.sh --fields msisdn=DE_MSISDN --overwrite tests_es.csanc109_tmp_pre2pos_201804_notprepared tests_es.csanc109_tmp_pre2pos_201804_prepared
    # sh /var/SP/data/home/csanc109/src/desanonimizar_jsotovi2/desanonimizar_csanc109.sh --fields msisdn=DE_MSISDN --overwrite tests_es.csanc109_tmp_pre2post_201902_notprepared tests_es.csanc109_tmp_pre2post_201902_prepared

    print("Starting process for desanonimizar....")
    p = (Popen(['sh',
                '/var/SP/data/home/csanc109/src/desanonimizar_jsotovi2/desanonimizar_csanc109.sh',
                '--fields',
                'msisdn=DE_MSISDN',
                '--overwrite',
                name_table_anonymized,
                name_table_deanonymized], stdin=PIPE, stdout=PIPE, stderr=PIPE, universal_newlines=True))


    print(p.stdout.readline())
    time.sleep(5) # wait until ravel is initialized

    output, err = p.communicate(input = "s\n")

    print 'STDOUT:{} --- {}'.format(output ,err)

    #logger.info("Process ended successfully" if p.returncode==0 else "Oooops.... Retry again")


def deliver_table(spark, hive_name_table, filename, dir_download, dir_delivery, local_dir_deliverables=None,
                  columns=None, sep=",", order_by=None, allow_multiple_partitions=True, file_extension="csv"):
    '''

    :param hive_name_table: table name with the data to write into a file
    :param filename: name of the output file
    :param dir_download: temporary location to download data from table
    :param dir_delivery: location to store the output file
    :param local_dir_deliverables: (optional) If passed, write a trace with the scp command to be used
    :param columns: columns to write. Useful if not all the table columns are written or if you want to change the order
    :param sep: separator character to write the output csv file
    :param order_by: Specify a column to order by. Sorting is done in descending order
    :param allow_multiple_partitions: If true, output may be written in more than one csv.
    :return:
    '''

    spark.sql("refresh table {}".format(hive_name_table))
    hive_table = (spark.read.table(hive_name_table))

    if columns:
        hive_table = hive_table.select(*columns)
        print("These columns will be written: {}".format("|".join(columns)))

    else:
        print("All columns will be written: {}".format("|".join(hive_table.columns)))

    if order_by:
        hive_table = hive_table.orderBy(order_by, ascending=False)

    if not allow_multiple_partitions:
        # Write output in only one file
        hive_table = hive_table.repartition(1)

    print("Columns will be written: {}".format("|".join(hive_table.columns)))
    print("Number of rows = {}".format(hive_table.count()))

    # Write to HDFS
    #logger.info("Writting to hdfs the file {}.csv".format(filename))
    (hive_table
     .write
     .mode('overwrite')
     .csv(filename, header=True, sep=sep))

    # Delete the folder previously (just in case there has been an old version)
    p = (Popen(['rm',
                '-rf',
                os.path.join(dir_download,filename)], stdin=PIPE, stdout=PIPE, stderr=PIPE))

    output, err = p.communicate()
    if err == '':
        print('Directory successfully deleted {}'.format(os.path.join(dir_download, filename)))
    else:
        print("Directory {} could not be deleted".format(os.path.join(dir_download, filename)))

    # Download the *.csv files to local
    print("Moving filename='{}' from dir_download={} to local".format(filename, dir_download))
    p = (Popen(['hadoop',
                'fs',
                '-copyToLocal',
                filename,
                dir_download +'.'], stdin=PIPE, stdout=PIPE, stderr=PIPE))

    output, err = p.communicate()
    if err == '':
        print("Downloaded file {}.csv".format(os.path.join(dir_download, filename)))
    else:
        print(err)
        print("Something happen while trying to download the file {}".format
            (os.path.join(dir_download, filename +".csv")))

    print("Merging {}".format(dir_download +filename +'/*.csv'))
    # Merge the files
    p = (Popen(' '.join(['awk',
                         "'FNR==1 && NR!=1{next;}{print}'",
                         dir_download +filename +'/*.csv',
                         '>',
                         # Hay que poner shell=True cuando se ejecuta una string
                         os.path.join(dir_delivery, filename +"."+file_extension)]), shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE))

    output, err = p.communicate()
    if err == '':
        print("File {}.csv successfully created".format(os.path.join(dir_delivery, filename)))
    else:
        print("Something went wrong creating the file {}".format(os.path.join(dir_delivery, filename)))

    execute_command_in_local = \
        'scp {}@milan-discovery-edge-387:{}.{} {}'.format(os.getenv("USER"),
                                                           os.path.join(dir_delivery, filename),
                                                           file_extension,
                                                           local_dir_deliverables if local_dir_deliverables else "<local_directoy>")

    print \
        ("To download the file in your local computer, type from your local: \n\n {}".format(execute_command_in_local))

    print("Process ended successfully")


def save_pred_as_table(spark, df_predictions, name_table_anonymized):


    sch = StructType([StructField("msisdn", StringType(), True), StructField("score", DoubleType(), True)])

    df_predictions_pyspark = spark.createDataFrame(df_predictions, sch)

    (df_predictions_pyspark.where(col('msisdn').isNotNull())
                                 .where(length(col('msisdn')) == 9)
                                 .write
                                 .format('parquet')
                                 .mode('overwrite')
                                 .saveAsTable(name_table_anonymized))

    print("Created table {}".format(name_table_anonymized))