import datetime as dt
import pandas as pd

DEFAULT_TABLE_NAME = "tests_es.churn_log_times"

class TimeLogger:

    table_name = None
    process_name = None

    def __init__(self, process_name, table_name=None):
        self.process_name = process_name
        if table_name:
            self.table_name = table_name
        else:
            self.table_name = DEFAULT_TABLE_NAME


    def register_time(self, spark, step_name, closing_day, start_time, end_time, extra_info=""):

        try:
            df_pandas = pd.DataFrame({"start_time_secs" : [start_time],
                                      "start_time" : [dt.datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S") if start_time != -1 else ""],
                                      "end_time_secs" : [end_time],
                                      "end_time" : [dt.datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S") if end_time != -1 else ""],
                                      "duration" : [end_time - start_time if all([start_time != -1, end_time != -1]) else -1],
                                      "process_name" : [self.process_name],
                                      "step_name" : [step_name],
                                      "closing_day" : [closing_day],
                                      "extra_info"  : [extra_info]
                                     })
            df = spark.createDataFrame(df_pandas)

            (df.write
             .format('parquet')
             .mode('append')
             .saveAsTable(self.table_name))
        except Exception as e:

            print("Error trying to register time: process={} step_name={} closing_day={} start_time={} end_time={} extra_info={}".format(self.process_name,
                                                                                                                                         step_name,
                                                                                                                                         closing_day,
                                                                                                                                         start_time,
                                                                                                                                         end_time,
                                                                                                                                         extra_info))
            print(e)