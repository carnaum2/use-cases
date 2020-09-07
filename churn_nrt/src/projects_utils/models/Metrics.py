


from collections import OrderedDict



class Metrics:

    #DF = None
    AUC = None
    LIFT_IN_TOP_CHURN = None
    #SCORE_COL = None
    #METRICS_STR = None
    METRICS_DICT = OrderedDict()
    FEATS_IMP = None

    def __init__(self, df=None, auc=None, score_col=None, feats_imp=None, lift_in_top_churn=None, add_metrics=None):
        self.AUC = auc
        self.LIFT_IN_TOP_CHURN = lift_in_top_churn
        self.FEATS_IMP = feats_imp
        if score_col and df:
            print("[Metrics] __init__ | Calling __compute_metrics_dict...")
            self.__compute_metrics_dict(df, score_col)
        if add_metrics:
            print("[Metrics] __init__ | Adding {} additional metrics...".format(len(add_metrics)))
            self.METRICS_DICT.update(add_metrics) if isinstance(add_metrics, OrderedDict) else self.METRICS_DICT.update(OrderedDict(add_metrics))
        print("[Metrics] __init__ | self.METRICS_DICT has {} metrics...".format(len(self.METRICS_DICT)))

    def __repr__(self):
        return ";".join(["{}={:.3f}".format(k,v) if isinstance(v, float) else  "{}={}".format(k,v) for k, v in self.METRICS_DICT.items()])

    def set_auc(self, auc):
        self.AUC = auc

    def get_metrics_dict(self):
        return self.METRICS_DICT

    def __compute_metrics_dict(self, df, score_col):
        '''
        Build the dict with metrics
        :param auc:
        :param df_model_scores:
        :param score_col: name of the column that contains the scoring info
        :return:
        '''

        if df is None:
            return OrderedDict()

        score_rdd = df.select([score_col]).rdd.map(lambda r: (r[score_col]))

        avg_score = score_rdd.mean()
        sd_score = score_rdd.stdev()
        sk_score = score_rdd.map(lambda x: pow((x - avg_score) / sd_score, 3.0)).mean()
        ku_score = score_rdd.map(lambda x: pow((x - avg_score) / sd_score, 4.0)).mean()
        min_score = score_rdd.min()
        max_score = score_rdd.max()


        metrics_ = []

        if self.AUC:
            metrics_.append(("roc", self.AUC))
        if self.LIFT_IN_TOP_CHURN:
            metrics_.append(("lift_in_top_churn", self.LIFT_IN_TOP_CHURN))

        metrics_ += [("avg_score", avg_score), ("sd_score", sd_score), ("skewness_score", sk_score),
                     ("kurtosis_score", ku_score), ("min_score", min_score), ("max_score", max_score)]

        self.METRICS_DICT =  OrderedDict(metrics_)




class MetricsRegression:

    RMSE = None
    R2 = None
    METRICS_DICT = None
    FEATS_IMP = None

    def __init__(self, df, rmse, score_col, feats_imp=None, r2=None):
        #self.DF = df # saving DF gave serialization problems
        self.RMSE = rmse
        self.R2 = r2
        self.FEATS_IMP = feats_imp
        self.__compute_metrics_dict(df, score_col)
        print(self.METRICS_DICT)

    def __repr__(self):
        return ";".join(["{}:{}".format(k,v) for k, v in self.METRICS_DICT.items()])

    def set_rmse(self, rmse):
        self.RMSE = rmse

    def get_metrics_dict(self):
        return self.METRICS_DICT

    def __compute_metrics_dict(self, df, score_col):
        '''
        Build the dict with metrics
        :param auc:
        :param df_model_scores:
        :param score_col: name of the column that contains the scoring info
        :return:
        '''

        if df is None:
            return OrderedDict()

        score_rdd = df.select([score_col]).rdd.map(lambda r: (r[score_col]))

        avg_score = score_rdd.mean()
        sd_score = score_rdd.stdev()
        sk_score = score_rdd.map(lambda x: pow((x - avg_score) / sd_score, 3.0)).mean()
        ku_score = score_rdd.map(lambda x: pow((x - avg_score) / sd_score, 4.0)).mean()
        min_score = score_rdd.min()
        max_score = score_rdd.max()

        self.METRICS_DICT =  OrderedDict([("RMSE", self.RMSE), ("R2", self.R2), ("avg_score", avg_score), ("sd_score", sd_score), ("skewness_score", sk_score),
                            ("kurtosis_score", ku_score), ("min_score", min_score), ("max_score", max_score)])






class ModelMetrics:

    MODEL_METRICS_DICT = {}

    def __init__(self):
        self.MODEL_METRICS_DICT = {}

    def create_metrics(self, set_name, df, auc, score_col, feats_imp=None, lift_in_top_churn=None):
        print("[ModelMetrics] create_metrics | Adding metric for set_name={} | auc={} score_col={} lift_in_top_churn={}".format(set_name, auc, score_col, lift_in_top_churn))
        self.MODEL_METRICS_DICT[set_name] = Metrics(df,auc,score_col, feats_imp, lift_in_top_churn)
        self.print_summary()

    def create_metrics_multiclass(self, set_name, metrics_dict):
        print("A-----")
        self.print_summary()
        print("A-----")

        print("[ModelMetrics] create_metrics_multiclass | Adding multiclass metric for set_name={} | num metrics = {}".format(set_name, len(metrics_dict)))
        self.MODEL_METRICS_DICT[set_name] = Metrics(add_metrics=metrics_dict)
        print("B-----")
        self.print_summary()
        print("B-----")

    def create_metrics_regression(self, set_name, df, rmse, score_col, feats_imp=None, r2=None):
        print("[ModelMetrics] create_metrics_regression | Adding regression metric for set_name={} | rmse={} score_col={} r2={}".format(set_name, rmse, score_col, r2))
        self.MODEL_METRICS_DICT[set_name] = MetricsRegression(df,rmse,score_col, feats_imp, r2)
        self.print_summary()

    def set_metrics(self, set_name, metric_obj):
        self.MODEL_METRICS_DICT[set_name] = metric_obj

    def set_auc(self, set_name, auc):
        self.get_model_metrics(set_name).set_auc(auc)

    def get_model_metrics(self, set_name):
        return self.MODEL_METRICS_DICT[set_name]

    def get_metrics_dict(self, set_name):
        return self.get_model_metrics(set_name).get_metrics_dict()

    def print_summary(self):
        print("[ModelMetrics] print_summary | Metrics summary:")
        print("  - number of metrics registered = {}".format(len(self.MODEL_METRICS_DICT)))
        print("  - set names registered = {}".format(",".join(self.MODEL_METRICS_DICT.keys())))
        for set_name, metrics_ in self.MODEL_METRICS_DICT.items():
            print("set_name={} -- {}".format(set_name, metrics_))

    def __repr__(self):
        return "\n".join(["set:{} :: {}".format(set_name, repr(metrics_)) for set_name, metrics_ in self.MODEL_METRICS_DICT.items()])

