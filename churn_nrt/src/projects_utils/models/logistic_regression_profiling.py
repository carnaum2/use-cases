import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.linear_model import LogisticRegression

def select_top_coefficients(df, cat_cols, threshold):
    """
    Select coefficients with absolute value above the threshold
    :param df: dataframe containing most important variables
    :param cat_cols: list of categorical variables
    :param threshold: threshold for selecting coefficients
    :return: two dataframes including names of variables with coefficient values
    """
    # One hot encode categorical variables

    columns = list(set(list(df.columns)) - set(cat_cols + ['label']))
    y = np.array(df['label'])
    X = MinMaxScaler().fit(df[columns]).transform(df[columns])

    # Build the classifier
    lr = LogisticRegression(penalty='l1')
    lr.fit(X, y)

    coef_df = pd.DataFrame({'cols': columns, 'coef': lr.coef_.flatten()})
    coef_df['abs_vals'] = np.absolute(coef_df.coef)
    if threshold > 1:
        coef_df = coef_df.sort_values('abs_vals', ascending=False).iloc[:int(threshold)]
    else:
        coef_df = coef_df.sort_values('abs_vals', ascending=False).iloc[:int(np.ceil(threshold * len(coef_df)))]
    positive = coef_df[coef_df.coef >= 0].sort_values('coef', ascending=False)
    negative = coef_df[coef_df.coef < 0].sort_values('coef', ascending=True)

    return [positive, negative]

def parse_lr_rules(spark, df):

    [positive, negative] = [df[0], df[1]]

    positive_pairs = zip(positive['cols'].tolist(), positive['coef'].tolist())

    negative_pairs = zip(negative['cols'].tolist(), negative['coef'].tolist())

    if not positive_pairs:
        positive_list = ["No variable has a significant positive correlation with churn"]
    else:
        positive_list = ["Variable " + p[0] + " has a positive correlation with churn with coef " + str(p[1]) for p in positive_pairs]

    if not negative_pairs:
        negative_list = ["No variable has a significant negative correlation with churn"]
    else:
        negative_list = ["Variable " + p[0] + " has a negative correlation with churn with coef " + str(p[1]) for p in negative_pairs]

    pattern_result = positive_list + negative_list

    # A list with the identified rules is obtained as output

    return pattern_result


def parse_lr_rules_old(spark, df):
    """
    Function to write rules for variables and coefficients
    :param spark: spark session
    :param df: output of the function select_top_coefficients; list [positive, negative]
    :return: saved file with results
    """

    sign = ["increase", "decrease"]
    dict_rules = {}

    for i in range(len(df)):

        if df[i].empty:
            dict_rules[sign[i].upper() + " PROBABILITY CHURN: "] = "No variables exist that significantly " + sign[
                i] + " the probability of churn"
        elif df[i].shape[0] > 2:
            # top = top 30%, mid = mid 40%, and bottom = bottom 30%
            top_var = df[i].iloc[:int(round(0.3 * df[i].shape[0])), :]
            mid_var = df[i].iloc[int(round(0.3 * df[i].shape[0])):int(round(0.7 * df[i].shape[0])), :]
            low_var = df[i].iloc[int(round(0.7 * df[i].shape[0])):, :]
            dict_rules[sign[i].upper() + " PROBABILITY CHURN: High variables"] = list(top_var.cols)
            dict_rules[sign[i].upper() + " PROBABILITY CHURN: Mid variables"] = list(mid_var.cols)
            dict_rules[sign[i].upper() + " PROBABILITY CHURN: Low variables"] = list(low_var.cols)
            dict_rules[sign[i].upper() + " PROBABILITY CHURN: High variables coefficients"] = list(top_var.coef)
            dict_rules[sign[i].upper() + " PROBABILITY CHURN: Mid variables coefficients"] = list(mid_var.coef)
            dict_rules[sign[i].upper() + " PROBABILITY CHURN: Low variables coefficients"] = list(low_var.coef)
        else:
            dict_rules[sign[i].upper() + " PROBABILITY CHURN: Variables"] = list(df[i].cols)
            dict_rules[sign[i].upper() + " PROBABILITY CHURN: Coefficients"] = list(df[i].coef)

    print('[Info] Writing to JSON file')

    for r in dict_rules:
        print "[Info] Rule: " + str(r)

    rules_rdd = spark.sparkContext.parallelize([dict_rules])

    rules_rdd.toDF().show(50, False)

    for (k, v) in dict_rules.items():
        print "[Info] Element in dict_rules - k: " + str(k) + " - v: " + str(v)

    return dict_rules
