import numpy as np
import pandas as pd
from sklearn import tree

def select_top_nodes(df, cat_cols, threshold):
    """
    Function to select leaf nodes with at least the threshold number of samples from the same class in the leaf node.
    :param df: dataframe containing most important variables
    :param cat_cols: list of variables that are categorical
    :param threshold: minimum number of samples from the same class to be included on a leaf node for it to be
        selected
    :return: processed dataframe, important leaf nodes, decision tree model, variable containing leaf nodes
    """

    # One hot encode categorical variables
    df = pd.get_dummies(df, columns=cat_cols)
    column_names = list(set(list(df.columns)) - set(['label']))

    # Build decision tree classifier
    dt = tree.DecisionTreeClassifier(criterion='entropy')
    dt.fit(df[column_names], df['label'])

    # Identify leaves in the decision tree for class 1
    leave_id = dt.apply(df[column_names])
    leaves = np.unique(leave_id)
    leaves = [leaf for leaf in leaves if np.argmax(dt.tree_.value[leaf]) == 1]

    if threshold >= 1:
        leaves = sorted(zip(leaves, [np.max(dt.tree_.value[leaf]) for leaf in leaves]), key=lambda x: x[1], reverse=True)
        leaves = leaves[:int(threshold)]
        leaves = [leaf[0] for leaf in leaves]

    if threshold < 1:
        leaves = [leaf for leaf in leaves if np.max(dt.tree_.value[leaf]) / len(df[df.label == 1]) > threshold]

    return [df, leaves, dt, leave_id]


def parse_dt_rules(spark, results):
    """
    Function to write tree rules in written format
    :param spark: spark session
    :param results: output from select_top_nodes
    :return: saved file with results
    """

    print('[Info] Decision Tree method selected')

    if len(results[1]) == 0:
        print("[Error] There are no leaf nodes with a sufficient number of samples for the threshold selected.")

    rules = {}
    label_dict = {0: 'no churn', 1: 'churn'}
    column_names = list(set(list(results[0].columns)) - set(['label']))
    results[0] = results[0][column_names]

    for leaf in results[1]:
        sample = np.where(results[3] == leaf)[0][0]
        node_indicator = results[2].decision_path(results[0])
        node_index = node_indicator.indices[node_indicator.indptr[sample]:
                                            node_indicator.indptr[sample + 1]]

        # For each node_id in node_index get variable name, threshold value and if less or greater than
        rules_dict = {}
        for node_id in node_index:
            if results[3][sample] == node_id:
                continue
            if (results[0].iloc[sample, results[2].tree_.feature[node_id]] <= results[2].tree_.threshold[node_id]):
                sign = "<="
            else:
                sign = ">"
            if column_names[results[2].tree_.feature[node_id]] in rules_dict:
                if sign in rules_dict[column_names[results[2].tree_.feature[node_id]]]:
                    if sign == '<=':
                        rules_dict[column_names[results[2].tree_.feature[node_id]]][sign] = np.min(
                            [rules_dict[column_names[results[2].tree_.feature[node_id]]][sign],
                             results[2].tree_.threshold[node_id]])
                    if sign == '>':
                        rules_dict[column_names[results[2].tree_.feature[node_id]]][sign] = np.max(
                            [rules_dict[column_names[results[2].tree_.feature[node_id]]][sign],
                             results[2].tree_.threshold[node_id]])
                else:
                    rules_dict[column_names[results[2].tree_.feature[node_id]]][sign] = results[2].tree_.threshold[
                        node_id]
            else:
                rules_dict[column_names[results[2].tree_.feature[node_id]]] = {}
                rules_dict[column_names[results[2].tree_.feature[node_id]]][sign] = results[2].tree_.threshold[node_id]

        # Write rules for leaf in text format
        label = str(leaf) + ' ' + label_dict[np.argmax(results[2].tree_.value[leaf])]
        rules[label] = "If "
        for feat in rules_dict:
            if len(rules_dict[feat]) == 1:
                rules[label] += "('{}' {} {}) \t ".format(feat, rules_dict[feat].keys()[0],
                                                          rules_dict[feat].values()[0])
            elif len(rules_dict[feat]) > 1:
                rules[label] += "({} < '{}' <= {}) \t ".format(rules_dict[feat]['>'], feat, rules_dict[feat]['<='])
        rules[label] = rules[label].replace("\t", "&", rules[label].count("\t") - 1)
        rules[label] += "then the observation is predicted as in group " + label_dict[np.argmax(results[2].tree_.value[leaf])]
        rules[label] = rules[label].replace("\t", "")

    # A list with the identified rules is obtained as output

    return rules.values()
