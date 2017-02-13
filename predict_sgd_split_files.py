"""
predict which recommended content each user will click
there are two modes of reading page view files: sample file and full file
this script currently utilizes two types of classifiers: Random Forest and stochastic gradient descent (SGD) learning
usage: python predict.py load_file_mode classifier cross_validation_on_off
running this script will create a submission file for Kaggle competition: https://www.kaggle.com/c/outbrain-click-prediction
example 1: python predict.py "sample" "random_forest" "True"
example 2: python predict.py "full" "SGD" "False"
"""

import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.cross_validation import cross_val_predict, KFold
from sklearn.metrics import roc_auc_score
import sys
import csv
import understand_outbrain_data as uod
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import accuracy_score
from optparse import OptionParser
import numpy as np
import os
__author__ = 'Tamby Kaghdo'


def predict():

    features_columns = ["document_id", "topic_id", "source_id", "publisher_id", \
                        "category_id", "platform"]

    chunksize = 1000
    clf2 = SGDClassifier(loss='log', penalty="l2")
    for train_df in pd.read_csv("./cleaned_data/train.csv", chunksize=chunksize, iterator=True):
        train_df.dropna(axis=0, inplace=True)
        X = train_df[features_columns]
        Y = train_df["clicked"]
        if len(train_df) > 0:
            clf2.partial_fit(X, Y, classes=[0,1])
        #break

    print("finished fitting")

    test_folder = "./cleaned_data/test/"
    submission_folder = "./submission/"
    count = 1
    for filename in os.listdir(test_folder):
        print("reading file {0}:{1}".format(filename,count))
        test_df = pd.read_csv(test_folder + filename)
        test_df.dropna(axis=0, inplace=True)

        '''
        test_df["document_id"] = test_df["document_id"].astype(int)
        test_df["topic_id"] = test_df["topic_id"].astype(int)
        test_df["source_id"] = test_df["source_id"].astype(int)
        test_df["publisher_id"] = test_df["publisher_id"].astype(int)
        test_df["category_id"] = test_df["category_id"].astype(int)
        test_df["platform"] = test_df["platform"].astype(int)
        '''

        print(test_df["document_id"].iloc[0])
        print(test_df["topic_id"].iloc[0])
        print(test_df["source_id"].iloc[0])
        print(test_df["publisher_id"].iloc[0])
        print(test_df["category_id"].iloc[0])
        print(test_df["platform"].iloc[0])

        print(test_df[features_columns].iloc[0])

        predictions = clf2.predict(test_df[features_columns])
        predictions_proba = clf2.predict_proba(test_df[features_columns])[:, 1]

        test_df["predicted_label"] = predictions
        test_df["predicted_proba"] = predictions_proba

        # prepare submission file
        drop_columns = ["document_id", "topic_id",
                        "source_id", "publisher_id", "category_id", "platform",
                        "traffic_source"]

        test_df.drop(drop_columns, axis=1, inplace=True)

        #append unsorted predcited data sets
        if not os.path.isfile(submission_folder + "unsorted_ungrouped_submission.csv"):
            test_df.to_csv(submission_folder + "unsorted_ungrouped_submission.csv", header=["display_id", "ad_id", "predicted_label", "predicted_proba"], index=False)
        else:  # else it exists so append without writing the header
            test_df.to_csv(submission_folder + "unsorted_ungrouped_submission.csv", mode='a', header=False, index=False)

        count += 1

    #TODO open sunsorted ungrouped ubmission file and drop the pred columns
    """
    test_df = pd.read_csv("./cleaned_data/test.csv")

    predictions = clf2.predict(test_df[features_columns])
    predictions_proba = clf2.predict_proba(test_df[features_columns])[:, 1]

    test_df["predicted_label"] = predictions
    test_df["predicted_proba"] = predictions_proba

    print("TEST DATA\n")
    print(test_df.head())
    """


def main():

        predict()


if __name__ == "__main__":
    sys.exit(0 if main() else 1)