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

__author__ = 'Tamby Kaghdo'


def getstuff(filename, criterion):
    with open(filename, "rb") as csvfile:
        datareader = csv.reader(csvfile)
        count = 0
        for row in datareader:
            if row[3] in ("column header", criterion):
                yield row
                count += 1
            elif count < 2:
                continue
            else:
                return


def predict(classifier, page_view_file_mode, cross_validation_switch):

    if classifier == "random_forest":
        print("Using Random Forest")
        # load files and transform the train and test files
        if page_view_file_mode == "sample":
            print("loadind and joining data sets (sample page views)")
            uod.load_files(load_full_page_views=False)
        elif page_view_file_mode == "full":
            print("loadind and joining data sets (full page views)")
            uod.load_files(load_full_page_views=True)

        print("finished loadind and joining files")

        try:
            # open train file
            print("open the train file")
            train_df = pd.read_csv("./cleaned_data/train.csv")

        except IOError as e:
            print("ERROR: loading files unsuccessful")
            print(e)
            sys.exit(e.errno)

        # remove row counts
        train_df.drop(train_df.columns[0],axis=1,inplace=True)

        features_columns = ["document_id", "topic_id", "source_id", "publisher_id", \
                            "category_id", "platform"]


        X = train_df[features_columns]
        Y = train_df["clicked"]
        rf = RandomForestClassifier(random_state=1, class_weight="balanced", n_estimators=25, max_depth=6)

        if cross_validation_switch == True:
            print("K Fold Cross Validation - RF")
            numFolds = 10
            kf = KFold(len(train_df), numFolds, shuffle=True)
            total = 0
            for train_indices, test_indices in kf:
                train_X = X.ix[train_indices]
                train_Y = Y.ix[train_indices]
                test_X = X.ix[test_indices]
                test_Y = Y.ix[test_indices]

                rf.fit(train_X, train_Y)
                predictions = rf.predict(test_X)
                total += accuracy_score(test_Y, predictions)

            accuracy = total / numFolds
            print("Train with cross validation accuracy score", accuracy)
        else:
            # fit the model
            print("fitting the model without cross validation")
            rf.fit(X, Y)

        try:
            # open test file
            test_df = pd.read_csv("./cleaned_data/test.csv")
            print("opened the test file. Here is how the first 10 rows look like:")
            print(test_df.head())

        except IOError as e:
            print("ERROR: loading files unsuccessful")
            print(e)
            sys.exit(e.errno)

        test_df.drop(test_df.columns[0],axis=1,inplace=True)

        # Use the model to make predictions based on testing data.
        predictions = rf.predict(test_df[features_columns])
        predictions_proba = rf.predict_proba(test_df[features_columns])[:,1]

        test_df["predicted_label"] = predictions
        test_df["predicted_proba"] = predictions_proba

        print("TEST DATA\n")
        print(test_df.head())

        # prepare submission file
        drop_columns = ["document_id", "topic_id",
                        "source_id", "publisher_id", "category_id", "platform",
                        "traffic_source", "clicked", "geo_location_codes"]

        test_df.drop(drop_columns,axis=1,inplace=True)
        test_df.sort_values(by=["display_id", "ad_id", "predicted_label", "predicted_proba"], ascending=[False, False, False, False], inplace=True)

        test_df.drop(["predicted_label", "predicted_proba"], axis=1, inplace=True)

        test_group_by = test_df.groupby("display_id")["ad_id"].apply(list)

        submission_dict = {}

        # create a dictionary to hold display ids and unique ad ids
        for index, row in test_group_by.iteritems():
            unique_lst = []
            for i in row:
                if i not in unique_lst:
                    unique_lst.append(i)
            submission_dict[index] = unique_lst

        # write to the submission file
        with open("./cleaned_data/submission.csv", "w") as submission_file_handle:
            file_writer = csv.writer(submission_file_handle, delimiter=',', quoting=csv.QUOTE_NONE, quotechar='', lineterminator='\n')
            file_writer.writerow(["display_id","ad_id"])
            for key, value in submission_dict.items():
                ad_id_str = ""
                for i, v in enumerate(value):
                    if i != 0:
                        ad_id_str += " " + str(v)
                    else:
                        ad_id_str += str(v)

                file_writer.writerow([key, ad_id_str])
    elif classifier == "SGD":
        print("Using SGD")
        # load files and transform the train and test files
        if page_view_file_mode == "sample":
            print("loadind and joining data sets (sample page views)")
            uod.load_files(load_full_page_views=False)
        elif page_view_file_mode == "full":
            print("loadind and joining data sets (full page views)")
            #uod.load_files(load_full_page_views=True)

        print("finished loadind and joining files")


        #TODO open the train file in chunks
        chunksize = 1
        clf2 = SGDClassifier(loss='log', penalty="l2", n_iter=1000)
        for train_df in pd.read_csv("./cleaned_data/train.csv", chunksize=chunksize):
            # remove row counts
            train_df.drop(train_df.columns[0],axis=1,inplace=True)

            features_columns = ["topic_id", "source_id", "publisher_id", \
                                "category_id", "platform"]

            print("****")
            print(train_df.columns.values.tolist())
            X = train_df[features_columns]
            Y = train_df["clicked"]


            # fit the model
            print("fitting the model without cross validation")
            clf2.partial_fit(X, Y,classes=np.unique(Y))

        try:
            # open test file
            test_df = pd.read_csv("./cleaned_data/test.csv")

        except IOError as e:
            print("ERROR: loading files unsuccessful")
            print(e)
            sys.exit(e.errno)

        test_df.drop(test_df.columns[0], axis=1, inplace=True)
        predictions = clf2.predict(test_df[features_columns])
        predictions_proba = clf2.predict_proba(test_df[features_columns])[:, 1]

        test_df["predicted_label"] = predictions
        test_df["predicted_proba"] = predictions_proba

        print("TEST DATA\n")
        print(test_df.head())

        # prepare submission file
        drop_columns = ["document_id", "topic_id",
                        "source_id", "publisher_id", "category_id", "platform",
                        "traffic_source", "clicked", "geo_location_codes"]

        test_df.drop(drop_columns, axis=1, inplace=True)
        test_df.sort_values(by=["display_id", "ad_id", "predicted_label", "predicted_proba"],
                            ascending=[False, False, False, False], inplace=True)

        test_df.drop(["predicted_label", "predicted_proba"], axis=1, inplace=True)

        test_group_by = test_df.groupby("display_id")["ad_id"].apply(list)

        submission_dict = {}

        # create a dictionary to hold display ids and unique ad ids
        for index, row in test_group_by.iteritems():
            unique_lst = []
            for i in row:
                if i not in unique_lst:
                    unique_lst.append(i)
            submission_dict[index] = unique_lst

        # write to the submission file
        with open("./cleaned_data/submission.csv", "w") as submission_file_handle:
            file_writer = csv.writer(submission_file_handle, delimiter=',', quoting=csv.QUOTE_NONE, quotechar='',
                                     lineterminator='\n')
            file_writer.writerow(["display_id", "ad_id"])
            for key, value in submission_dict.items():
                ad_id_str = ""
                for i, v in enumerate(value):
                    if i != 0:
                        ad_id_str += " " + str(v)
                    else:
                        ad_id_str += str(v)

                file_writer.writerow([key, ad_id_str])


def main():
    status = True
    use = '''Usage: %prog model_methodload_file_mode classifier cross_validation_on_off
             example 1: python predict.py "sample" "random_forest" "CV"
             example 2: python predict.py "full" "SGD" "NCV"
    '''
    parser = OptionParser(usage=use)
    (options, args) = parser.parse_args()
    print(args)
    if len(args) != 3:
        parser.error("incorrect number of arguments")
        status = False
    else:
        pv_file_mode = args[0]
        classifier = args[1]
        if args[2] == "CV":
            cv_switch = True
        else:
            cv_switch = False

        # use the transformed train and test to predict and create a submission file
        #predict("random_forest")
        #predict("SGD")

        predict(classifier=classifier, page_view_file_mode=pv_file_mode, cross_validation_switch=cv_switch)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
