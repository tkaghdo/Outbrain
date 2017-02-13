"""
This script is used to understand the basics of the data provided
and to do some cleanup and create complete train and test files
"""

import pandas as pd
import zipfile
import sys

__author__ = 'Tamby Kaghdo'


def load_files(load_full_page_views=False):
    """
    loads files from zip files and joins them together to created a train and test files with more features

    :param load_full_page_views: If True, loads the full page view files else, load the sample file
    :return: None
    """
    try:
        print("Loading Files...")
        # clicks_train
        zf = zipfile.ZipFile('./data/clicks_train.csv.zip')
        clicks_train_df = pd.read_csv(zf.open('clicks_train.csv'))

        # clicks_test
        zf = zipfile.ZipFile('./data/clicks_test.csv.zip')
        clicks_test_df = pd.read_csv(zf.open('clicks_test.csv'))

        # documents_topics
        zf = zipfile.ZipFile('./data/documents_topics.csv.zip')
        documents_topics_df = pd.read_csv(zf.open('documents_topics.csv'))
        documents_topics_df.drop("confidence_level", axis=1, inplace=True)

        # documents_entities
        zf = zipfile.ZipFile('./data/documents_entities.csv.zip')
        documents_entities_df = pd.read_csv(zf.open('documents_entities.csv'))
        documents_entities_df.drop("confidence_level", axis=1, inplace=True)

        # documents_meta
        zf = zipfile.ZipFile('./data/documents_meta.csv.zip')
        documents_meta_df = pd.read_csv(zf.open('documents_meta.csv'))

        # document_categories
        zf = zipfile.ZipFile('./data/documents_categories.csv.zip')
        documents_categories_df = pd.read_csv(zf.open('documents_categories.csv'))
        documents_categories_df.drop("confidence_level", axis=1, inplace=True)

        # load the sample file or full file
        if load_full_page_views == False:
            # page_views_sample
            # zf = zipfile.ZipFile('./data/page_views.csv.zip')
            page_views_sample_df = pd.read_csv("./data/page_views.csv")
        else:
            #page_views. this file is huge!
            zf = zipfile.ZipFile('./data/page_views.csv.zip')
            page_views_sample_df = pd.read_csv(zf.open('page_views.csv'))
            page_views_sample_df = pd.read_csv("./data/page_views.csv")

        # events
        zf = zipfile.ZipFile('./data/events.csv.zip')
        events_df = pd.read_csv(zf.open('events.csv'))
        # cleanup by removing all rows that that have "\N" as platform. There were only 5
        events_df = events_df[events_df["platform"] != "\\N"]

        # promoted content
        zf = zipfile.ZipFile('./data/promoted_content.csv.zip')
        promoted_content_df = pd.read_csv(zf.open('promoted_content.csv'))

    except IOError as e:
        print("ERROR: loading files unsuccessful")
        print(e)
        sys.exit(e.errno)

    # join documents_topics and documents_entities by document_id
    by_document_id_df = documents_topics_df.merge(documents_entities_df, left_on='document_id', right_on='document_id', how='inner')
    by_document_id_df.rename(columns={'confidence_level_x': 'topic_confidence_level', 'confidence_level_y': 'entities_confidence_level'}, inplace=True)

    # join by_document_id_df with documents_meta_df by document_id
    by_document_id_df = by_document_id_df.merge(documents_meta_df, left_on="document_id", right_on="document_id", how="inner")

    # join by_document_id_df with documents_categories_df by document_id
    by_document_id_df = by_document_id_df.merge(documents_categories_df, left_on="document_id", right_on="document_id", how="inner")
    by_document_id_df.rename(columns={'confidence_level': 'category_confidence_level'}, inplace=True)

    # join page_views_sample with events by uuid, document_id, timestamp, platform, geo_location
    join_columns = ["uuid", "document_id", "timestamp", "platform", "geo_location"]
    page_views_sample_events_df = page_views_sample_df.merge(events_df, left_on=join_columns, right_on=join_columns, how="inner")

    # join by_document_id_df with page_views_sample_events_df by document_id
    by_document_id_with_page_views_sample_events_df = by_document_id_df.merge(page_views_sample_events_df, left_on="document_id", right_on="document_id", how="inner")

    # join above with clicks_train_df and clicks_test_df by display_id
    train_df = by_document_id_with_page_views_sample_events_df.merge(clicks_train_df, left_on="display_id", right_on="display_id", how="inner")
    test_df = by_document_id_with_page_views_sample_events_df.merge(clicks_test_df, left_on="display_id", right_on="display_id", how="inner")

    # drop entity_id, publish_time, uuid, timestamp
    columns_to_drop = ["entity_id", "publish_time", "uuid", "timestamp"]
    train_df = train_df.drop(columns_to_drop, axis=1)
    test_df = test_df.drop(columns_to_drop, axis=1)

    print("Train Info:")
    print(len(train_df))
    print(train_df.head())
    print(train_df.columns.values)

    print("Test Info:")
    print(len(test_df))
    print(test_df.head())
    print(test_df.columns.values)

    # is there any nulls
    print("Train Null Counts:")
    null_counts = train_df.isnull().sum()
    print(null_counts)

    print("Test Null Counts:")
    null_counts = test_df.isnull().sum()
    print(null_counts)

    # which columns have string values
    object_columns_df = train_df.select_dtypes(include=["object"])
    print("Columns with string values:")
    print(object_columns_df.head(1))

    # convert geo_location to numerical (Train)
    train_df["geo_location_categories"] = train_df["geo_location"].astype('category')
    train_df["geo_location_codes"] = train_df["geo_location_categories"].cat.codes
    columns_to_drop = ["geo_location_categories", "geo_location"]
    train_df = train_df.drop(columns_to_drop, axis=1)

    # convert geo_location to numerical (Test)
    test_df["geo_location_categories"] = test_df["geo_location"].astype('category')
    test_df["geo_location_codes"] = test_df["geo_location_categories"].cat.codes
    columns_to_drop = ["geo_location_categories", "geo_location"]
    test_df = test_df.drop(columns_to_drop, axis=1)

    # save train and test data to file
    train_df.to_csv("./cleaned_data/train.csv", sep=',')
    print("saved train data to file")

    train_df.to_csv("./cleaned_data/test.csv", sep=',')
    print("saved test data to file")

    print("Finished loading files")


def main():

    # load files and transform the train and test files
    load_files()

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
