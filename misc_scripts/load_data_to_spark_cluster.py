
import findspark
findspark.init()

import pyspark
from pyspark.sql import SQLContext

def create_spark_table(sqlCtx, file_name, table_name):
    spark_df = sqlCtx.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(file_name)
    spark_df.registerTempTable(table_name)

sc = pyspark.SparkContext()
sqlCtx = SQLContext(sc)

#import files to spark
#create_spark_table(sqlCtx, "../data/clicks_train.csv", "clicks_train")
create_spark_table(sqlCtx, "../data/documents_topics.csv", "my_table")
#create_spark_table(sqlCtx, "../data/documents_entities.csv", "documents_entities")
'''
create_spark_table(sqlCtx, "./data/clicks_test.csv", "clicks_test")
create_spark_table(sqlCtx, "./data/documents_categories.csv", "documents_categories")
create_spark_table(sqlCtx, "./data/documents_meta.csv", "documents_meta")
create_spark_table(sqlCtx, "./data/events.csv", "events")
create_spark_table(sqlCtx, "./data/page_views.csv", "page_views_sample")
create_spark_table(sqlCtx, "./data/promoted_content.csv", "promoted_content")
#create_spark_table(sqlCtx, "./data/page_views.csv", "page_views")
'''
print("\n")


#sqlCtx.sql("select count(*) from clicks_train").show()
#sqlCtx.sql("create table by_document_id_df(document_id int, topic_id int)")

sqlCtx.sql("create table by_document_id_df as (select * from my_table)").show()


#sqlCtx.sql("create table by_document_id_df as (select a.document_id from documents_topics a "
           #"inner join documents_entities b on a.document_id = b.document_id)")
#sqlCtx.sql("select count(*) from by_document_id_df").show()

spark_tables = sqlCtx.tableNames()
print(spark_tables)

