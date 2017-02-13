
import findspark
findspark.init()

import pyspark
from pyspark.sql import SQLContext


sc = pyspark.SparkContext()
#sqlCtx = SQLContext(sc)
from pyspark.sql import HiveContext

sqlCtx= HiveContext(sc)

'''
spark_df = sqlCtx.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("./data/documents_topics.csv")
spark_df.registerTempTable("my_table")
sqlCtx.sql("drop table my_table_3")
sqlCtx.sql("drop table my_table_2")
sqlCtx.sql("CREATE TABLE my_table_3 AS SELECT * from my_table")
sqlCtx.sql("select * from my_table_3").show()
'''
print("---")
print(sc._conf.getAll())
print("----")
spark_df = sqlCtx.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("./data/documents_topics.csv")
spark_df.registerTempTable("documents_topics")
#sqlCtx.sql("select * from documents_topics limit 5").show()

spark_df = sqlCtx.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("./data/documents_entities.csv")
spark_df.registerTempTable("documents_entities")
#sqlCtx.sql("select * from documents_entities limit 5").show()

try:
    sqlCtx.sql("drop table table_1")
except pyspark.sql.utils.AnalysisException as e:
    pass

sqlCtx.sql("CREATE TABLE table_1 AS SELECT a.document_id, a.topic_id, b.entity_id from documents_topics a "
           "inner join documents_entities b on a.document_id = b.document_id")


sqlCtx.sql("drop table documents_topics")
sqlCtx.sql("drop table documents_entities")

spark_df = sqlCtx.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("./data/documents_meta.csv")
spark_df.registerTempTable("documents_meta")
try:
    sqlCtx.sql("drop table table_2")
except pyspark.sql.utils.AnalysisException as e:
    pass

sqlCtx.sql("create table table_2 as select a.document_id, a.topic_id, b.source_id, b.publisher_id "
           "from table_1 a inner join documents_meta b on a.document_id = b.document_id")



spark_df = sqlCtx.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("./data/documents_categories.csv")
spark_df.registerTempTable("documents_categories")

try:
    sqlCtx.sql("drop table table_3")
except pyspark.sql.utils.AnalysisException as e:
    pass
sqlCtx.sql("create table table_3 as select a.document_id, a.topic_id, a.source_id, a.publisher_id, b.category_id "
           "from table_2 a inner join documents_categories b on a.document_id = b.document_id")

try:
    sqlCtx.sql("drop table table_2")
    sqlCtx.sql("drop table documents_categories")
except Exception as e:
    pass

print("*** READING PAGE VIEW ***")
# join page_views and events as table_5
spark_df = sqlCtx.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("./data/page_views.csv")
spark_df.registerTempTable("page_views")
print("FINISHED READING PAGE VIEW ***")
spark_df = sqlCtx.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("./data/events.csv")
spark_df.registerTempTable("events")

try:
    sqlCtx.sql("drop table table_5")
except pyspark.sql.utils.AnalysisException as e:
    pass
except Exception as e:
    pass

print("*** CREATING TABLE 5 ***")
sqlCtx.sql("create table table_5 as select a.document_id, a.timestamp, a.platform, a.geo_location, a.traffic_source"
           ", b.display_id from page_views a inner join "
            "events b on a.uuid = b.uuid and a.document_id = b.document_id "
            "and a.timestamp = b.timestamp and a.platform = b.platform and a.geo_location= b.geo_location").show()
print("FINISHED CREATING TABLE 5 ***")
#sqlCtx.sql("drop table page_views")
#sqlCtx.sql("drop table events")

try:
    sqlCtx.sql("drop table table_6")
except pyspark.sql.utils.AnalysisException as e:
    pass
except Exception as e:
    pass

# place holder for geo_location_code
'''
sqlCtx.sql("create table table_6 as select a.document_id, a.timestamp, a.platform, a.geo_location, a.traffic_source, "
           " a.display_id, b.topic_id, b.source_id, b.publisher_id, b.category_id, cast(a.geo_location as int) as geo_location_code "
           "from table_5 a inner join table_3 b on a.document_id = b.document_id").show()
'''

sqlCtx.sql("create table table_6 as select a.document_id, a.timestamp, a.platform, a.geo_location, a.traffic_source, "
           " a.display_id, b.topic_id, b.source_id, b.publisher_id, b.category_id "
           "from table_5 a inner join table_3 b on a.document_id = b.document_id").show()

#sqlCtx.sql("drop table table_5")
#sqlCtx.sql("drop table table_3")



spark_df = sqlCtx.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("./data/clicks_test.csv")
spark_df.registerTempTable("clicks_train")

try:
    sqlCtx.sql("drop table table_7")
except pyspark.sql.utils.AnalysisException as e:
    pass
except Exception as e:
    pass

print("*** CREATING TABLE 7 ***")
# table 7 is train but geo location needs to be coded.
# I don't know how to coded in sql so I will just remove it for now
sqlCtx.sql("create table table_7 as select a.document_id, a.platform, "
           "a.traffic_source, a.display_id, a.source_id, a.publisher_id, "
           "a.category_id, b.ad_id, a.topic_id from table_6 a inner join clicks_train b on a.display_id = b.display_id")

print("*** FINISHED CREATING TABLE 7 ***")


# create train file from table_7
train_spark_df = sqlCtx.sql("select * from table_7")
train_spark_df.write.csv('./cleaned_data/test_files_from_spark')

# TODO do test file

spark_tables = sqlCtx.tableNames()
print(spark_tables)


