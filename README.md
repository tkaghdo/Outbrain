# Outbrain-Click-Prediction
Kaggle competition to predict which recommended content each user will click.

[Competition link](https://www.kaggle.com/c/outbrain-click-prediction)

# Data
clicks_train.csv

* display_id: content recommendations served to a specific user

* ad_id: displayed advertisement id

* clicked: a flag that indicated if the ad (represented by ad_id) was clicked or not (1 for clicked, 0 for not clicked)

sample output

|display_id   |ad_id    |clicked|
| ----------  | ------- | ------|
|           1  |42337   |      0|
|           1  |139684  |      0|
|           1  |144739  |      1|
|           1  |156824  |      0|
|           1  |279295  |      0|
|           1  |296965  |      0|
|           2  |125211  |      0|
|           2  |156535  |      0|
|           2  |169564  |      0|
|           2  |308455  |      1|


clicks_test.csv

* display_id: content recommendations served to a specific user

* ad_id: advertisements shown on the document

sample output

|display_id   |ad_id|
|-------------|-----|
|    16874594  |66758
|    16874594  |150083
|    16874594  |162754
|    16874594  |170392
|    16874594  |172888
|    16874594  |180797
|    16874595  |  8846
|    16874595  |30609
|    16874595  |143982
|    16874596  | 11430


documents_topics.csv

* document_id: html page that contains the all the content including Outbrains's promoted content
* topic_id: the topic type covered in the document
* confidence_level: confidence that the given topic was referred to in the document

sample output

|document_id  |topic_id  |confidence_level|
|-------------|----------|----------------|
|      1595802|       140|          0.073113
1      1595802|        16|          0.059416
2      1595802|       143|          0.045421
3      1595802|       170|          0.038867
4      1524246|       113|          0.196450
5      1524246|       260|          0.142878
6      1524246|        92|          0.033159
7      1524246|       168|          0.014090
8      1524246|        54|          0.008782
9      1524246|       207|          0.008282


documents_entities.csv

* document_id: html page that contains the all the content including Outbrains's promoted content
* entity_id: an entity_id can represent a person, organization, or location
* confidence_level: confidence that the given entity was referred to in the document

sample output

|   document_id|                         entity_id|  confidence_level|
|--------------|----------------------------------|------------------|
|      1524246 | f9eec25663db4cd83183f5c805186f16 |         0.672865
|      1524246 | 55ebcfbdaff1d6f60b3907151f38527a |         0.399114
|      1524246 | 839907a972930b17b125eb0247898412 |         0.392096
|      1524246 | 04d8f9a1ad48f126d5806a9236872604 |         0.213996
|      1617787 | 612a1d17685a498aff4f036c1ee02c16 |         0.386193
|      1617787 | fb8c6cb0879e0de876298177eb1d3fcc |         0.364116
|      1617787 | 793c6a6cf386edb820600d49df045475 |         0.349168
|      1617787 | b525b84d5ed52a34565b8fb870555abe |         0.287005
|      1617787 | 758cb9cb3014607cb4a691cbd485cc94 |         0.237957
|      1617787 | d523aaba6d3916f8b7039fcce0f29639 |         0.235799


documents_meta.csv

* document_id: html page that contains the all the content including Outbrains's promoted content
* source_id: the website (subdomain + domain) displaying the document. ex, edition.cnn.com
* publisher_id: the publisher (domain) hosting the document. ex, cnn.com
* publish_time: the time the document was published on the website (source_id + publisher_id)

sample output

|   document_id|  source_id|  publisher_id|         publish_time|
|--------------|-----------|--------------|---------------------|
|      1595802 |       1.0 |        603.0 | 2016-06-05 00:00:00
|      1524246 |       1.0 |        603.0 | 2016-05-26 11:00:00
|      1617787 |       1.0 |        603.0 | 2016-05-27 00:00:00
|      1615583 |       1.0 |        603.0 | 2016-06-07 00:00:00
|      1615460 |       1.0 |        603.0 | 2016-06-20 00:00:00
|      1615354 |       1.0 |        603.0 | 2016-06-10 00:00:00
|      1614611 |       1.0 |        603.0 | 2016-06-05 13:00:00
|      1614235 |       1.0 |        603.0 | 2016-06-09 00:00:00
|      1614225 |       1.0 |        603.0 | 2016-06-09 00:00:00
|      1488264 |       1.0 |        603.0 | 2016-05-23 13:00:00


documents_catgories.csv

* document_id: html page that contains the all the content including Outbrains's promoted content
* category_id: the category type covered in the document
* confidence_level: confidence that the given category was referred to in the document

sample output

|   document_id|  category_id|  confidence_level|
|--------------|-------------|------------------|
|      1595802 |         1611|          0.920000
|      1595802 |        1610 |         0.070000
|      1524246 |        1807 |         0.920000
|      1524246 |        1608 |         0.070000
|      1617787 |        1807 |         0.920000
|      1617787 |        1608 |         0.070000
|      1615583 |        1305 |         0.920000
|      1615583 |        1806 |         0.070000
|      1615460 |        1613 |         0.540646
|      1615460 |        1603 |         0.041136


events.csv

* display_id: html page that contains the all the content including Outbrains's promoted content
* uuid: user unique id
* document_id: html page that contains the all the content including Outbrains's promoted content
* timestamp: the timestamps are relative to the first time in the dataset. If you wish to recover the actual epoch time of the visit, add 1465876799998 to the timestamp
* platform: desktop = 1, mobile = 2, tablet =3
* geo_location: country>state>DMA

sample output

|   display_id|            uuid|  document_id|  timestamp| platform| geo_location|
|-------------|----------------|-------------|-----------|---------|-------------|
|           1 | cb8c55702adb93 |      379743 |        61 |       3 |   US>SC>519
|           2 | 79a85fa78311b9 |     1794259 |        81 |       2 |   US>CA>807
|           3 | 822932ce3d8757 |     1179111 |       182 |       2 |   US>MI>505
|           4 | 85281d0a49f7ac |     1777797 |       234 |       2 |   US>WV>564
|           5 | 8d0daef4bf5b56 |      252458 |       338 |       2 |       SG>00
|           6 | 7765b4faae4ad4 |     1773517 |       395 |       3 |   US>OH>510
|           7 | 2cc3f6457d16da |     1149661 |       602 |       2 |   US>MT>762
|           8 | 166fc654d73c98 |     1330329 |       638 |       2 |   US>PA>566
|           9 | 9dddccf70f6067 |     1772126 |       667 |       1 |   US>FL>528
|          10 | b09a0e92aa4d17 |      157455 |       693 |       1 |          US


promoted_content.csv
* ad_id: advertisements shown on the document
* document_id: html page that contains the all the content including Outbrains's promoted content
* campaign_id: the ad campaign that is run by the advertiser
* advertiser_id: the advertiser

sample output

|   ad_id|  document_id|  campaign_id|  advertiser_id|
|--------|-------------|-------------|---------------|
|      1 |         6614|            1|              7|
|      2 |      471467 |           2 |             7
|      3 |        7692 |           3 |             7
|      4 |      471471 |           2 |             7
|      5 |      471472 |           2 |             7
|      6 |       12736 |           1 |             7
|      7 |       12808 |           1 |             7
|      8 |      471477 |           2 |             7
|      9 |       13379 |           1 |            7
|     10 |       13885 |           1 |             7


page_views.csv

* uuid: user unique id
* document_id: html page that contains the all the content including Outbrains's promoted content
* timestamp: the timestamps are relative to the first time in the dataset. If you wish to recover the actual epoch time of the visit, add 1465876799998 to the timestamp
* platform: desktop = 1, mobile = 2, tablet =3
* geo_location: country>state>DMA
* traffic_source: internal = 1, search = 2, social = 3


sample output

|             uuid|  document_id|  timestamp|  platform| geo_location|  traffic_source|
|-----------------|-------------|-----------|----------|-------------|----------------|
|  1fd5f051fba643 |         120 |  31905835 |        1 |          RS |  2
|  8557aa9004be3b |         120 |  32053104 |        1 |       VN>44 |  2
|  c351b277a358f0 |         120 |  54013023 |        1 |       KR>12 |  2
|  8205775c5387f9 |         120 |  44196592 |        1 |       IN>16 |  2
|  9cb0ccd8458371 |         120 |  65817371 |        1 |   US>CA>807 |  2
|  2aa611f32875c7 |         120 |  71495491 |        1 |       CA>ON |  2
|  f55a6eaf2b34ab |         120 |  73309199 |        1 |       BR>27 |  2
|  cc01b582c8cbff |         120 |  50033577 |        1 |       CA>BC |  2
|  6c802978b8dd4d |         120 |  66590306 |        1 |       CA>ON |  2
|  f4e423314303ff |         120 |  48314254 |        1 |   US>LA>622 |  1

Usage

python predict.py load_file_mode classifier cross_validation_on_off
running this script will create a submission file for Kaggle competition: https://www.kaggle.com/c/outbrain-click-prediction
example 1: python predict.py "sample" "random_forest" "True"
example 2: python predict.py "full" "SGD" "False"

there are two modes of reading page view files: sample file and full file
this script currently utilizes two types of classifiers: Random Forest and stochastic gradient descent (SGD) learning

Accuracy

Accuracy is calculated by running accuracy_score function from sklearn.metrics and by Mean Average Precision

|Classifier   |Accuracy method                |Accuracy|page view file size (Sample or Full)|
|-------------|-------------------------------|--------|------------------------------------|
|Random Forest| sklearn.metrics.accuracy_score|    0.56|Sample
|SGD          | sklearn.metrics.accuracy_score|    0.74|Sample

