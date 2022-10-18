# manypets
Data Operations on sample data

Download the code using command below.

```
git clone https://github.com/mindcrusher11/manypets
```
I have used spark and scala for this project to provide scalable, distributed processing.

Configuration settings are defined in application.conf file in resources foolder

```
spark {
  masterurl = "local[*]"
  appName = "omicsspark"
  checkpointDir = "./checkpoint"
  batchDuration = "15"
}

file {
  writeformat = "com.databricks.spark.csv"
  claimsPath = "/partition/DE_test_data_sets/DE_test_claims.csv"
  policiesPath = "/partition/DE_test_data_sets/policies"
}
```
define master url to run on local or cluster
update paths for claims data and policy data.

Once it is updated it can be run using 

sbt tool for scala need to be installed.

```
sbt run
```

Run test cases using 

```
sbt test
```

3 test cases are defined which will display as passed.


Scala Docs for this project can be generated using command below in current porject parent directory.
```
sbt doc
```
It will generate index.html file as "target/scala-2.11/api/index.html".

It can be opened in the browser.

Project can be run using jar file

Steps to create jar file 


```
sbt package
```

Code can run in cluster using command below

```
spark-submit --class "manypetsspark" --master local[4] .target/scala-2.11/manypets_2.11-0.1.0-SNAPSHOT.jar 
```

** Logging is pending

Output will be like

```

+-----------+
|policyCount|
+-----------+
|       8643|
+-----------+

+--------------------+---------+
|                uuid|totalpets|
+--------------------+---------+
|6f4ba89f-97e4-41d...|      2.0|
|f7b208ad-5886-43c...|      1.0|
|46161a16-9138-404...|      2.0|
|6c93e803-3f13-443...|      2.0|
|90dd298e-0b33-4c6...|      3.0|
|443f21fd-69e7-4bf...|      1.0|
|727d6615-30a2-4d8...|      1.0|
|d57a04de-2cff-49d...|      2.0|
|24a5dcab-93b5-43f...|      2.0|
|24dc7679-935e-4da...|      1.0|
|64066fc7-8acb-4d5...|      1.0|
|99b6f209-5592-46d...|      1.0|
|de4daacb-6344-461...|      1.0|
|90e83de2-56c2-41d...|      1.0|
|dc3a53d3-640a-451...|      1.0|
|84d0e796-9ede-469...|      1.0|
|b10905c4-d771-49a...|      1.0|
|08116168-43ef-42f...|      1.0|
|c26eb4c4-85f2-4c1...|      1.0|
|c1bcc13a-b0aa-4a4...|      1.0|
+--------------------+---------+
only showing top 20 rows

+--------------------+-----+
|               breed|count|
+--------------------+-----+
|               MOGGY| 1191|
|           COCKERPOO|  737|
|      COCKER_SPANIEL|  717|
|  LABRADOR_RETRIEVER|  684|
|MEDIUM_MONGREL_(1...|  472|
|SMALL_MONGREL_(UP...|  325|
|      FRENCH_BULLDOG|  308|
|             CAVAPOO|  267|
|DACHSHUND_SHORT_H...|  236|
|LARGE_MONGREL_(MO...|  227|
|    GOLDEN_RETRIEVER|  210|
|     GERMAN_SHEPHERD|  209|
|              MOGGIE|  207|
|       BORDER_COLLIE|  194|
|STAFFORDSHIRE_BUL...|  185|
|  DOMESTIC_SHORTHAIR|  163|
|        JACK_RUSSELL|  136|
|         LABRADOODLE|  129|
|            SHIH_TZU|  124|
|    SPROCKER_SPANIEL|  122|
+--------------------+-----+
only showing top 20 rows

+--------------------+
|claimedPoliciesCount|
+--------------------+
|                3174|
+--------------------+

+--------------------+------------------+
|               breed|       avg(payout)|
+--------------------+------------------+
|    HUNGARIAN_VIZSLA|16407.064547212383|
|       FIELD_SPANIEL| 3605.911263527523|
|     ENGLISH_BULLDOG| 32232.93231838796|
|   EMPATRICE_MALTESE| 1702.177020289687|
|          POMERANIAN| 28918.17585373688|
|     NORFOLK_TERRIER| 2073.154075566022|
|   LANCASHIRE_HEELER| 2117.314270021428|
|   BLUE_MERLE_COLLIE| 3820.062303203209|
|PERRO_DE_PRESA_CA...| 7647.735329851478|
|       CAIRN_TERRIER|11688.329532304428|
|               TABBY| 41556.18304980803|
|TORTOISESHELL_SHO...| 5378.906213607468|
|      SPRINGERDOODLE|11555.184101867117|
|   SHETLAND_SHEEPDOG| 28912.73214936565|
|            SPANADOR|               0.0|
|          LEONBERGER|3287.4579161887737|
|              POMSKY|45434.391752635165|
|          MAINE_COON| 35537.18263910073|
|ENGLISH_SPRINGER_...| 19798.77515693985|
|               WHITE| 8503.510668469587|
+--------------------+------------------+
```

