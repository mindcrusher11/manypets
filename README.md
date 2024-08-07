# NBC Universal

Data Operations on Sample Data

Download the code using the command below.

```
git clone https://github.com/mindcrusher11/manypets
```
I have used Spark and scala for this project to provide scalable, distributed processing.

Configuration settings are defined in the application.conf file in the resources folder

# Update f1 racing data path [f1racingDataPath] with the path of files in the file section.
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
  flightsDataPath = "/home/gaur/Downloads/Flight_Data_Assignment/Flight Data Assignment/flightData.csv"
  passengersDataPath = "/home/gaur/Downloads/Flight_Data_Assignment/Flight Data Assignment/passengers.csv"
  f1racingDataPath = "/home/gaur/f1racing.csv"
}
```
define master URL to run on local or cluster
update paths for claims data and policy data.

Once it is updated it can be run using 

sbt tool for Scala needs to be installed.

# Run NBCUniversal Tasks

```
sbt "runMain org.manypets.cam.service.NBCUniversalService"
```

Run test cases using 

```
sbt "testOnly org.manypets.cam.NBCUniversalTest -- -z Testing"
```

1 test case is defined which will display as passed.


Scala Docs for this project can be generated using the command below in the current project parent directory.
```
sbt doc
```
It will generate index.html file as "target/scala-2.11/api/index.html".

It can be opened in the browser.

The project can be run using the jar file

Steps to create jar file 


```
sbt package
```

Code can run in the cluster using the command below

```
spark-submit --class org.manypets.cam.service.NBCUniversalService  --master local[4] .target/scala-2.11/manypets_2.11-0.1.0-SNAPSHOT.jar 
```

** Logging is pending

Output will be like

```

Driver with Time


+------------------+----+
|            DRIVER|TIME|
+------------------+----+
|    Max Verstappen|1.31|
|    Lewis Hamilton|1.31|
|      Pierre Gasly|1.32|
|   Valtteri Bottas|1.31|
|      Lando Norris|1.31|
|   Charles Leclerc|1.31|
|      Sergio Perez|1.32|
|      Carlos Sainz|1.32|
|  Daniel Ricciardo|1.32|
|Antonio Giovinazzi|1.32|
|    Kimi Räikkönen|1.33|
|  Sebastian Vettel|1.33|
|      Lance Stroll|1.33|
|      Yuki Tsunoda|1.33|
|      Esteban Ocon|1.33|
|   Fernando Alonso|1.33|
|    George Russell|1.34|
|   Nicholas Latifi|1.34|
|   Mick Schumacher|1.34|
|    Nikita Mazepin|1.34|
+------------------+----+

Driver with average time and fastest time.

+---------------+-------+--------+
|         DRIVER|avgtime|fasttime|
+---------------+-------+--------+
|Charles Leclerc|   1.24|    1.13|
| Lewis Hamilton|   1.25|    1.11|
|Fernando Alonso|   1.26|    1.13|
+---------------+-------+--------+

```

# Quantexa

Data Operations on sample data

Download the code using command below.

```
git clone https://github.com/mindcrusher11/manypets
```
I have used spark and scala for this project to provide scalable, distributed processing.

Configuration settings are defined in application.conf file in resources foolder

# Update flights and passengers data path with path of files in file section.
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
  flightsDataPath = "/home/gaur/Downloads/Flight_Data_Assignment/Flight Data Assignment/flightData.csv"
  passengersDataPath = "/home/gaur/Downloads/Flight_Data_Assignment/Flight Data Assignment/passengers.csv"
  f1racingDataPath = "/home/gaur/f1racing.csv"

}
```
define master url to run on local or cluster
update paths for claims data and policy data.

Once it is updated it can be run using 

sbt tool for scala need to be installed.

# Run Quantexa Tasks

```
sbt "runMain org.manypets.cam.service.QuantexxaService"
```

Run test cases using 

```
sbt "testOnly org.manypets.cam.QuantexxaTests -- -z Testing"
```

1 test case is defined which will display as passed.


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
spark-submit --class org.manypets.cam.service.QuantexxaService  --master local[4] .target/scala-2.11/manypets_2.11-0.1.0-SNAPSHOT.jar 
```

** Logging is pending

Output will be like

```

Based on unique flightids if number of flights are according to flightIDs

+-----+---------------+
|month|numberOfFlights|
+-----+---------------+
|   12|             94|
|    1|             97|
|    6|             71|
|    3|             82|
|    5|             92|
|    9|             85|
|    4|             92|
|    8|             76|
|    7|             87|
|   10|             76|
|   11|             75|
|    2|             73|
+-----+---------------+



+-----+-----+
|month|count|
+-----+-----+
|   12| 9400|
|    1| 9700|
|    6| 7100|
|    3| 8200|
|    5| 9200|
|    9| 8500|
|    4| 9200|
|    8| 7600|
|    7| 8700|
|   10| 7600|
|   11| 7500|
|    2| 7300|
+-----+-----+

+-----------+---------+--------+-----+
|passengerId|firstName|lastName|count|
+-----------+---------+--------+-----+
|       2068|  Yolande|    Pete|   32|
|       4827|    Jaime|   Renay|   27|
|       1677|Katherina|Vasiliki|   27|
|       3173| Sunshine|   Scott|   26|
|       8961|    Ginny|   Clara|   26|
|       2857|      Son| Ginette|   25|
|       6084|     Cole|  Sharyl|   25|
|       8363|   Branda|  Kimiko|   25|
|       5867|    Luise| Raymond|   25|
|       5096|   Blythe|    Hyon|   25|
|        288|   Pamila|   Mavis|   25|
|        917|   Anisha|  Alaine|   25|
|        760|   Vernia|     Mui|   25|
|       2441|    Kayla|   Rufus|   24|
|       1240|Catherine|   Missy|   24|
|       5668|   Gladis| Earlene|   24|
|       1343|  Bennett|   Staci|   24|
|       3367|Priscilla|   Corie|   24|
|       7643|   Albina|    Joni|   23|
|       1673|   Jeanie|  Gladis|   23|
+-----------+---------+--------+-----+

+-----------+------------+
|passengerId|maxCountries|
+-----------+------------+
|        148|          10|
|        463|           4|
|        471|           8|
|        496|           2|
|        833|          14|
|       1088|           5|
|       1238|           7|
|       1342|          11|
|       1580|           7|
|       1591|          11|
|       1645|           7|
|       1829|          10|
|       1959|           2|
|       2122|           7|
|       2142|           4|
|       2366|           3|
|       2659|           4|
|       2866|           2|
|       3175|          10|
|       3749|           8|
+-----------+------------+

Updated after not in UK

+-----------+--------------------+------------+
|passengerId|           countries|maxCountries|
+-----------+--------------------+------------+
|        148|[co, ir, au, nl, ...|          11|
|        463|[tk, pk, ca, ir, ...|           5|
|        471|[tk, pk, il, ar, ...|           9|
|        496|    [tk, pk, pk, ir]|           3|
|        833|[ca, sg, bm, ch, ...|          14|
|       1088|[dk, tj, jo, jo, ...|           6|
|       1238|[cg, tj, tj, ir, ...|           8|
|       1342|[fr, nl, nl, sg, ...|          12|
|       1580|[se, tj, ir, jo, ...|           8|
|       1591|[se, tj, dk, ar, ...|          12|
|       1645|[us, pk, pk, tj, ...|           8|
|       1829|[ar, il, tk, co, ...|          11|
|       1959|[ca, cg, cg, cg, ...|           3|
|       2122|[be, be, cn, cg, ...|           7|
|       2142|[be, be, cn, cg, ...|           5|
|       2366|[co, ir, no, ir, ...|           4|
|       2659|[au, tj, uk, se, ...|           3|
|       2866|    [dk, ir, ir, iq]|           3|
|       3175|[fr, cn, at, pk, ...|          10|
|       3749|[ar, bm, tj, ch, ...|           9|
+-----------+--------------------+------------+

+-----------+-----------+---------------+-------------------+-------------------+
|passengerId|passengerId|flightsTogether|               from|                 to|
+-----------+-----------+---------------+-------------------+-------------------+
|       2759|       4316|             12|2017-02-07 00:00:00|2017-05-09 00:00:00|
|       4316|       4373|             12|2017-01-24 00:00:00|2017-05-09 00:00:00|
|        975|       1371|             12|2017-01-25 00:00:00|2017-04-22 00:00:00|
|       4607|       4674|             11|2017-02-02 00:00:00|2017-05-03 00:00:00|
|       2717|       4316|             11|2017-02-07 00:00:00|2017-05-03 00:00:00|
|       4025|       4053|             11|2017-01-20 00:00:00|2017-04-24 00:00:00|
|       5443|       5481|             11|2017-02-17 00:00:00|2017-05-09 00:00:00|
|       5443|       5491|             11|2017-02-17 00:00:00|2017-05-09 00:00:00|
|        606|       3169|             10|2017-01-13 00:00:00|2017-05-06 00:00:00|
|        975|       1337|             10|2017-01-25 00:00:00|2017-04-15 00:00:00|
|       4388|       4395|             10|2017-01-24 00:00:00|2017-04-02 00:00:00|
|       2759|       5481|             10|2017-02-22 00:00:00|2017-05-09 00:00:00|
|       2759|       5443|             10|2017-02-22 00:00:00|2017-05-09 00:00:00|
|        477|       3120|             10|2017-01-21 00:00:00|2017-05-09 00:00:00|
|       5440|       5491|             10|2017-02-17 00:00:00|2017-05-03 00:00:00|
|        975|       5423|             10|2017-02-22 00:00:00|2017-05-06 00:00:00|
|       1484|       2867|             10|2017-02-02 00:00:00|2017-04-15 00:00:00|
|        928|       1337|             10|2017-01-25 00:00:00|2017-04-15 00:00:00|
|       4388|       4399|             10|2017-01-24 00:00:00|2017-04-02 00:00:00|
|       5481|       5494|             10|2017-02-17 00:00:00|2017-05-03 00:00:00|
+-----------+-----------+---------------+-------------------+-------------------+

```


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

