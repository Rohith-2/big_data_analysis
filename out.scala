scala> :load sampleReport.scala
Loading sampleReport.scala...
Loading loadData.scala...
DF: org.apache.spark.rdd.RDD[String] = airline.csv MapPartitionsRDD[1] at textFile at loadData.scala:24
header: String = ActualElapsedTime,AirTime,ArrDelay,ArrTime,CRSArrTime,CRSDepTime,CRSElapsedTime,CancellationCode,Cancelled,CarrierDelay,DayOfWeek,DayofMonth,DepDelay,DepTime,Dest,Distance,Diverted,FlightNum,LateAircraftDelay,Month,NASDelay,Origin,SecurityDelay,TailNum,TaxiIn,TaxiOut,UniqueCarrier,WeatherDelay,Year

df: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[2] at filter at loadData.scala:27
saClean: org.apache.spark.rdd.RDD[(String, String, String, String, String, String, String, String)] = MapPartitionsRDD[3] at map at loadData.scala:25
+--------+---------+---------+----------+----+------+----+------------+
|arrDelay|deptDelay|dayOfWeek|dayofMonth|year|origin|dest|weatherDelay|
+--------+---------+---------+----------+----+------+----+------------+
|-8      |4        |4        |10        |2002|DCA   |PIT |NA          |
|-11     |0        |4        |2         |1999|MCO   |MCI |NA          |
|15      |15       |5        |10        |1993|ATL   |CLT |NA          |
|-5      |-1       |4        |28        |1989|MEM   |BNA |NA          |
|2       |5        |1        |19        |2006|CVG   |CMH |0           |
|2       |0        |4        |2         |1997|MYR   |CLT |NA          |
|-3      |-4       |7        |20        |2008|DFW   |LAW |NA          |
|-19     |0        |4        |15        |1998|PVD   |ATL |NA          |
|-5      |0        |2        |16        |1998|SLC   |SEA |NA          |
|NA      |NA       |7        |10        |2005|DEN   |SLC |0           |
|-4      |1        |2        |3         |1991|DEN   |DFW |NA          |
|-8      |-2       |3        |13        |1991|STL   |CLE |NA          |
|10      |1        |4        |22        |1998|LGA   |MCO |NA          |
|-2      |-5       |1        |6         |2006|GNV   |ATL |0           |
|-5      |0        |6        |27        |1990|SAT   |DFW |NA          |
|-16     |5        |7        |19        |1995|STL   |PIT |NA          |
|43      |48       |5        |8         |1993|ATL   |DEN |NA          |
|40      |32       |4        |30        |1997|ORD   |BOS |NA          |
|-3      |-2       |2        |6         |2007|OGG   |HNL |0           |
|-9      |-10      |5        |4         |2005|TYS   |ATL |0           |
+--------+---------+---------+----------+----+------+----+------------+
only showing top 20 rows

 
Clean => saClean

Loading Year_AvgDelay.scala...
year_sorted: org.apache.spark.rdd.RDD[(String, String, String)] = MapPartitionsRDD[12] at map at Year_AvgDelay.scala:25
delay_filtered: org.apache.spark.rdd.RDD[(String, (Int, Int, Int))] = MapPartitionsRDD[15] at map at Year_AvgDelay.scala:25
delay_sum: org.apache.spark.rdd.RDD[(String, (Int, Int, Int))] = ShuffledRDD[16] at reduceByKey at Year_AvgDelay.scala:25
delay_avg: org.apache.spark.rdd.RDD[(String, Int, Int)] = MapPartitionsRDD[22] at sortBy at Year_AvgDelay.scala:25
+----+-----------+--------+
|Year|Avg_Arrival|Avg_Dept|
+----+-----------+--------+
|1987|          9|       7|
|1988|          6|       6|
|1989|          8|       8|
|1990|          6|       6|
|1991|          4|       5|
|1992|          4|       5|
|1993|          5|       6|
|1994|          5|       6|
|1995|          7|       8|
|1996|          9|       9|
|1997|          7|       8|
|1998|          7|       8|
|1999|          8|       9|
|2000|         10|      11|
|2001|          5|       8|
|2002|          3|       5|
|2003|          3|       5|
|2004|          6|       7|
|2005|          7|       8|
|2006|          8|      10|
+----+-----------+--------+
only showing top 20 rows

overall_delay_sum: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[26] at map at Year_AvgDelay.scala:25
+----+---------------------+
|Year|Overall Average Delay|
+----+---------------------+
|1987|                   16|
|1988|                   12|
|1989|                   16|
|1990|                   12|
|1991|                    9|
|1992|                    9|
|1993|                   11|
|1994|                   11|
|1995|                   15|
|1996|                   18|
|1997|                   15|
|1998|                   15|
|1999|                   17|
|2000|                   21|
|2001|                   13|
|2002|                    8|
|2003|                    8|
|2004|                   13|
|2005|                   15|
|2006|                   18|
+----+---------------------+
only showing top 20 rows

Loading Monthly_AvgDelay.scala...
year_sorted: org.apache.spark.rdd.RDD[(String, String, String)] = MapPartitionsRDD[35] at map at Monthly_AvgDelay.scala:25
delay_filtered: org.apache.spark.rdd.RDD[(Int, (Int, Int, Int))] = MapPartitionsRDD[38] at map at Monthly_AvgDelay.scala:25
delay_sum: org.apache.spark.rdd.RDD[(Int, (Int, Int, Int))] = ShuffledRDD[39] at reduceByKey at Monthly_AvgDelay.scala:25
delay_avg: org.apache.spark.rdd.RDD[(Int, Int, Int)] = MapPartitionsRDD[45] at sortBy at Monthly_AvgDelay.scala:25
+----+-----------+--------+
|Date|Avg_Arrival|Avg_Dept|
+----+-----------+--------+
|   1|          6|       7|
|   2|          6|       7|
|   3|          6|       7|
|   4|          6|       7|
|   5|          6|       7|
|   6|          6|       7|
|   7|          6|       7|
|   8|          6|       7|
|   9|          6|       7|
|  10|          7|       7|
|  11|          6|       7|
|  12|          7|       8|
|  13|          6|       7|
|  14|          7|       8|
|  15|          7|       8|
|  16|          7|       8|
|  17|          7|       8|
|  18|          7|       8|
|  19|          8|       9|
|  20|          7|       8|
+----+-----------+--------+
only showing top 20 rows

Loading Weekly_AvgDelay.scala...
year_sorted: org.apache.spark.rdd.RDD[(String, String, String)] = MapPartitionsRDD[54] at map at Weekly_AvgDelay.scala:25
delay_filtered: org.apache.spark.rdd.RDD[(Int, (Int, Int, Int))] = MapPartitionsRDD[57] at map at Weekly_AvgDelay.scala:25
delay_sum: org.apache.spark.rdd.RDD[(Int, (Int, Int, Int))] = ShuffledRDD[58] at reduceByKey at Weekly_AvgDelay.scala:25
delay_avg: org.apache.spark.rdd.RDD[(String, Int, Int)] = MapPartitionsRDD[65] at map at Weekly_AvgDelay.scala:25
+---------+-----------+--------+
|      Day|Avg_Arrival|Avg_Dept|
+---------+-----------+--------+
|   Monday|          6|       7|
|  Tuesday|          5|       6|
|Wednesday|          7|       7|
| Thursday|          8|       9|
|   Friday|          9|      10|
| Saturday|          4|       6|
|   Sunday|          6|       8|
+---------+-----------+--------+

Loading Origin_AvgDelay.scala...
origin_sorted: org.apache.spark.rdd.RDD[(String, String, String)] = MapPartitionsRDD[74] at map at Origin_AvgDelay.scala:25
delay_filtered: org.apache.spark.rdd.RDD[(String, (Int, Int, Int))] = MapPartitionsRDD[77] at map at Origin_AvgDelay.scala:25
delay_sum: org.apache.spark.rdd.RDD[(String, (Int, Int, Int))] = ShuffledRDD[78] at reduceByKey at Origin_AvgDelay.scala:25
delay_avg: org.apache.spark.rdd.RDD[(String, Int, Int)] = MapPartitionsRDD[84] at sortBy at Origin_AvgDelay.scala:25
+------+-----------+--------+
|Origin|Avg_Arrival|Avg_Dept|
+------+-----------+--------+
|   ABE|          4|       7|
|   ABI|          2|       3|
|   ABQ|          4|       6|
|   ABY|          8|       9|
|   ACK|         29|      27|
|   ACT|          0|       1|
|   ACV|          9|      11|
|   ACY|          4|       5|
|   ADK|         14|      20|
|   ADQ|          9|       8|
|   AEX|          7|       6|
|   AGS|          7|       7|
|   AKN|         10|       9|
|   ALB|          5|       6|
|   ALO|          3|       5|
|   AMA|          4|       5|
|   ANC|          7|       7|
|   ANI|          7|       4|
|   APF|          6|       7|
|   ASE|         11|      12|
+------+-----------+--------+
only showing top 20 rows

overall_delay_sum: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[88] at map at Origin_AvgDelay.scala:25
+------+---------------------+
|Origin|Overall Average Delay|
+------+---------------------+
|   ABE|                   11|
|   ABI|                    5|
|   ABQ|                   10|
|   ABY|                   17|
|   ACK|                   56|
|   ACT|                    1|
|   ACV|                   20|
|   ACY|                    9|
|   ADK|                   34|
|   ADQ|                   17|
|   AEX|                   13|
|   AGS|                   14|
|   AKN|                   19|
|   ALB|                   11|
|   ALO|                    8|
|   AMA|                    9|
|   ANC|                   14|
|   ANI|                   11|
|   APF|                   13|
|   ASE|                   23|
+------+---------------------+
only showing top 20 rows

Maxdelay_origin: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[92] at filter at Origin_AvgDelay.scala:25
+------+-------------------------------------+
|Origin|Overall Average Delay above Threshold|
+------+-------------------------------------+
|   BFF|                                  224|
|   CYS|                                  267|
|   FMN|                                  367|
|   OGD|                                  320|
|   PIR|                                   71|
+------+-------------------------------------+

Loading Dest_AvgDelay.scala...
dest_sorted: org.apache.spark.rdd.RDD[(String, String, String)] = MapPartitionsRDD[101] at map at Dest_AvgDelay.scala:25
delay_filtered: org.apache.spark.rdd.RDD[(String, (Int, Int, Int))] = MapPartitionsRDD[104] at map at Dest_AvgDelay.scala:25
delay_sum: org.apache.spark.rdd.RDD[(String, (Int, Int, Int))] = ShuffledRDD[105] at reduceByKey at Dest_AvgDelay.scala:25
delay_avg: org.apache.spark.rdd.RDD[(String, Int, Int)] = MapPartitionsRDD[111] at sortBy at Dest_AvgDelay.scala:25
+-----------+-----------+--------+
|Destination|Avg_Arrival|Avg_Dept|
+-----------+-----------+--------+
|        ABE|          7|       7|
|        ABI|          8|       9|
|        ABQ|          6|       8|
|        ABY|          9|      11|
|        ACK|         22|      18|
|        ACT|          2|       4|
|        ACV|          9|      10|
|        ACY|          5|       7|
|        ADK|         10|      12|
|        ADQ|          8|       7|
|        AEX|          7|       9|
|        AGS|          8|       8|
|        AKN|         10|      11|
|        ALB|          8|       9|
|        ALO|          4|       5|
|        AMA|          7|       8|
|        ANC|          9|       9|
|        ANI|         11|       6|
|        APF|          9|      10|
|        ASE|         10|      10|
+-----------+-----------+--------+
only showing top 20 rows

overall_delay_sum: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[115] at map at Dest_AvgDelay.scala:25
+-----------+---------------------+
|Destination|Overall Average Delay|
+-----------+---------------------+
|        ABE|                   14|
|        ABI|                   17|
|        ABQ|                   14|
|        ABY|                   20|
|        ACK|                   40|
|        ACT|                    6|
|        ACV|                   19|
|        ACY|                   12|
|        ADK|                   22|
|        ADQ|                   15|
|        AEX|                   16|
|        AGS|                   16|
|        AKN|                   21|
|        ALB|                   17|
|        ALO|                    9|
|        AMA|                   15|
|        ANC|                   18|
|        ANI|                   17|
|        APF|                   19|
|        ASE|                   20|
+-----------+---------------------+
only showing top 20 rows

Maxdelay_origin: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[119] at filter at Dest_AvgDelay.scala:25
+-----------+-------------------------------------+
|Destination|Overall Average Delay above Threshold|
+-----------+-------------------------------------+
+-----------+-------------------------------------+

Loading Waether_AvgDelay.scala...
weather_sorted: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[128] at map at Waether_AvgDelay.scala:25
delay_filtered: org.apache.spark.rdd.RDD[(String, (Int, Int))] = MapPartitionsRDD[130] at map at Waether_AvgDelay.scala:25
delay_sum: org.apache.spark.rdd.RDD[(String, (Int, Int))] = ShuffledRDD[131] at reduceByKey at Waether_AvgDelay.scala:25
delay_avg: org.apache.spark.rdd.RDD[(String, Float)] = MapPartitionsRDD[137] at sortBy at Waether_AvgDelay.scala:25
+----+-------------+
|Year|Average Delay|
+----+-------------+
|2003|    0.5207705|
|2004|   0.70658845|
|2005|   0.66013956|
|2006|     0.679627|
|2007|   0.77009034|
|2008|     3.039031|
+----+-------------+

weather_sorted: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[146] at map at Waether_AvgDelay.scala:25
delay_filtered: org.apache.spark.rdd.RDD[(Int, (Int, Int))] = MapPartitionsRDD[148] at map at Waether_AvgDelay.scala:25
delay_sum: org.apache.spark.rdd.RDD[(Int, (Int, Int))] = ShuffledRDD[149] at reduceByKey at Waether_AvgDelay.scala:25
delay_avg: org.apache.spark.rdd.RDD[(Int, Float)] = MapPartitionsRDD[155] at sortBy at Waether_AvgDelay.scala:25
+----+-------------+
|Date|Average Delay|
+----+-------------+
|   1|    0.7853695|
|   2|   0.73008907|
|   3|    0.5993609|
|   4|   0.84527975|
|   5|     0.801431|
|   6|   0.71509516|
|   7|    0.6873032|
|   8|   0.70720476|
|   9|     0.667396|
|  10|    0.7631065|
|  11|    0.7633907|
|  12|    0.7745317|
|  13|     0.883992|
|  14|    0.8603203|
|  15|    0.8889233|
|  16|    0.8394951|
|  17|   0.74784005|
|  18|   0.76382506|
|  19|    0.8337837|
|  20|   0.72553706|
+----+-------------+
only showing top 20 rows