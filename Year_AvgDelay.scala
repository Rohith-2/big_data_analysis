//:load samplingData.scala

val year_sorted = saClean.sortBy(_._5.toInt).map{case (arrDelay,deptDelay,dayOfWeek,dayofMonth,year,origin,dest,weatherDelay) => (year,arrDelay,deptDelay)};
val delay_filtered = year_sorted.filter(_._2 != "NA").filter(_._3 != "NA").map{case (a,b,c) => (a,(b.toInt,c.toInt,1))};
val delay_sum = delay_filtered.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2,x._3+y._3));
val delay_avg = delay_sum.map{case(a,b) => (a,b._1/b._3 ,b._2/b._3)}.sortBy(_._1);
delay_avg.toDF("Year","Avg_Arrival","Avg_Dept").show()

val overall_delay_sum = delay_avg.map{case(a,b,c)=>(a,b+c)};
overall_delay_sum.toDF("Year","Overall Average Delay").show()
