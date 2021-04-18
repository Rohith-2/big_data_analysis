val weather_sorted = saClean.sortBy(_._5.toInt).map{case (arrDelay,deptDelay,dayOfWeek,dayofMonth,year,origin,dest,weatherDelay) => (year,weatherDelay)}
val delay_filtered = weather_sorted.filter(_._2 != "NA").map{case (a,b) => (a,(b.toInt,1))}
val delay_sum = delay_filtered.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
val delay_avg = delay_sum.map{case(a,b)=>(a,b._1/b._2.toFloat)}.sortBy(_._1)
delay_avg.toDF("Year","Average Delay").show()

val weather_sorted = saClean.sortBy(_._5.toInt).map{case (arrDelay,deptDelay,dayOfWeek,dayofMonth,year,origin,dest,weatherDelay) => (dayofMonth,weatherDelay)}
val delay_filtered = weather_sorted.filter(_._2 != "NA").map{case (a,b) => (a.toInt,(b.toInt,1))}
val delay_sum = delay_filtered.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
val delay_avg = delay_sum.map{case(a,b)=>(a,b._1/b._2.toFloat)}.sortBy(_._1)
delay_avg.toDF("Date","Average Delay").show()