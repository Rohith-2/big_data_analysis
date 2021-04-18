val year_sorted = saClean.sortBy(_._5.toInt).map{case (arrDelay,deptDelay,dayofWeek,dayofMonth,year,origin,dest,weatherDelay) => (dayofWeek,arrDelay,deptDelay)}
val delay_filtered = year_sorted.filter(_._2 != "NA").filter(_._3 != "NA").map{case (a,b,c) => (a.toInt,(b.toInt,c.toInt,1))}
val delay_sum = delay_filtered.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2,x._3+y._3))
val delay_avg = delay_sum.map{case(a,b) => (a,b._1/b._3 ,b._2/b._3)}.sortBy(_._1).map{ case(a,b,c) =>
	var d = ""
	a match {
		case 1 => d = "Monday"
		case 2 => d = "Tuesday"
		case 3 => d = "Wednesday"
		case 4 => d = "Thursday"
		case 5 => d = "Friday"
		case 6 => d = "Saturday"
		case 7 => d = "Sunday"
	}
	(d,b,c)
}
delay_avg.toDF("Day","Avg_Arrival","Avg_Dept").show()