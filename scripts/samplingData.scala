val DF = sc.textFile("airline.csv")
val sample = sc.makeRDD(DF.takeSample(false, 10000, 69420))
val saClean = sample.map{l =>
     val s0 = l.split(',') 
     val (arrDelay,deptDelay,dayOfWeek,dayofMonth,year,origin,dest,weatherDelay)=(s0(2),s0(12),s0(10),s0(11),s0(28),s0(21),s0(14),s0(27))
     (arrDelay,deptDelay,dayOfWeek,dayofMonth,year,origin,dest,weatherDelay) }

saClean.toDF("arrDelay","deptDelay","dayOfWeek","dayofMonth","year","origin","dest","weatherDelay").show(false)
println(" ")
println("Clean and Sampled => saClean")


