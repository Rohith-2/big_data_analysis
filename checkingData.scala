val DF = sc.textFile("airline.csv")
DF.take(1).foreach(println)
//ActualElapsedTime,AirTime,ArrDelay,ArrTime,CRSArrTime,CRSDepTime,CRSElapsedTime,CancellationCode,Cancelled,CarrierDelay,DayOfWeek,DayofMonth,DepDelay,DepTime,Dest,Distance,Diverted,FlightNum,LateAircraftDelay,Month,NASDelay,Origin,SecurityDelay,TailNum,TaxiIn,TaxiOut,UniqueCarrier,WeatherDelay,Year
val sample = sc.makeRDD(DF.takeSample(false, 1000, 69))
val saClean = sa.map{l =>
     val s0 = l.split(',') 
     val (arrDelay,deptDelay,dayOfWeek,dayofMonth,year,origin,dest,weatherDelay)=(s0(2),s0(12),s0(10),s0(11),s0(28),s0(21),s0(14),s0(27))
     (arrDelay,deptDelay,dayOfWeek,dayofMonth,year,origin,dest,weatherDelay) }
saClean.toDF("arrDelay","deptDelay","dayOfWeek","dayofMonth","year","origin","dest","weatherDelay").show(false)