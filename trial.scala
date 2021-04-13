:load samplingData.scala
saClean.toDF("arrDelay","deptDelay","dayOfWeek","dayofMonth","year","origin","dest","weatherDelay").show(false)