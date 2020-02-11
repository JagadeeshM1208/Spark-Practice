package com.tavant.sparkEx

import org.apache.spark.sql.{Dataset, SparkSession}


object CrimeAnalysisExample extends SparkContextConf {

  def main(args: Array[String]): Unit = {

    val crimeInfo = sc.textFile("src/main/scala/resources/SacramentocrimeJanuary2006.csv")


    val crimeDate = crimeInfo.map(col=> (col.split(",")(0),1)).reduceByKey(_+_)
    crimeDate.take(5).foreach(println)

    val crime459 = crimeInfo.map(col=> (col.split(",")(5).contains("459 PC"),1)).reduceByKey(_+_)
    crime459.take(5).foreach(println)

    val crime459count = crimeInfo.map(col=> (col.split(",")(5).startsWith("459 PC")))
    crime459count.take(5).foreach(println)

    val dataSet = crimeInfo.map(col=>{
      val dataSplit = col.split(",")
      if(dataSplit(2)=="3" && dataSplit(5).equals("459 PC  BURGLARY RESIDENCE"))
        //println(dataSplit(1))
        dataSplit
    })

    dataSet.foreach(println)


    //RDD Actions

    val storeInfo = sc.textFile("src/main/scala/resources/acme.csv")

    val dataFrame = storeInfo.map(crime=> (crime.split(",")))

    println(dataFrame.count())
    println(dataFrame.first().foreach(println))
    //println(dataFrame.map(oo=> oo(2)).reduce(_+_))

    println(dataFrame.take(2).foreach(obj=>println(obj(0))))
    println(dataFrame.takeSample(true,2).foreach(obj=>println(obj(0))))


    val rdd1 = sc.parallelize(List(("Hadoop PIG Hive"), ("Hive PIG PIG Hadoop"), ("Hadoop Hadoop Hadoop")))

    val rdd2 = rdd1.flatMap(x => x.split(" ")).map(x => (x,1))

    val rdd3 = rdd2.reduceByKey((x,y) => (x+y))

    rdd3.takeOrdered(3)(Ordering[Int].reverse.on(x=>x._2))

    rdd3.takeOrdered(3)(Ordering[Int].on(x=>x._2))



     val addressRepeating = crimeInfo.map(spl=>(spl.split(",")(1),1)).countByKey().take(10).foreach(println)

    import spark.implicits._

    val crimeInfo1 = sparkSession.read.csv("src/main/scala/resources/SacramentocrimeJanuary2006.csv").toDF(
      "cdatetime","address","district","beat","grid",
      "crimedescr","ucr_ncic_code","latitude","longitude"
    )

    crimeInfo1.createOrReplaceTempView("CrimeInfo")

    val crimeInfoAsc = sqlContext.sql("select *from CrimeInfo c where c.district='1'")
    val crimeRec:Dataset[CrimeInfo] = crimeInfoAsc.as[CrimeInfo]

    crimeRec.foreach(obj=> println("Crime--Date=="+obj.cdatetime+"  Distict=="+obj.district))
    println("Total Records==="+crimeInfoAsc.count())
  }

  case class CrimeInfo(cdatetime:String,address:String,district:String,beat:String,grid:String,
                       crimedescr:String,ucr_ncic_code:String,latitude:String,longitude:String)
}
