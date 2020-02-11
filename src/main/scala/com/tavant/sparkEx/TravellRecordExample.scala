package com.tavant.sparkEx

object TravellRecordExample extends SparkContextConf {

  def main(args: Array[String]): Unit = {
      val travelRecords = sc.textFile("src/main/scala/resources/TravelRec.csv")
      travelRecords.map(rec=> (rec.split(",")(0).toInt,(1,rec.split(",")(2).toInt))).
        reduceByKey((v1,v2) => (v1._1+v2._1,v1._2+v2._2)).take(4).foreach(println)


    //Hash Partitioning How internally work
    println("Partition Node number"+(401.hashCode%4))

    //Range Partitioning Internal flow


  }
}
