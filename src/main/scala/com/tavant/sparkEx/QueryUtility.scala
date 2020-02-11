package com.tavant.sparkEx

object QueryUtility extends SparkContextConf {
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org").setLevel(Level.OFF)
    println("key1 config value is " + spark.conf.get("key1"))
    val companyDF = spark.read.json(spark.conf.get("Employee.json"))
    companyDF.printSchema()
    companyDF.createOrReplaceTempView("employee")
    println("All Companies")
    spark.sql("select * from employee").show()
  }
}
