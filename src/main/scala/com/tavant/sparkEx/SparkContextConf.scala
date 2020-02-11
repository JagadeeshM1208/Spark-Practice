package com.tavant.sparkEx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

trait SparkContextConf {

  var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  println(sc.version)

  lazy val sparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  val spark = SparkSession.builder()
    .master("local")
    .appName("QueryUtility")
    //.config(new SparkConf().setAll(ConfigLoader.load("src/main/resources/conf/conf.json")))
    .getOrCreate()

}
