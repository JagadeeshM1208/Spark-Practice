package com.tavant.jcxpmInput

import com.tavant.jcxpmInput.MainCompany.df1
import com.tavant.sparkEx.SparkContextConf
import com.tavant.sparkEx.SparkSQL.sparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import sparkSession.sqlContext.implicits._
import org.apache.spark.sql.functions.explode

import scala.collection.mutable.ListBuffer

object CompnayInfoRead extends SparkContextConf with App{

  import org.apache.spark.sql.functions._

  Logger.getLogger("org").setLevel(Level.ERROR)

  val ss = new java.text.SimpleDateFormat("ddMMyyyyhhmmss")

  val spark1 = SparkSession
    .builder().master("local")
    .appName("Spark Hive Example")
    .getOrCreate()
  import spark.implicits._



  val ds = sparkSession.read.option("multiline", "true").json("src/main/scala/resources/input.json").toDF()
  val df1 = ds.select($"company.*").toDF()
  df1.describe().show(false)
  // val df2 = df1.select( explode($"filial"))

  /*df1.select(explode($"filial.endereco_cad")).select("col.*")
    .withColumn("date",to_timestamp(col("dt_remessa"),"ddMMyyyyhhmmss"))
    .sort(desc("date"))
    .select("de_endereco","dt_remessa").show(false)*/

  val chUdf = udf((x:String,y:String)=>{
     x.length()
  })

  def lengthCheck(s:String):Int = {
    s.length
  }

  sparkSession.udf.register("lengthCheck1",lengthCheck _);

  println(lengthCheck("qwqwqwqwq"))



}
