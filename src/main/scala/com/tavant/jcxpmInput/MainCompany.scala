package com.tavant.jcxpmInput

import java.time.LocalDate

import com.google.gson.{Gson, GsonBuilder}
import com.tavant.jcxpmInput.FlattenJson.{df1, df2}
import com.tavant.sparkEx.Employee
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode, udf, unix_timestamp}
import org.apache.spark.sql.functions._
import org.codehaus.jackson.map.ObjectMapper
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONObject


object MainCompany extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val ss = new java.text.SimpleDateFormat("ddMMyyyyhhmmss")

  val spark = SparkSession
    .builder().master("local")
    .appName("Spark Hive Example")
    .getOrCreate()
  import spark.implicits._



  val ds = spark.read.option("multiline", "true").json("src/main/scala/resources/input.json")
  val df1 = ds.select($"company.*")
  val df2 = df1.select( explode($"filial"))

  val filialArrayTrue= df2.select($"col.*").filter($"fl_matriz"===true).toDF().as[Filial_True]
  val filialArrayFalse =  df2.select($"col.*").filter($"fl_matriz"===false).drop(col("qsa")).toDF().as[Filial_False]

  filialArrayTrue.foreach(obj=>{
    val sortedData = sortBasedOnDate(obj.endereco_cad)
    println("Sorted Endereco===="+sortedData.dt_remessa+"===="+sortedData.de_endereco)
    println("======================================True QSA Info=======================================")
    obj.qsa.foreach(ob=>{
      ob.capital.foreach(vv=>{
        println("Capital Info======"+vv.vl_capital_soc);
      })
    })
  })

  println("======================================False Info=======================================")

  filialArrayFalse.foreach(obj=>{
    val sortedData = sortBasedOnDate(obj.endereco_cad)
    println("Sorted Endereco===="+sortedData.dt_remessa+"===="+sortedData.de_endereco+"Object position=")

  })


  def sortBasedOnDate(s: Array[endereco_cad]): endereco_cad = {
    var date1 =  ss.parse("00000000000000")
    var desc = "";
    for (elem <- s) {
      if(desc=="") {
        desc = elem.de_endereco;
        date1 = ss.parse(elem.dt_remessa)
      } else{
        if(ss.parse(elem.dt_remessa).after(date1)){
          desc = elem.de_endereco
          date1 = ss.parse(elem.dt_remessa)
        }
      }
    }
    endereco_cad(desc,date1.toString,"","")
  }

  var outputFalse :ListBuffer[Filial_False]= ListBuffer.empty
  outputFalse += Filial_False(" "," "," "," "," "," ",Array(endereco_cad("adsd","dsda","eww","dsd")),false,Array.empty);
  outputFalse += Filial_False("dd", " ", " ", " ", "ee", " ", Array.empty, true, Array.empty)

  val jsonStr = new ObjectMapper()
  println("======================================JAON STR===========================================")
  println(jsonStr.writeValueAsString(outputFalse.toString()))
  val gson = (new GsonBuilder()).setPrettyPrinting.create
  println(gson.toJson(outputFalse.toArray))
  println("=========================================================================================")
}



case class Filial_False(co_atos:String,
                  co_cidade: String,
                  co_junta_comer: String,
                  co_nat_jur: String,
                  co_seq_empresa: String,
                  co_tipo_arquiv: String,endereco_cad:Array[endereco_cad],fl_matriz: Boolean,
                  no_empresa_cad:Array[no_empresa_cad])

case class Filial_True(co_atos:String,
                  co_cidade: String,
                  co_junta_comer: String,
                  co_nat_jur: String,
                  co_seq_empresa: String,
                  co_tipo_arquiv: String,endereco_cad:Array[endereco_cad],fl_matriz: Boolean,
                  no_empresa_cad:Array[no_empresa_cad],qsa:Array[qsa])

case class endereco_cad(de_endereco: String,dt_remessa: String,no_arquivo: String,no_bairro: String)

case class no_empresa_cad(dt_remessa: String,no_arquivo: String,no_empresa: String)

case class qsa(capital:Array[capital],co_tipo_arquiv: String,dt_vinculacao_cad:Array[dt_vinculacao_cad]
               ,nu_documento: String,nu_pin_bin: String)

case class capital(vl_capital_soc: String)

case class dt_vinculacao_cad(dt_vinculacao: String)

case class endereco_cad1(de_endereco:String)
