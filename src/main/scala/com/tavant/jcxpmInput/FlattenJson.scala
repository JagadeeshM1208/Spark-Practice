package com.tavant.jcxpmInput

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, explode, explode_outer}
import org.apache.spark.sql.functions.udf
import org.codehaus.jackson.map.ObjectMapper
import scala.compat.Platform.EOL


object FlattenJson extends App {

  val spark = SparkSession
    .builder().master("local")
    .appName("Spark Hive Example")
    .getOrCreate()
  import spark.implicits._

  println("aaa"+EOL+"bbb")

  val ds = spark.read.option("multiline", "true").json("src/main/scala/resources/input.json")
  val df1 = ds.select($"company.*")
  val df2 = df1.select( explode($"filial"))
  val fl_matriz_false =  df2.select($"col.*").filter($"fl_matriz"===false).drop(col("qsa"))
  val fl_matriz_true= df2.select($"col.*").filter($"fl_matriz"===true)

  println("NU_PIN_BIN===========DATA")



  def flattenDataframe(df: DataFrame): DataFrame = {
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length
    for(i <- 0 to fields.length-1){
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_!=fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
          // val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName.*"))
          val explodedDf = df.selectExpr(fieldNamesAndExplode:_*)
          return flattenDataframe(explodedDf)
        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName +"."+childname)
          val newfieldNames = fieldNames.filter(_!= fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
          val explodedf = df.select(renamedcols:_*)
          return flattenDataframe(explodedf)
        case _ =>
      }
    }
    df
  }
  val flattendedJSON_true = flattenDataframe(fl_matriz_true)
  val flattendedJSON_false = flattenDataframe(fl_matriz_false)


  def branchJson(dataFrame: DataFrame):DataFrame={

    val df = dataFrame.select(
      $"co_cidade",
      $"co_junta_comer",
      $"co_nat_jur",
      $"co_seq_empresa",
      $"co_tipo_arquiv",
      $"fl_matriz",
      $"endereco_cad_de_endereco".as("de_endereco"),
      $"endereco_cad_no_bairro".as("no_bairro"),
      $"no_empresa_cad_dt_ultimo_arq".as("dt_ultimo_arq"),
      $"no_empresa_cad_no_empresa".as("no_empresa"))

    df

  }


  def MainCompany(dataFrame: DataFrame):DataFrame={

    val df = dataFrame.select(
      $"co_atos",
      $"co_cidade",
      $"co_junta_comer",
      $"co_nat_jur",
      $"co_seq_empresa",
      $"co_tipo_arquiv",
      $"fl_matriz",
      $"endereco_cad_de_endereco".as("de_endereco"),
      $"endereco_cad_no_bairro".as("no_bairro"),
      $"no_empresa_cad_dt_ultimo_arq".as("dt_ultimo_arq"),
      $"no_empresa_cad_no_empresa".as("no_empresa")



    )
    df

  }

  val upperUDF = udf { s: String =>
    s.split("_")(1)
     }

  def partnerJson(dataFrame: DataFrame):DataFrame={
    val df = dataFrame.select(
      cols =  $"qsa_capital_vl_capital_soc".as("vl_capital_soc"),
      $"qsa_co_tipo_arquiv".as("co_tipo_arquiv"),
      $"qsa_dt_vinculacao_cad_dt_vinculacao".as("dt_vinculacao"),
      $"qsa_nu_documento".as("nu_documento"),
      $"qsa_nu_pin_bin".as("nu_pin_bin"))

    .withColumn("flag",upperUDF(col("nu_pin_bin")))

    df
  }
  /*

  val branch= branchJson(flattendedJSON_false).toJSON
  val mainCompany= MainCompany(flattendedJSON_true).dropDuplicates().toJSON.show(false)
  val partners= partnerJson(flattendedJSON_true)
  //val compnay_Pin_Bin = ds.select($"company.nu_pin_bin").toString()

  partners.filter(col("flag")==="2").drop("flag").toJSON.show(false)

  val personDfWithSameCompanyPinBin= partners.filter(col("flag")==="2").filter(col("nu_pin_bin")===compnay_Pin_Bin).drop("flag").toJSON.show(false)
  val subpersonDf= partners.filter(col("flag")==="1").drop("flag").toJSON

  val mapJson:Map[String,Any]= Map("Branch" -> branch,"PartnerPerson"->personDfWithSameCompanyPinBin,"PartnerCompany"->subpersonDf);
  val mapper = new ObjectMapper()

  println("==================================================================")
  println(mapJson)
  println(mapper.writeValueAsString(mapJson.toString()));
  println("==================================================================")*/

  //val resultJson = mainCompany.union(branch).union(personDf).union(subpersonDf).show(false)


}
