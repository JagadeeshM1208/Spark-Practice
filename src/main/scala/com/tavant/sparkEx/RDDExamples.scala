package com.tavant.sparkEx

import org.apache.spark.sql.Dataset

object RDDExamples extends SparkContextConf {


  def main(args: Array[String]): Unit = {

    val moviesCsv = sc.textFile("src/main/scala/resources/moviesList.csv");

    // Create SimpleDateFormat object
    val ss = new java.text.SimpleDateFormat("ddMMyyyyhhmmss");

    println("Formated Date is======"+ss.parse("21012020102020"))

    // Get the two dates to be compared
    val d1 = ss.parse("2010-03-31");
    val d2 = ss.parse("2012-03-31");

    // Print the dates
    System.out.println("Date1 : " + ss.format(d1));
    System.out.println("Date2 : " + ss.format(d2));

    // Compare the dates using compareTo()
    if (d1.compareTo(d2) > 0) {

      // When Date d1 > Date d2
      System.out.println("Date1 is after Date2");
    }

    //println("Date Comparission====================================="+dob.compare(currentDateAsString))

    val sortDataByKey = moviesCsv.map(
      mlist=>(mlist.split(",")(1).toString,mlist.split(",")(0).toInt));

    println("RDD GroupByKey")
    val data2 = sc.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
    val group11 = data2.groupByKey().collect()
    group11.foreach(println)


    sortDataByKey.groupByKey().collect().foreach(println)
    sortDataByKey.take(3).foreach(println)
    sortDataByKey.sortByKey(true).foreach(println)

    //sortByKey(true or false)
    val sortedDataAdnSaveAsJson = data2.sortByKey(false)
    //sortedDataAdnSaveAsJson.saveAsTextFile("D:/sampleTxt123.txt")

    import sparkSession.implicits._
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.functions._
    val employeeJson = sqlContext.read.json("src/main/scala/resources/employees_singleLine.json")



    val employeeCasting = employeeJson.select(
      employeeJson.col("empno").cast("Integer"),
      employeeJson.col("ename").cast("String"),
      employeeJson.col("designation").cast("String"),
      employeeJson.col("manager").cast("Integer"),
      employeeJson.col("hire_date").cast("String"),
      employeeJson.col("sal").cast("Integer"),
      employeeJson.col("deptno").cast("Integer"))
    employeeJson.printSchema()
    employeeJson.show()

    employeeCasting.printSchema()
    employeeCasting.show()

   val emp : Dataset[EmployeeJsonAssign] =  employeeCasting.as[EmployeeJsonAssign]
    emp.show()
    emp.foreach(emp=>println(s"Employee Number = ${emp.empno} and Employee Name is ${emp.ename}"))


    val institutionData = sqlContext.read.json("src/main/scala/resources/institution.json")
    institutionData.createOrReplaceTempView("Institution_Info")



  }

}
case class EmployeeJsonAssign(empno:Int,ename:String,designation:String,manager:Int,hire_date:String,sal:Int
                              ,deptno:Int)


/*


  val data:String=""
  val latestDate:Long=0L;
  val fl_matriz_false =  df2.select($"col.*").filter($"fl_matriz"===false).drop(col("qsa"))
  val fl_matriz_true= df2.select($"col.*").filter($"fl_matriz"===true)

  import org.apache.spark.sql.functions._

  val sortDates = udf { arr: scala.collection.mutable.WrappedArray[org.apache.spark.sql.Row] =>
    arr.map { case org.apache.spark.sql.Row(i: Int, s: String) => (i, s) }.sortBy(_._2)
  }


  val result = fl_matriz_true.select( "endereco_cad.de_endereco","endereco_cad.dt_remessa",
    "endereco_cad.no_arquivo","endereco_cad.no_bairro")

  val d = result.select(result.col("de_endereco").cast("String"),
    result.col("dt_remessa").cast("String"),
    result.col("no_arquivo").cast("String"),
    result.col("no_bairro").cast("String"))

  val t = result.withColumn("new",kdf(col("de_endereco")))

    val kdf = udf(convertRowToJSON _)

 // result.createOrReplaceTempView("endereco_cad_Tbl")
  def convertRowToJSON(row: Row): String = {
//    val m = row.map(x=>{x.getValuesMap(x.schema.fieldNames)
   // JSONObject(m).toString()})
    "fd"
  }

  t.show(false)

  val empDataSet1:Dataset[endereco_cad]=result.as[endereco_cad]
  empDataSet1.show()


  empDataSet1.foreach(ob => println("Desc==="+ob.dt_remessa+ " date of it=========" +ob.de_endereco))

 // println(prefixStackoverflow1(Array(endereco_cad("jjfjdsjhgfsdfjsd","21012020102020","fdf","dsjsd"),
  //  endereco_cad("jjfjdsjhgfscdssdfdsdfjsd","21012020092020","zczxfdf","dsjsfsdfd"))))
    def prefixStackoverflow1(s: Array[endereco_cad]): endereco_cad = {
      val ss = new java.text.SimpleDateFormat("ddMMyyyyhhmmss")
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


  //prefixStackoverflow(Array("21012020102020","21012020092020"));
  def prefixStackoverflow(s: Array[String]): String = {
    val ss = new java.text.SimpleDateFormat("ddMMyyyyhhmmss")
    var date1 =  ss.parse("00000000000000")
    for (elem <- s) {
        if(ss.parse(elem).after(date1)) println(date1 = ss.parse(elem)) else println("else Latest Datec===="+date1)
    }
    date1.toString
  }
*/
/*


val data:String=""
val latestDate:Long=0L;
val fl_matriz_false =  df2.select($"col.*").filter($"fl_matriz"===false).drop(col("qsa"))
val fl_matriz_true= df2.select($"col.*").filter($"fl_matriz"===true)

import org.apache.spark.sql.functions._

val sortDates = udf { arr: scala.collection.mutable.WrappedArray[org.apache.spark.sql.Row] =>
  arr.map { case org.apache.spark.sql.Row(i: Int, s: String) => (i, s) }.sortBy(_._2)
}


val result = fl_matriz_true.select( "endereco_cad.de_endereco","endereco_cad.dt_remessa",
  "endereco_cad.no_arquivo","endereco_cad.no_bairro")

val d = result.select(result.col("de_endereco").cast("String"),
  result.col("dt_remessa").cast("String"),
  result.col("no_arquivo").cast("String"),
  result.col("no_bairro").cast("String"))

val t = result.withColumn("new",kdf(col("de_endereco")))

  val kdf = udf(convertRowToJSON _)

// result.createOrReplaceTempView("endereco_cad_Tbl")
def convertRowToJSON(row: Row): String = {
//    val m = row.map(x=>{x.getValuesMap(x.schema.fieldNames)
 // JSONObject(m).toString()})
  "fd"
}

t.show(false)

val empDataSet1:Dataset[endereco_cad]=result.as[endereco_cad]
empDataSet1.show()


empDataSet1.foreach(ob => println("Desc==="+ob.dt_remessa+ " date of it=========" +ob.de_endereco))

// println(prefixStackoverflow1(Array(endereco_cad("jjfjdsjhgfsdfjsd","21012020102020","fdf","dsjsd"),
//  endereco_cad("jjfjdsjhgfscdssdfdsdfjsd","21012020092020","zczxfdf","dsjsfsdfd"))))
  def prefixStackoverflow1(s: Array[endereco_cad]): endereco_cad = {
    val ss = new java.text.SimpleDateFormat("ddMMyyyyhhmmss")
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


//prefixStackoverflow(Array("21012020102020","21012020092020"));
def prefixStackoverflow(s: Array[String]): String = {
  val ss = new java.text.SimpleDateFormat("ddMMyyyyhhmmss")
  var date1 =  ss.parse("00000000000000")
  for (elem <- s) {
      if(ss.parse(elem).after(date1)) println(date1 = ss.parse(elem)) else println("else Latest Datec===="+date1)
  }
  date1.toString
}
*/