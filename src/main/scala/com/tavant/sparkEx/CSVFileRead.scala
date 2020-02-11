package com.tavant.sparkEx
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object CSVFileRead extends SparkContextConf{


  case class Employee(firstName:String, lastName:String, company_name:String, address:String, city:String,
                      province:String , postal:String, phone1:String, phone2:String, email:String, web:String)

  def main(args : Array[String]): Unit = {

    import sqlContext.implicits._

    // Basic map example in scala
    val x = sc.parallelize(List("spark", "rdd", "example",  "sample", "example"), 3)
    val y = x.map(x => (x, 1)).collect
    println(y)

    val y1 = x.map((_, 1))
    y1.collect

    val y2 = x.map(x => (x, x.length))
    y2.collect


    //String Manipulations


    val str = "dshhs jksdfsd bjkjf jkkhksd  jjkfsdk njkkfshd njkhfs";
    str.substring(0,10)
    str.length
    str.isEmpty
    str.replace("dshhs","wwwwww")
    val str1 = str.concat("12222222222222")

    str.toUpperCase()
    str.toLowerCase()



    val csvRDD = sc.textFile("D:/ca-500.csv")
    //println(csvRDD.foreach(println)
    val empRdd = csvRDD.map {
      line =>
        val col = line.split(",")
        Employee(col(0), col(1), col(2), col(3), col(4), col(5), col(6),col(7),col(8),col(9),col(10))
    }
    val empDF = empRdd.toDF()
    empDF.show()

    //take will pick the data as per the order
    val empData = csvRDD.map(lines=>(lines.split(",")(0),lines))
    empData.take(5).foreach(println)

    //TakeSample will pick the Data randomly
    empData.takeSample(true,5).foreach(println)

    //take all firstNames
    val allFirstNames = csvRDD.map(fName=>(fName.split(",")(0))).collect().toList
    allFirstNames.take(10).foreach(println)

    //pick all the employee whose city is Delhi

    val city = csvRDD.map(empData=>(empData.split(",")(4).equals("Delhi")))

    println(city.count())


    val firstNames = csvRDD.map(fName=>(fName.split(",")(0),1)).reduceByKey(_+_)

    firstNames.take(10).foreach(println)


    val rddCity = csvRDD.filter(empData=>(empData.split(",")(4).equals("Aurora")))

    println(rddCity.count())

    /* case class TavntEmployee(empno:String, ename:String, designation:String, manager:String, hire_date:String,
                              sal:String , deptno:String)


     val jsonRDD = sqlContext.read.json("src/main/scala/resources/Employee.json")
     //jsonRDD.show()*/


  }


}
