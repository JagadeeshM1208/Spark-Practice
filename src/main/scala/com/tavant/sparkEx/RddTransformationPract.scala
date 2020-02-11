package com.tavant.sparkEx
 import org.apache.spark.{SparkConf,SparkContext}
 import org.apache.spark.sql._

object RddTransformationPract extends SparkContextConf{

  def main(args: Array[String]): Unit = {


      val l = (1 to 100).toList

      sc.parallelize(l).foreach(println)

    //Spark RDD Transformations Examples

    //map Examples
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
    object  mapTest{
      def main(args: Array[String]) = {
        val spark = SparkSession.builder.appName("mapExample").master("local").getOrCreate()
        val data = sc.textFile("resources/spark_test.txt")
        val mapFile = data.map(line => (line,line.length))
        mapFile.foreach(println)
      }
    }

    //flatMap Examples

    val data = sc.textFile("resources/strTxt.txt")
    val flatmapFile = data.flatMap(lines => lines.split(" "))
    //flatmapFile.foreach(println)

    //Filter Examples

    val data1 = sc.textFile("resources/strTxt.txt")
    val mapFile = data1.flatMap(lines => lines.split(" ")).filter(value => value=="spark")
    println(mapFile.count())


    //Union Examples

    val rdd1 = sc.parallelize(Seq((1,"jan",2016),(3,"nov",2014),(16,"feb",2014)))
    val rdd2 = sc.parallelize(Seq((5,"dec",2014),(17,"sep",2015)))
    val rdd3 = sc.parallelize(Seq((6,"dec",2011),(16,"may",2015)))
    val rddUnion = rdd1.union(rdd2).union(rdd3)
   // rddUnion.foreach(Println)

    //Intersection Examples

    val rdd44 = sc.parallelize(Seq((1,"jan",2016),(3,"nov",2014, (16,"feb",2014))))
    val rdd55 = sc.parallelize(Seq((5,"dec",2014),(1,"jan",2016)))
   /* val comman = rdd44.intersection(rdd55)
    println(comman)*/

    //Distinct Example
    val rdd6 = sc.parallelize(Seq((1,"jan",2016),(3,"nov",2014),(16,"feb",2014),(3,"nov",2014)))
    val result22 = rdd6.distinct()
    println(result22.collect().mkString(", "))

    //groupByKey() Example
    val data2 = sc.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
    val group11 = data2.groupByKey().collect()
    group11.foreach(println)

    //reduceByKey() example

    val words = Array("one","two","two","four","five","six","six","eight","nine","ten")
    val data3 = sc.parallelize(words).map(w => (w,1)).reduceByKey(_+_)
    data3.foreach(println)

    //sortByKey() Example

    val data4 = sc.parallelize(Seq(("maths",52), ("english",75), ("science",82), ("computer",65), ("maths",85)))
    val sorted = data4.sortByKey()
    sorted.foreach(println)

    //join() Example
    val data5 = sc.parallelize(Array(('A',1),('b',2),('c',3)))
    val data6 =sc.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
    val result = data5.join(data6)
    println(result.collect().mkString(","))

    //Coalesce() example
    val rdd9 = sc.parallelize(Array("jan","feb","mar","april","may","jun"),3)
    val result11 = rdd9.coalesce(2)
    result11.foreach(println)




    //Spark RDD Action Examples

    //Collect() example
    val data7 = sc.parallelize(Array(('A',1),('b',2),('c',3)))
    val data8 =sc.parallelize(Array(('A',4),('A',6),('b',7),('c',3),('c',8)))
    val result1 = data7.join(data8)
    println(result1.collect().mkString(","))


    //Take() example1

    val data9 = sc.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
    val group = data9.groupByKey().collect()
    val twoRec = result1.take(2)
    twoRec.foreach(println)


    //top() example

    val data10 = sc.textFile("resources/spark_test.txt")
    val mapFile1 = data10.map(line => (line,line.length))
    val res = mapFile1.top(3)
    res.foreach(println)


    //countByValue() example

    val data11 = sc.textFile("resources/spark_test.txt")
    val result2= data11.map(line => (line,line.length)).countByValue()
    result2.foreach(println)

    //Reduce() example

    val rdd11 = sc.parallelize(List(20,32,45,62,8,5))
    val sum = rdd11.reduce(_+_)
    println(sum)

    //Fold() example

    val rdd12 = sc.parallelize(List(("maths", 80),("science", 90)))
    val additionalMarks = ("extra", 4)
    val sum2 = rdd12.fold(additionalMarks){ (acc, marks) => val add = acc._2 + marks._2
      ("total", add)
    }
    println(sum2)





  }


}
