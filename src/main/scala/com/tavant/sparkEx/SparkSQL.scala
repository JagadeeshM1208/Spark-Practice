package com.tavant.sparkEx

import org.apache.spark.sql.{Dataset, SparkSession}

object SparkSQL extends SparkContextConf {

  def main(args: Array[String]): Unit = {
    val dfTags = sqlContext
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/scala/resources/question_tags_10K.csv")
      .toDF("id", "tag")


    dfTags.createOrReplaceTempView("Tags_Details")
    sparkSession.catalog.listTables().show()

    sparkSession
      .sql("select id, tag from Tags_Details limit 10")
      .show()

    sparkSession.sql("select distinct id from Tags_Details limit 5").show()

    sparkSession.sql("select id , tag from Tags_Details where id not in ('1','4') limit 10 ").show()


    sparkSession.sql("select count(*) from Tags_Details where tag in('html','css')").show()

    sparkSession.sql("select * from Tags_Details where tag like '%explorer%'").show()

    sparkSession.sql("select * from Tags_Details where tag like '%explorer%' and id not in('6','8')").show()


    sparkSession
      .sql(
        """select tag, count(*) as count
          |from Tags_Details group by tag""".stripMargin)
      .show(10)

    sparkSession
      .sql(
        """select tag, count(*) as count
          |from Tags_Details group by tag having count > 5""".stripMargin)
      .show(10)


    //Typed columns, filter and create temp table

    //Register User Defined Function (UDF),
    // let us say we have created a function it will take hike percentage and salary form employee table and
    //returns new salary including hike


    val employee = sqlContext
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/scala/resources/EmployeeHike.csv")
      .toDF("employeeId","employeeName","salary","Department","Designation","hikepercentage")

    //custom typecast csv data

      employee.select(employee.col("employeeId").cast("integer"),
      employee.col("employeeName").cast("String"),
      employee.col("salary").cast("integer"),
      employee.col("Department").cast("String"),
      employee.col("Designation").cast("String"),
      employee.col("hikepercentage").cast("integer"))

      employee.createOrReplaceTempView("Employee_Sal")

      sparkSession
      .udf
      .register("employee_hike", calculateHikePercentage _)

    sparkSession
      .sql("""select employeeId, employee_hike(salary,hikepercentage) as newSalary from Employee_Sal""".stripMargin)
      .show()

    val jsonConversion  = sparkSession.sql("select avg(salary) from Employee_Sal ").toJSON
    println(jsonConversion.show())




    //DataFrame Statistics Introduction , DataFrame Statics like avg, min,max,mean,sum,
    //dfQuestions
    //    .filter("id > 400 and id < 450")
    //    .filter("owner_userid is not null")
    //    .join(dfTags, dfQuestions.col("id").equalTo(dfTags("id")))
    //    .groupBy(dfQuestions.col("owner_userid"))
    //    .agg(avg("score"), max("answer_count"))
    //    .show()
    //Group by with statistics

    //DataFrame describe() function will give us the detail info of count, mean, standard deviation, min and max




   /*val empSel = employee.select(employee.col("employeeId").cast("integer"),
      employee.col("employeeName").cast("String"),
      employee.col("salary").cast("integer"),
      employee.col("Department").cast("String"),
      employee.col("Designation").cast("String"),
      employee.col("hikepercentage").cast("integer"))
    val correlationEx = empSel.stat.corr("salary","hikepercentage")
    println(s"Correlation Result of Employee=====$correlationEx")
    val coverianceEx = empSel.stat.cov("salary","hikepercentage")

    val frequentItems = empSel.stat.freqItems(List("hikepercentage"))

    empSel.stat.crosstab("employeeId","hikepercentage").show()


    // Create a fraction map where we are only interested:
    // - 50% of the rows that have answer_count = 5
    // - 10% of the rows that have answer_count = 10
    // - 100% of the rows that have answer_count = 20
    // Note also that fractions should be in the range [0, 1]
    val fractionKeyMap = Map(10 -> 1.0)

    // Stratified sample using the fractionKeyMap.
    empSel.filter("hikepercentage >5")
      .stat
      .sampleBy("hikepercentage", fractionKeyMap, 7L)
      .groupBy("hikepercentage")
      .count()
      .show()


    //Approximate Quantile Finding
    //0 = minimum
    //0.5 = median
    //1 = maximum
    val quantiles = empSel
      .stat
      .approxQuantile("hikepercentage", Array(0, 0.5, 1), 0.25)
    println(s"Qauntiles segments = ${quantiles.toSeq}")



*/






    val  people= sparkSession.read.json("src/main/scala/resources/Employee.json")
    people.printSchema()
    people.createOrReplaceTempView("people")


    println("All Companies")
   // sparkSession.sql("select * from people").show()




    //DataFrame Operations
    //converting csv file data into case class

    import sparkSession.implicits._
    val empDataSet:Dataset[Employee]=employee.as[Employee]
    empDataSet.foreach(emp=>println(s"Emp-Id=${emp.employeeId} and Salary is ==${emp.salary}"))


    //Create DataFrame from collection
    val dataList1 = Seq(1->"data",2->"ssss",3->"wsa")
    val dataList2 = Seq(4->"data",5->"ssss",3->"wsa")

    val dfList1 = dataList1.toDF("Id","Name")
    dfList1.show()
    val dfList2 = dataList1.toDF("Id","Name")

    //DataFrame union
    dfList2.union(dfList1).filter("id in('1','4')").show()
    dfList2.intersect(dfList1).show()

    //Append column to DataFrame using withColumn()
    import org.apache.spark.sql.functions._
    employee.withColumn("Team",split($"Department","_")).
      select($"employeeId",$"Department",$"Team".getItem(0).as("Work)," +
        $"Team".getItem(1).as("Under"))).drop("Team")
      .show()




  }

  def calculateHikePercentage(salary:Int,percentage:Int):Int ={
    (salary+(salary/100)*percentage)
  }
}
case class Employee(employeeId:Int,employeeName:String,salary:Int,Department:String
                        ,Designation:String,hikepercentage:Int)
