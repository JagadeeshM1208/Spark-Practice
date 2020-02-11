package com.tavant.sparkEx


object DataFramesPractice extends SparkContextConf {

  def main(args: Array[String]): Unit = {



    val dfTags = sqlContext
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/scala/resources/ca-500.csv")
      .toDF("firstName", "lastName", "company_name", "address", "city",
        "province" , "postal", "phone1", "phone2", "email", "web")

    dfTags.show(3)

    //Print DataFrame schema

    dfTags.printSchema()

    //DataFrame Query: select columns from a dataframe

    dfTags.select("firstName","lastName").take(2).foreach(println)

    //DataFrame Query: filter by column value of a dataframe

    dfTags.filter("city == 'Aurora'").show(5)

    //DataFrame Query: count rows of a dataframe

    dfTags.filter("city == 'Aurora'").count()


    //DataFrame Query: SQL like query

    dfTags.filter("city like 'D%'").select("firstName","lastName","city").show(10)


    //DataFrame Query: Multiple filter chaining

    val multiFilter = dfTags.filter("province == 'ON'").filter("phone1 like '905%'")

    println("MultiFilter Count:"+multiFilter.count())
    multiFilter.show()


    //DataFrame Query: SQL IN clause
    println("in Dataframe Query" +dfTags.filter("city in('Ajax','Bradford')").count())


    println("GroupBy DataFrame Filter ==" +dfTags.groupBy("city").count().show())





    val dfTags1 = sqlContext
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/scala/resources/question_tags_10K.csv")
      .toDF("id", "tag")

    val dfQuestionsCSV = sqlContext
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dateFormat","yyyy-MM-dd HH:mm:ss")
      .csv("src/main/scala/resources/questions_10K.csv")
      .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")


    //make explicit making data type casting

    val dfQuestions = dfQuestionsCSV.select(
      dfQuestionsCSV.col("id").cast("integer"),
      dfQuestionsCSV.col("creation_date").cast("timestamp"),
      dfQuestionsCSV.col("closed_date").cast("timestamp"),
      dfQuestionsCSV.col("deletion_date").cast("date"),
      dfQuestionsCSV.col("score").cast("integer"),
      dfQuestionsCSV.col("owner_userid").cast("integer"),
      dfQuestionsCSV.col("answer_count").cast("integer")
    )

    val dfQuestionsSubset = dfQuestions.filter("score > 400 ").toDF()
    dfQuestionsSubset.show()

    println("Count of Owner details whose Score is above 400=="+dfQuestionsSubset.count())

    dfQuestions.select("id","owner_userid","score").distinct().show()

    //DataFrame Query: Join , it will fetch the matched records from the both file by id

    dfQuestionsSubset.join(dfTags1, "id").show(10)


    //DataFrame Query: Join and select columns


    dfQuestionsSubset.join(dfTags1, "id").
      select("id","creation_date","owner_userid","tag").show(20)



    //DataFrame Query: Join on explicit columns,
    // if the column name is different that time we can explicitly specify the column name as below

    dfQuestionsSubset
      .join(dfTags1, dfTags1("id") === dfQuestionsSubset("owner_userid"))
      .show(10)


    //Inner Join

    dfQuestionsSubset
      .join(dfTags1, Seq("id"), "inner")
      .show(10)


    //DataFrame Query: Left Outer Join

    dfQuestionsSubset
      .join(dfTags1, Seq("id"), "left_outer")
      .show(10)

    //DataFrame Query: Right Outer Join
    dfTags1
      .join(dfQuestionsSubset, Seq("id"), "right_outer")
      .show(10)


    //DataFrame Query: Distinct

    dfTags1
      .select("tag")
      .distinct()
      .show(10)



    //DataFrames for JSON File

    import sqlContext.implicits._
    val tagsDF22= sparkSession
      .read
      .option("multiLine", true)
      .option("inferSchema", true)
      .json("src/main/scala/resources/Employee.json")

    //val df = tagsDF22.select("*")

  }

}
