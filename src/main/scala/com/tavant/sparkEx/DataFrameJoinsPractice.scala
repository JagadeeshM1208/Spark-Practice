package com.tavant.sparkEx

object DataFrameAndSQLJoinsPractice extends SparkContextConf {
  def main(args: Array[String]): Unit = {

    val moviesList = sqlContext.read.option("header",true)
      .csv("src/main/scala/resources/moviesList.csv")
      .toDF("realeseYear","movieName","collection","status","personId")

    val herosList = sqlContext.read.option("header",true)
        .csv("src/main/scala/resources/FilmStars.csv")
        .toDF("personId","personName")

    //Type of join to perform. Default `inner`. Must be one of:
    //  `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
    //  `right`, `right_outer`, `left_semi`, `left_anti`.

    herosList.join(moviesList,Seq("personId"),"outer")
      .select("movieName","realeseYear","status","personName")
      .filter("personId!='10'").show()

    herosList.join(moviesList,Seq("personId"),"inner")
      .select("movieName","realeseYear","status","personName")
      .filter("personId!='10'").show()


    herosList.join(moviesList,Seq("personId"),"left_outer")
      .select("movieName","realeseYear","status","personName")
      .filter("personId!='10'").show()

    herosList.join(moviesList,Seq("personId"),"right_outer")
      .select("movieName","realeseYear","status","personName")
      .filter("personId!='10'").show()

    herosList.join(moviesList,Seq("personId"),"full_outer")
      .select("movieName","realeseYear","status","personName")
      .filter("personId!='10'").show()

    herosList.join(moviesList,Seq("personId"),"left")
      .select("movieName","realeseYear","status","personName")
      .filter("personId!='10'").show()

    herosList.join(moviesList,Seq("personId"),"right")
      .select("movieName","realeseYear","status","personName")
      .filter("personId!='10'").show()


    moviesList.createOrReplaceTempView("Movies_List")
    herosList.createOrReplaceTempView("Hero_List")

    /*sqlContext.sql("select *from Movies_List").show(10)
    sqlContext.sql("select *from Hero_List").show(10)
*/
    sqlContext.sql("select hero.*,movie.* from Hero_List hero inner join Movies_List movie on movie.personId=hero.personId").show()


    sqlContext.sql(
      """
        |select hero.*,movie.* from Hero_List hero left outer join Movies_List movie
        |on movie.personId=hero.personId
      """.stripMargin).show

    sqlContext.sql(
      """
        |select hero.*,movie.* from Movies_List movie left outer join Hero_List hero
        |on movie.personId=hero.personId where hero.personId not in ('10','4','5') order by
        | hero.personId asc
      """.stripMargin).show





  }

}
