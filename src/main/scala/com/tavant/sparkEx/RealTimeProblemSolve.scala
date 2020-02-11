package com.tavant.sparkEx

object RealTimeProblemSolve extends SparkContextConf {

  def main(args: Array[String]): Unit = {
    /*
        Problem Statement::
            1.Get the daily revenue by product considering completed and closed items
            2.Data need to be sorted by ascending order by date adn the descending order by revenue
              completed for each product for each day
            3.Data Should be delimited by ',' in the below order
            order_date,daily_revenue_per_product,product_name
     */


    //read order and order items
    //filter completed and closed orders
    //convert both completed and closed orders into key value pair
    //join the two data sets
    //get the daily revenue per product id
    //load the product from local file system and convert into RDD
    //join daily revenue per product id with products to get the daily revenue per product (by name)
    //sort the data b date in ascending order and daily revenue by descending order
  }
}
