package org.training.spark.testing

import java.util.Properties

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 29/7/16.
 */
object PredicatePush1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("spark_jdbc")
    val sc  = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val properties:Properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","training")

    val where = "(select customerId from sales_test where amountPaid > 2500.0) tmp "

    /*here tmp is mandatory while getting data from rdbms if u remove it gives error
    i.e Exception in thread "main" com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException:
     Every derived table must have its own alias*/
    val jdbcDF = sqlContext.read.jdbc("jdbc:mysql://localhost:3306/ecommerce", where, properties)

    jdbcDF.printSchema()
    jdbcDF.show()
    jdbcDF.write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", ";").save("src/main/resources/csv_output")
  }
}
