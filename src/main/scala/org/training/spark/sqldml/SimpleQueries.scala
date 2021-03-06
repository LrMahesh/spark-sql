package org.training.spark.sqldml

import breeze.linalg.sum
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions._

/**
 * Running basic queries on schemaRDD
 */
object SimpleQueries {
  def main(args: Array[String]) {

    //Register the table
    val conf = new SparkConf().setMaster(args(0)).setAppName("simplequeries")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val hiveContext=new HiveContext(sc)

    val opt = Map("header" -> "true", "inferSchema" -> "true")
    val salesDf = sqlContext.read.format("com.databricks.spark.csv").options(opt).load(args(1))
    salesDf.registerTempTable("sales")
    sqlContext.cacheTable("sales")

    //Projection
    val itemids = sqlContext.sql("SELECT distinct itemId FROM sales")
    println("Projecting itemId column from sales")
    itemids.show()

    //Aggregation
    val totalSaleCount = sqlContext.sql("SELECT count(*) FROM sales")
    println("Counting total number of sales")
    totalSaleCount.show()

    //Group by
    val customerWiseCount = sqlContext.sql("SELECT customerId,count(*) FROM sales group by customerId")
    println("Customer wise sales count")
    customerWiseCount.show()

    val ItemWiseCount = sqlContext.sql("SELECT itemId,count(*) cnt FROM sales group by itemId order by cnt")
    println("Item wise sales count")
    ItemWiseCount.show()

    salesDf.show()
     }
}
