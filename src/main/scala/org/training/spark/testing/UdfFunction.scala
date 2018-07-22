package org.training.spark.testing

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

/**
 * Created by hduser on 29/7/16.
 */
object UdfFunction {

  def priceIncreaseDef (price: Double) = {
    price + 50.0
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("dataframe_udf")
    val sc : SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val salesDf = sqlContext.read.format("org.apache.spark.sql.json").load("/home/hduser/Projects/spark-sql/src/main/resources/sales.json")
    salesDf.registerTempTable("sales")

    val resultBefore = salesDf.select("customerId", "amountPaid").show()


   /*val priceIncreaseUdf = (price:Double) => {
     price + 50.0
   }

    val priceIncrease = udf(priceIncreaseUdf)*/

    // use existing definition is available in some other package as udf .if it is other package then import and create object it then by using object.methodname
      //_ is partial applied functions gives function object
      //udf takes only function object and ananyomus functions
    //val priceIncrease = udf(priceIncreaseDef _)

    sqlContext.udf.register("priceincrease", (price:Double) => {
      price + 50.0})

    import sqlContext.implicits._
    //val priceIncrease = udf((price:Double) => price + 50.0)
    //val results = salesDf.select($"customerId", priceIncrease(col("amountPaid")) as "IncreasedValue")
    val results1 = sqlContext.sql("select customerId,priceincrease(amountPaid) from sales")
    //println("dataframe DSL api usage")
    //results.show()
    println("sql query usage")
    results1.show()

  }
}
