package org.training.spark.datasource

import breeze.linalg.*
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
 * Created by hadoop on 15/2/15.
 */
object JsonFile {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("json file")
    val sc : SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    ///val sales = sqlContext.read.format("json").load(args(1))

    //to get columns we make use of columns
    /*val sales = sqlContext.read.json(args(1)).columns
     sales.foreach(println)*/
    val sales = sqlContext.read.json(args(1))
    /*val sele=sales.select("amountPaid")
    sele.show()
*/


   // amountPaid|customerId|itemId|transactionId|

     val df=sales.groupBy("customerId").agg(sum("amountPaid")).as("amountPaid")
         df.show()



    sales.show()
    /*val df1=sales.groupBy("customerId")
      .agg(sum("amountPaid").as("totalAmount"),
        collect_list("itemId").as("itemids"),
        countDistinct("itemId").as("distinct_itemids_cnt"))
    df1.show()*/


    sales.registerTempTable("sample")

    val df2=sqlContext.sql("select customerId,sum(amountPaid),collect_list(itemId),countDistinct(itemId) from sample groupBy(customerId)")
    df2.show()
    //select sum(amountPaid),collect_list(),countDistinct() groupBy(customerId)

    /*val testDF = sales.groupBy("customerId").sum("amountPaid")
    testDF.show

    val groupedDF = sales.groupBy("customerId").
      agg(sum("amountPaid").alias("totalAmt"),collect_list("itemId").as("all_items"))
    groupedDF.show()*/

   //Thread.sleep(1000000)
  }

}
