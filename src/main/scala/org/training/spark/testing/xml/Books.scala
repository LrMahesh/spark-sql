package org.training.spark.testing.xml

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

import org.apache.spark.sql.hive.HiveContext

/**
 * Created by hduser on 13/8/16.
 */
object Books {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(args(0), "xml file", sparkConf)
    val sqlContext = new HiveContext(sc)
    //insted of using multipule option  method we make use of options method to pass n number of properties
    val xmlOptions = Map("rowTag" -> "book")
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .options(xmlOptions)
      //options will take map as input i.e key and value
      .load(args(1))
    df.printSchema()


    val flatDf=df.withColumn("publishDate",explode(df("publish_date")))
    df.show()
    flatDf.registerTempTable("sample")
    /*val versionDf=sqlContext.sql("select *, row_number over(partition by title order by publish_date) as version from sample")
    versionDf.show()*/

    val versionDf=flatDf.withColumn("version", row_number().over(Window.partitionBy("title").orderBy("publishdate")))
    val versionDf1=flatDf.withColumn("version", row_number().over(Window.partitionBy("title").orderBy(desc("publishdate"))))
    versionDf1.show()



   /* val df1 = df.withColumn("publishdate", explode(df("publish_date")))
    df1.show()*/
    //df.show(truncate = false) //passing value to default argument using named argument
  }
}
