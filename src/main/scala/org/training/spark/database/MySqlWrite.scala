package org.training.spark.database

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import java.io.{FileNotFoundException, IOException}

import scala.collection.immutable.Map
import scala.collection.JavaConverters._



/**
 * Created by hduser on 31/8/15.
 */
object MySqlWrite   {
  def main(args: Array[String]) {

    try {

      val conf = new SparkConf().setAppName("spark_jdbc_write").setMaster("local")
      val sc: SparkContext = new SparkContext(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)

      val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/sales.csv")

      //val option = Map("url"->"jdbc:mysql://localhost:3306/ecommerce","dbtable"->"sales")

      val properties = new Properties()
      properties.setProperty("user", "root")
      properties.setProperty("password", "training")

      salesDf.printSchema()
      salesDf.show()

      salesDf.withColumn("test", lit(4.5)).write.mode("append").jdbc("jdbc:mysql://localhost:3306/ecommerce", "sales_training1", properties)
      //salesDf.withColumn
    }
    catch {
      case e : IOException => { e.printStackTrace(); e.getMessage }
      case e : FileNotFoundException => { e.printStackTrace(); e.getMessage }
    }
    finally {

    }

  }
}
