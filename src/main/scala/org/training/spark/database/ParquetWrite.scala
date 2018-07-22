package org.training.spark.database

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Write into parquet file
 */
object ParquetWrite {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster(args(0)).setAppName("parquet write")
    sparkConf.set("spark.sql.parquet.compression.codec", "snappy")
    //to set at cluster level
    val sc : SparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    val salesDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))

    //to see number of partitions
    println(salesDf.rdd.getNumPartitions)


    //salesDf.write.format("orc").save(args(2))
    //to set compression at application level it's not suppourted by using option

    salesDf.write.mode(SaveMode.Overwrite).parquet(args(2))
   // option("spark.sql.parquet.compression.codec","snappy")
      //
    //salesDf.coalesce(1).write.mode(SaveMode.Append).orc(args(2))


  }

}
