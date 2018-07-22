package org.training.spark.hiveintegration

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by hduser on 6/13/18.
  */
object Mahesh1 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("hiveintegration").setMaster("local")
    val sc=new SparkContext(conf)
    val hc=new HiveContext(sc)
   // hc.sql("")
    hc.sql("show tables").show()


  }

}
