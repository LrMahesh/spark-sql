package org.mahesh.learning.org.mahesh.SparkDefenetiveGuide

import org.apache.spark
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 6/14/18.
  */
object Chapter4 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Chapter4").setMaster("local")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    //val df=Range(1 ,100).toDF("number")
     //df.show()
  }

}
