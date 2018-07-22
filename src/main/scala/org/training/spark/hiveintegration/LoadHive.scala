package org.training.spark.hiveintegration

/**
  * Created by hduser on 6/12/18.
  */
import org.apache.spark._
import org.apache.spark.sql.hive.HiveContext


object LoadHive {
  def main(args: Array[String]) {
    /*if (args.length < 1) {
      println("Usage: [sparkmaster] [tablename]")
      exit(1)
    }*/
    //val master = args(0)
    //val tableName = args(0)
    val conf=new SparkConf().setAppName("LoadHive").setMaster("local")
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc)
    hiveCtx.sql("use spark")
    val input =hiveCtx.sql("select * from t1")
    val data = input.map(_.getInt(0))
    println(data.collect().toList)
  }
}
