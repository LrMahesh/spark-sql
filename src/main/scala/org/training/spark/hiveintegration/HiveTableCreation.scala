package org.training.spark.hiveintegration

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache

/**
 * Created by hduser on 7/9/15.
 */
object HiveTableCreation {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(args(0)).setAppName("hive metastore")
    val sc : SparkContext = new SparkContext(conf)
    //System.setProperty("hive.metastore.uris", "thrift://localhost:9083");

    System.setProperty("javax.jdo.option.ConnectionURL",
    "jdbc:mysql://localhost/hive_metastore?createDatabaseIfNotExist=true")
    System.setProperty("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
    System.setProperty("javax.jdo.option.ConnectionUserName", "root")
    System.setProperty("javax.jdo.option.ConnectionPassword", "training")
    System.setProperty("hive.metastore.warehouse.dir", "hdfs://localhost:54310/user/hive/warehouse")

    val hiveContext = new HiveContext(sc)

    //hiveContext.setConf("hive.metastore.uris", "thrift://localhost:9083");
    //hiveContext.sql("show tables").show()

    val sales = hiveContext.read.format("com.databricks.spark.csv").option("header", "true").load(args(1))
    //sales.write.mode("append").saveAsTable("learning.sales")
    //sales.write.insertInto("learning.sales") //by defalut append
    sales.registerTempTable("test")
    hiveContext.sql("use learning")
    hiveContext.sql("INSERT INTO SALES1 select * from test")
    //hiveContext.sql("SET hive.metastore.warehouse.dir=hdfs://localhost:54310/user/hive/warehouse");

    //hiveContext.sql("show tables").show()

   // hiveContext.sql("select count(*) from learning.sales").show()
    //hiveContext.sql("drop table sales1")
    //hiveContext.sql("create table sales1 as select * from test")
   hiveContext.sql("select * from sales1").show()
    hiveContext.sql("select count(*) from sales1").show()
    hiveContext.sql("show tables").show()

  }

}
