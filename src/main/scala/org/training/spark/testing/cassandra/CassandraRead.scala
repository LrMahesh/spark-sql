package org.training.spark.testing.cassandra

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 16/5/16.
 */
object CassandraRead {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()

    val master =  args(0)
    sparkConf.setMaster(master)
    sparkConf.setAppName("Cassandra Read")
 //to know where cassandra is running to spark .cassandra by default run at port 9042
    //if cassandra not run at this port then we should specify port number
    sparkConf.set("spark.cassandra.connection.host", "localhost")

    //sparkConf.set("spark.cassandra.connection.keep_alive_ms", "40000")

    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val cassandraOptions = Map( "table" -> "sales", "keyspace" -> "ecommerce")
    val cityStatsDF = sqlContext.read.format("org.apache.spark.sql.cassandra")
      .options(cassandraOptions)
      .load()

    cityStatsDF.show()

    cityStatsDF.registerTempTable("sales")

    val citydata = sqlContext.sql("Select * from sales")
    citydata.show()
    //table should be there in cassandra before loding data into table because cassadndra have primarykey it can't
    //  select which column is primary key but in rdbms we can write into rdbms without table it can create
    //on the fly ,without primary key also we cancreate in rdbms.

    val cassandraOptions1 = Map( "table" -> "test", "keyspace" -> "ecommerce")
    citydata.write.format("org.apache.spark.sql.cassandra")
      .options(cassandraOptions1).mode(SaveMode.Append).save()


  }
}
