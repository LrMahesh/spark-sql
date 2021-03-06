package org.training.spark.testing

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hduser on 22/10/16.
 */
object MysqlParallel {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.memory", "2g")
    val sc: SparkContext = new SparkContext(args(0), "spark_jdbc", sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val mysqlOption = Map("url" -> "jdbc:mysql://localhost:3306/ecommerce",
      "dbtable" -> "sales",
      "user" -> "root",
      "password" -> "training",
      "fetchSize" -> "10",
      "partitionColumn" -> "customerId", "lowerBound" -> "1", "upperBound" -> "4", "numPartitions" -> "4")
  //im real time we should do preprocessing to get lower and upper bound values

    val jdbcDF = sqlContext.read
                           .format("org.apache.spark.sql.jdbc")
                           .options(mysqlOption)
                           .load()

    //jdbcDF.printSchema()

    jdbcDF.registerTempTable("sales")

    sqlContext.sql("SELECT transactionId, customerId, itemId, amountPaid from sales").write.mode("overwrite").json(args(1))
    Thread.sleep(10000)
    //sqlContext.sql("select Year, Month, count(*)  Total, Avg(DepDelay) Avg from ontime group by Year, Month").show()

  }
}
