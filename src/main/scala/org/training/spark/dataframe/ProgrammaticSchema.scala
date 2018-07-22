package org.training.spark.dataframe

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}


object ProgrammaticSchema {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("programmaticschema")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("INFO")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val salesRDD = sc.textFile(args(1))
    val schema =
      StructType(
        Array(StructField("transactionId", IntegerType, true),
          StructField("customerId", IntegerType, true),
          StructField("itemId", IntegerType, true),
          StructField("amountPaid", DoubleType, true))
      )

    val testDF = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "false").option("delimiter", "|").option("mode","DROPMALFORMMED").schema(schema).load(args(1))



    testDF.select("transactionId","amountPaid").show

    //to remove header there is no option to remove header i.e option("header","true")
    //to remove header
    val rowRDD = salesRDD.filter(line => !line.startsWith("transactionId"))
      .map(_.split("\\|")).map(p => Row(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toDouble))
    //ESCAPE PIPE WITH DOUBLE SLASH  "SPECIAL CHARECTORS SHOULD ESCAPE WITH DOUBLESLASH
  //here row is sql row
    //mostly used scenario
    val salesDF = sqlContext.createDataFrame(rowRDD, testDF.schema)
    //QsalesDF.write.save(args(2))
    salesDF.printSchema()
    salesDF.show()
  }

}
