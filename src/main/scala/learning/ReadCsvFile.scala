package learning

/**
  * Created by hduser on 1/7/18.
  */
import org.apache.spark._
import org.apache.spark.sql.SQLContext
object ReadCsvFile extends App {

  val conf = new SparkConf().setMaster("local")
    .setAppName("reading csv file")

  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  val csvDF = sqlContext.read.
    format("com.databricks.spark.csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/sales.csv")

  csvDF.printSchema()
  csvDF.show

  println(csvDF.count)

  csvDF.registerTempTable("sales_table")
  val whereDF = sqlContext.sql("select * from sales_table where amountPaid > 1000")
  whereDF.show()

  val df1 = csvDF.where("amountPaid > 1000").
    selectExpr("transactionId as tid", "customerId as cid")
  df1.show()

  val df2 = csvDF.filter(col("amountPaid") > 1000.0).
    select(col("transactionId").alias("tid"), col("customerId").alias("cid"))
  df2.show
}