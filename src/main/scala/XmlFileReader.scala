import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hduser on 6/8/18.
  */
object XmlFileReader extends App{

  val conf=new SparkConf().setAppName("master").setMaster("local")

  val sc = new SparkContext(conf)

  val sqlContext=new SQLContext(sc)

  val dataFrame=sqlContext.read.format("xml").option("rowTag","person").load("/home/hduser/Projects/spark-sql/src/main/resources/ages.xml")

  dataFrame.show()
  //dataFrame.select(col("name"))
}
