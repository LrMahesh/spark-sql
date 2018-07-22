package org.mahesh.learning


/**
  * Created by hduser on 6/14/18.
  * http://xinhstechblog.blogspot.com/2016/05/reading-json-nested-array-in-spark.html
  *
  *

object NestedJonn1 {
  def main(args: Array[String]): Unit = {

  val conf=new SparkConf().setAppName("NestedJosn").setMaster("local")
  val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)




    val file=sqlContext.read.json("src/main/resources/maheshinput/Nestedjson")
    file.show(truncate = false)



    +-------+----------------------------------+
    |name   |schools                           |
    +-------+----------------------------------+
    |Michael|[[stanford,2010], [berkeley,2012]]|
    |Andy   |[[ucsb,2011]]                     |
    +-------+----------------------------------+
    here schools is nested structure use explode to unnested

 val flatted=file.select($"name",explode($"schools").as("schools_flat"))
    flatted.show(truncate = false)

    +-------+---------------+
|name   |schools_flat   |
+-------+---------------+
|Michael|[stanford,2010]|
|Michael|[berkeley,2012]|
|Andy   |[ucsb,2011]    |
+-------+---------------+
    //now select

    flatted.select($"name",$"schools_flat.sname").show()
    +-------+--------+
    |   name|   sname|
    +-------+--------+
    |Michael|stanford|
    |Michael|berkeley|
    |   Andy|    ucsb|
    +-------+--------+
    flatted.printSchema()
    root
    |-- name: string (nullable = true)
    |-- schools_flat: struct (nullable = true)
    |    |-- sname: string (nullable = true)
    |    |-- year: long (nullable = true)
     val fileDF=flatted.select($"name",$"schools_flat.sname",$"schools_flat.year")
      fileDF.select($"year").show()

  }

}
*/
