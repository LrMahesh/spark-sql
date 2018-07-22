name := "spark-sql"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.1"

libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"

libraryDependencies += "com.databricks" %% "spark-xml" % "0.3.3"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.6"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-M3"

unmanagedJars in Compile += file("lib/hive-jdbc-1.2.1.spark2.jar")

unmanagedJars in Compile += file("lib/hive-metastore-1.2.1.spark2.jar")
    