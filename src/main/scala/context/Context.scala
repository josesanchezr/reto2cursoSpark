package context

import org.apache.spark.sql.SparkSession

object Context {
  val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("Reto 2 Curso Spark")
    .master("local[1]")
    .getOrCreate()
}
