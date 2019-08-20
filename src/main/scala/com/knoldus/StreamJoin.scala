package com.knoldus

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession



object StreamJoin extends App {

  val spark = SparkSession
    .builder
    .appName("ImageLocale")
    .getOrCreate()

  import spark.implicits._

  val images = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")


}
