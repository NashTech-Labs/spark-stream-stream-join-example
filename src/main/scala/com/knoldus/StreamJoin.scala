package com.knoldus

import org.apache.spark.sql.SparkSession



object StreamJoin {

  val spark = SparkSession
    .builder
    .appName("ImageLocale")
    .getOrCreate()

  import spark.implicits._

  val images = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9193")
    .option("subscribe", "camerasource")
    .option("includeTimestamp", value = true)


}
