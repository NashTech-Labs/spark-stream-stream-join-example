package com.knoldus.api

import com.knoldus.model.{GpsDetails, ImageDetails}
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.concurrent.duration.Duration

class StreamToStreamJoin(spark: SparkSession) {
  def aggregateOnWindow(imageStream: Dataset[ImageDetails], gpsDetails: Dataset[GpsDetails], window: Duration): Dataset[Map[ImageDetails, Array[GpsDetails]]] = ???
}
