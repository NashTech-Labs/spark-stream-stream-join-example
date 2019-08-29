package com.knoldus.api

import java.sql.Timestamp

import com.knoldus.model.{GpsDetails, ImageDetails}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class StreamToStreamJoin(spark: SparkSession) {
  def aggregateOnWindow(imageStream: Dataset[ImageDetails], gpsDetails: Dataset[GpsDetails], window: Long): Dataset[Row] = {
    spark.udf.register("time_in_milliseconds", (str: String) => Timestamp.valueOf(str).getTime)
    imageStream.join(
      gpsDetails,
      expr(
        s"""
            cameraId = gpscameraId AND
            abs(time_in_milliseconds(timestamp) - time_in_milliseconds(gpsTimestamp)) <= $window
            """.stripMargin)
    )
  }
}
