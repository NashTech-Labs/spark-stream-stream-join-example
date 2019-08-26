package com.knoldus.model

import java.sql.Timestamp


case class ImageDetails(cameraId: String, imageId: String, imageUrl: String, timeStamp: Timestamp) {
  override def toString: String = s"""{"cameraId":"$cameraId", "imageId":"$imageId", "imageUrl":"$imageUrl", "timestamp":"$timeStamp"}"""
}


case class GpsDetails(cameraId: String, gpsId: String, lat: Double , lon: Double, timestamp: Timestamp) {
  override def toString: String = s"""{"cameraId":"$cameraId", "gpsId":"$gpsId", "lat":"$lat", "timestamp":"$timestamp"}"""
}
