package com.knoldus.model

import java.sql.Timestamp
import java.util.UUID


case class ImageDetails(cameraId: UUID, imageId: UUID, imageUrl: String, timeStamp: Timestamp)
case class GpsDetails(cameraId: UUID, gpsId: UUID, lat: Double , lon: Double, timestamp: Timestamp)
