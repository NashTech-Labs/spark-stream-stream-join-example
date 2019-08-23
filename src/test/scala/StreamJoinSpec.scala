package com.knoldus

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import com.knoldus.api.StreamToStreamJoin
import com.knoldus.model.{GpsDetails, ImageDetails}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession
import org.scalatest.WordSpec

class StreamJoinSpec extends WordSpec with EmbeddedKafka {

  implicit val serializer = new StringSerializer()

  def publishImagesToKafka = {
    1 to 100 foreach { recordNum =>
      val uuid = UUID.randomUUID().toString
      //TODO Add Serializer and dser
      val imageDetails = ImageDetails(uuid, uuid, recordNum.toString, Timestamp.from(Instant.ofEpochSecond(recordNum)))
      publishToKafka("camerasource", imageDetails.toString)
    }
  }

  def publishGPSDataToKafka = {
    1 to 100000 by 100 foreach { recordNum =>
      val uuid = UUID.randomUUID().toString
      //TODO Add Serializer and dser
      val gpsDetails = GpsDetails(uuid, uuid, recordNum, recordNum, Timestamp.from(Instant.ofEpochMilli(recordNum)))
      publishToKafka("gpssource", gpsDetails.toString)
    }
  }

  "StreamToStreamJoin" should {

    "aggregateOnWindow for a duration of 10 seconds" in {

      val testSession =
        SparkSession
          .builder()
          .appName("StreamToStreamJoinTest")
          .master("local")
          .getOrCreate()

      val sut = new StreamToStreamJoin(testSession)

      withRunningKafka {

        implicit val config = EmbeddedKafkaConfig(kafkaPort = 9193)

        publishImagesToKafka
        publishGPSDataToKafka

        val imagesDf = testSession
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9193")
          .option("subscribe", "camerasource")
          .option("includeTimestamp", value = true)
          .load()

        imagesDf.selectExpr("")
      }

    }

  }

}
