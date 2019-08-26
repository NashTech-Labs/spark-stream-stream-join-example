package com.knoldus

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import com.knoldus.api.StreamToStreamJoin
import com.knoldus.model.{GpsDetails, ImageDetails}
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession
import org.scalatest.WordSpec

class StreamJoinSpec extends WordSpec with EmbeddedKafka {

  implicit val serializer = new StringSerializer()

  def publishImagesToKafka = {
    1 to 10 foreach { recordNum =>
      val uuid = UUID.randomUUID().toString
      //TODO Add Serializer and dser
      val imageDetails = ImageDetails(uuid, uuid, recordNum.toString, Timestamp.from(Instant.ofEpochSecond(recordNum)))
      publishToKafka("camerasource", imageDetails.toString)
    }
  }

  def publishGPSDataToKafka = {
    1 to 10000 by 100 foreach { recordNum =>
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

      val sc = testSession.sparkContext

      sc.setLogLevel("WARN")

      val sut = new StreamToStreamJoin(testSession)

      withRunningKafka {
        val imagesDf = testSession
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:6001")
          .option("subscribe", "camerasource")
          .option("checkpointLocation", "/home/knoldus/")
          .load()

        val df = imagesDf.selectExpr("CAST(value AS STRING)").groupBy("value").count()


        val query =
        df.writeStream
            .outputMode("Update")
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:6001")
          .option("topic", "testoutput")
          .option("checkpointLocation", "/home/knoldus/")
          .start()

        publishImagesToKafka

        query.awaitTermination(5000)

       // publishGPSDataToKafka

        implicit val deserializer = new serialization.StringDeserializer()

        consumeFirstMessageFrom("testoutput").contains("cameraId")
      }

    }

  }

}
