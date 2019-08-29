package com.knoldus

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import com.knoldus.api.StreamToStreamJoin
import com.knoldus.model.{GpsDetails, ImageDetails}
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.scalatest.WordSpec
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import org.apache.spark.sql.functions.{col, udf}

class StreamJoinSpec extends WordSpec with EmbeddedKafka {

  implicit val serializer = new StringSerializer()

  def publishImagesToKafka(start: Int, end: Int) = {
    start to end foreach { recordNum =>
      //TODO Add Serializer and dser
      val imageDetails = ImageDetails(recordNum.toString, recordNum.toString, recordNum.toString, Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis())))
      publishToKafka("camerasource", imageDetails.toString)
      Thread.sleep(1000)
    }
  }

  def publishGPSDataToKafka(start: Int, end: Int) = {
    start to end foreach { recordNum =>
      val uuid = UUID.randomUUID().toString
      //TODO Add Serializer and dser
      val gpsDetails = GpsDetails((recordNum%10).toString, recordNum.toString, recordNum.toDouble, recordNum.toDouble, Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis())))
      publishToKafka("gpssource", gpsDetails.toString)
      Thread.sleep(100)
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
          .option("startingOffsets", "earliest")
          .option("checkpointLocation", "/home/knoldus/")
          .load()

        val gpssDf = testSession
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:6001")
          .option("subscribe", "gpssource")
          .option("checkpointLocation", "/home/knoldus/")
          .load()

        implicit val imageEncoder: Encoder[ImageDetails] = Encoders.product[ImageDetails]
        implicit val gpsEncoder: Encoder[GpsDetails] = Encoders.product[GpsDetails]

        val imagesSchema = StructType(
          Seq(
            StructField("cameraId", StringType),
            StructField("imageId", StringType),
            StructField("imageUrl", StringType),
            StructField("timestamp", TimestampType)
          )
        )

        val gpsSchema = StructType(
          Seq(
            StructField("gpscameraId", StringType),
            StructField("gpsId", StringType),
            StructField("lat", DoubleType),
            StructField("lon", DoubleType),
            StructField("gpsTimestamp", TimestampType)
          )
        )

        val imageDf = imagesDf.select(from_json(column("value").cast(StringType), imagesSchema).as[ImageDetails])
        val gpsDf = gpssDf.select(from_json(column("value").cast(StringType), gpsSchema).as[GpsDetails])

        testSession.udf.register("time_in_milliseconds", (str:String) => Timestamp.valueOf(str).getTime)

        val joinedDef = imageDf.join(
          gpsDf,
          expr(
            """
              cameraId = gpscameraId AND
              abs(time_in_milliseconds(timestamp) - time_in_milliseconds(gpsTimestamp)) <= 9000
            """.stripMargin)
        )

        val query =
          joinedDef.select("cameraId", "timeStamp", "gpsTimeStamp")
            .writeStream
            .outputMode("append")
            .option("truncate", "false")
            .format("console")
            /*.option("kafka.bootstrap.servers", "localhost:6001")
            .option("topic", "testoutput")
            .option("checkpointLocation", "/home/knoldus/")*/
            .start()

        Future {
          publishImagesToKafka(1,1000)
        }

        Future {
          publishGPSDataToKafka(1,10000)
        }

        query.awaitTermination()

        implicit val deserializer = new serialization.StringDeserializer()
        val ret = consumeFirstMessageFrom("gpssource")

        println(s"The published topic :::::::$ret")
      }

    }

  }

}
