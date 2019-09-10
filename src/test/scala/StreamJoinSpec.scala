package com.knoldus

import java.sql.Timestamp
import java.time.Instant

import com.knoldus.api.StreamToStreamJoin
import com.knoldus.model.{GpsDetails, ImageDetails}
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.scalatest.WordSpec


import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class StreamJoinSpec extends WordSpec with EmbeddedKafka {

  implicit val serializer = new StringSerializer()

  def publishImagesToKafka(start: Int, end: Int) = {
    start to end foreach { recordNum =>
      //TODO Add Serializer and dser
      val imageDetails = ImageDetails((recordNum % 4).toString, recordNum.toString, recordNum.toString, Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis())))
      publishToKafka("camerasource", imageDetails.toString)
      Thread.sleep(1000)
    }
  }

  def publishGPSDataToKafka(start: Int, end: Int) = {
    start to end foreach { recordNum =>
      //TODO Add Serializer and dser
      val gpsDetails = GpsDetails((recordNum % 4).toString, recordNum.toString, recordNum.toDouble, recordNum.toDouble, Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis())))
      publishToKafka("gpssource", gpsDetails.toString)
      Thread.sleep(100)
    }
  }

  "StreamToStreamJoin" should {

    val testSession =
      SparkSession
        .builder()
        .appName("StreamToStreamJoinTest")
        .master("local")
        .getOrCreate()

    val sc = testSession.sparkContext
    sc.setLogLevel("WARN")

    //sut = system under test
    val sut = new StreamToStreamJoin(testSession)

    /*"should find nearest frame with relevant timestamp" in {

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

        testSession.udf.register("time_in_milliseconds", (str: String) => Timestamp.valueOf(str).getTime)

        val joinedDef = new StreamToStreamJoin(testSession).findNearest(imageDf, gpsDf, 500)

        joinedDef.printSchema()


        val query =
          joinedDef.select( "ImageId", "timestamp", "nearest" )
            .writeStream
            .outputMode("Append")
            .option("truncate", "false")
            .format("console")
            /*.option("kafka.bootstrap.servers", "localhost:6001")
            .option("topic", "output")
            .option("checkpointLocation", "/home/knoldus/")*/
            .start()
        Future {
          publishImagesToKafka(1, 1000)
        }

        Future {
          publishGPSDataToKafka(1, 10000)
        }

        query.awaitTermination()

        implicit val deserializer = new serialization.StringDeserializer()
        val ret = consumeFirstMessageFrom("gpssource")

        println(s"The published topic :::::::$ret")
      }

    }*/

    "should aggregate images into a 10 seconds" in {

      withRunningKafka {
        val imagesDf = testSession
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:6001")
          .option("subscribe", "camerasource")
          .option("startingOffsets", "earliest")
          .option("checkpointLocation", "/home/knoldus/")
          .load()

        val imagesSchema = StructType(
          Seq(
            StructField("cameraId", StringType),
            StructField("imageId", StringType),
            StructField("imageUrl", StringType),
            StructField("timestamp", TimestampType)
          )
        )

        implicit val imageEncoder: Encoder[ImageDetails] = Encoders.product[ImageDetails]
        val imageDf = imagesDf.select(from_json(column("value").cast(StringType), imagesSchema).as[ImageDetails])
        val outputdf = sut.aggregatedWindow(imageDf, "10 seconds")

        val query =
          outputdf.select( "window", "collect_list(ImageId)")
            .writeStream
            .outputMode("Append")
            .option("truncate", "false")
            .format("console")
            /*.option("kafka.bootstrap.servers", "localhost:6001")
            .option("topic", "output")
            .option("checkpointLocation", "/home/knoldus/")*/
            .start()
        outputdf.printSchema()

        val res = Future {
          publishImagesToKafka(1, 1000)
        }

        Await.result(res, Duration.Inf)
      }
    }
  }

}
