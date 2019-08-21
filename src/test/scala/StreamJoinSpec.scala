package com.knoldus

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import com.knoldus.api.StreamToStreamJoin
import com.knoldus.model.ImageDetails
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.WordSpec

class StreamJoinSpec extends WordSpec with EmbeddedKafka {


  def publishImagesToKafka = {
    1 to 100 map { recordNum =>
      val uuid = UUID.randomUUID()
      //TODO Add Serializer and dser
      publishToKafka("camerasource", ImageDetails(uuid, uuid, recordNum.toString, Timestamp.from(Instant.ofEpochMilli(recordNum))))
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

        //TODO Publish required records before aggregating
        publishImagesToKafka

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
