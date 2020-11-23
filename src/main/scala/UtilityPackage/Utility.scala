/*
The objective is to create a utility class which can be passed over
different classes to create the required session and context object if Required
@author:Niraj Kumar
 */

package UtilityPackage

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Utility {

  /* Function to Create Spark Session Object
  @return SparkSession
   */
  def createSessionObject(appName: String): SparkSession = {

    val sparkConfigurations = new SparkConf()
      .setAppName(appName)
      .setMaster("local")
      .set("spark.streaming.kafka.maxRatePerPartition", "1")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val sparkSessionObj = SparkSession
      .builder()
      .config(sparkConfigurations)
      .getOrCreate()
    sparkSessionObj
  }
  /* Function to get Root Logger with Level as Error
  @return RootLogger
   */

  def getRootLogger(): Logger = {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    rootLogger
  }

  def runPythonCommand(
      filepath: String,
      inputDataFrame: DataFrame
  ): RDD[String] = {
    try {
      val command = "python3" + " " + filepath
      // creating rdd with the input files,repartitioning the rdd and passing the command using pipe

      val predictedPriceRDD =
        inputDataFrame.rdd
          .repartition(1)
          .pipe(command)
      predictedPriceRDD
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw new Exception("Error in running python command")
    }
  }

  /**
    * The function sets the Kafka Producer Properties and return a kafka producer
    * @return producer KafkaProducer[String, String]
    */

  def createKafkaProducer(brokers: String): KafkaProducer[String, String] = {
    try {
      val properties = new Properties()
      // Adding bootstrap servers
      properties.put("bootstrap.servers", brokers)
      //Adding serializer for key
      properties.put(
        "key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      //Adding serializer for value
      properties.put(
        "value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      // Creating a producer with provided properties
      val producer = new KafkaProducer[String, String](properties)
      producer
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw new Exception("Difficulty in Configuring Kafka Producer")
    }
  }

}
