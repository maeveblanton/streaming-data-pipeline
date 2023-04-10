package com.labs1904.hwe

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountBatchApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "WordCountBatchApp"

  def main(args: Array[String]): Unit = {
    logger.info(s"$jobName starting...")
    try {
      val spark = SparkSession.builder()
        .appName(jobName)
        .config("spark.sql.shuffle.partitions", "3")
        .master("local[*]")
        .getOrCreate()
      import spark.implicits._

      val sentences = spark.read.csv("src/main/resources/sentences.txt").as[String]

      val words = sentences.flatMap(row => splitSentenceIntoWords(row))

      // TODO: implement me

      val counts = words.groupBy(col("value")).count().sort(col("count").desc, col("value"))
      counts.printSchema()
      counts.show()

      //counts.foreach(wordCount=>println(wordCount))
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  // TODO: implement this function
  // HINT: you may have done this before in Scala practice...
  def splitSentenceIntoWords(sentence: String): Array[String] =
    sentence
      .toLowerCase
      .replaceAll("[^a-z0-9'\\s]+","")
      .split(" ")

}
