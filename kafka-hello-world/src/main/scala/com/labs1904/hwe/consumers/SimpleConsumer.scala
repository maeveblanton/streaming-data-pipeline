package com.labs1904.hwe.consumers

import com.labs1904.hwe.model.{EnrichedUser, RawUser}
import com.labs1904.hwe.util.Util
import com.labs1904.hwe.util.Util.getScramAuthString
import net.liftweb.json.DefaultFormats
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.time.Duration
import java.util.{Arrays, Properties, UUID}
import scala.collection.JavaConverters._

object SimpleConsumer {
  val BootstrapServer : String = "CHANGEME"
  val ConsumerTopic: String = "question-1"
  val ProducerTopic: String = "question-1-output"
  val username: String = "1904labs"
  val password: String = "1904labs"
  //Use this for Windows
  val trustStore: String = "src\\main\\resources\\kafka.client.truststore.jks"
  //Use this for Mac
  //val trustStore: String = "src/main/resources/kafka.client.truststore.jks"

  implicit val formats: DefaultFormats.type = DefaultFormats

  def main(args: Array[String]): Unit = {

    // Create the KafkaConsumer
    val consumerProperties = getConsumerProperties(BootstrapServer)
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerProperties)
    val producerProperties = getProducerProperties(BootstrapServer)
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerProperties)
    producer
    // Subscribe to the topic
    consumer.subscribe(Arrays.asList(ConsumerTopic))

    while ( {
      true
    }) {
      // poll for new data
      val duration: Duration = Duration.ofMillis(100)
      val records: ConsumerRecords[String, String] = consumer.poll(duration)

      records.asScala
        .map(transform)
        .foreach((message: String) => {
          println(s"Message Received: $message")
          val record = new ProducerRecord[String, String](ProducerTopic, message)
          producer.send(record)
        })
    }
  }

  def transform(record: ConsumerRecord[String, String]): String = {
    val list: Array[String] =
      record.value()
        .split('\t')


    val rawUser =
      RawUser(
        id = list(0),
        username = list(1),
        name = list(2),
        gender = list(3).charAt(0),
        email = list(4),
        dob = list(5)
      )

    EnrichedUser(
      id = rawUser.id,
      numberAsWord = Util.mapNumberToWord(rawUser.id),
      username = rawUser.username,
      name = rawUser.name,
      gender = rawUser.gender,
      email = rawUser.email,
      dob = rawUser.dob,
      hweDeveloper = "Maeve"
    ).toString
  }

  def getConsumerProperties(bootstrapServer: String): Properties = {
    // Set Properties to be used for Kafka Consumer
    val properties = new Properties
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString)
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    properties.put("security.protocol", "SASL_SSL")
    properties.put("sasl.mechanism", "SCRAM-SHA-512")
    properties.put("ssl.truststore.location", trustStore)
    properties.put("sasl.jaas.config", getScramAuthString(username, password))

    properties
  }

  def getProducerProperties(bootstrapServer: String): Properties = {
    // Set Properties to be used for Kafka Producer
    val properties = new Properties
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
//    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString)
//    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    properties.put("security.protocol", "SASL_SSL")
    properties.put("sasl.mechanism", "SCRAM-SHA-512")
    properties.put("ssl.truststore.location", trustStore)
    properties.put("sasl.jaas.config", getScramAuthString(username, password))

    properties
  }
}
