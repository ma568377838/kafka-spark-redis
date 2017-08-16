import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.codehaus.jettison.json.JSONObject

import scala.util.Random

/**
  * 描述：Kafka  事件生产者
  * 作者: JinHuaTao
  * 时间：2017/8/15 8:48
  */
object KafkaEventProducer {

  private val users = Array(
    "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
    "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
    "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
    "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
    "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d"
  )

  private val random = new Random()

  private var pointer = -1

  def getUserID(): String = {
    pointer += 1
    if(pointer >= users.length){
      pointer = 0
      users(pointer)
    }else{
      users(pointer)
    }
  }

  def click(): Double = {
    random.nextInt(10)
  }

  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --create --topic user_events --replication-factor 2 --partitions 2
  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --list
  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --describe user_events
  // bin/kafka-console-consumer.sh --zookeeper zk1:2181,zk2:2181,zk3:22181/kafka --topic test_json_basis_event --from-beginning

  // bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic user-events
  def main(args: Array[String]) {
    // step01: prepare kafka parameters
    val topic = "user-events"
    //val brokers = "10.10.4.126:9092,10.10.4.127:9092"
    val brokers = "192.168.40.128:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    //step02: use parameters to create kafka config object
    val kafkaConfig = new ProducerConfig(props)

    //step03: use config object to create kafka producer
    val producer = new Producer[String, String](kafkaConfig)

    while (true){
      //prepare event data
      val event = new JSONObject()
      event.put("uid", getUserID())
        .put("event_time", System.currentTimeMillis())
        .put("os_type", "Android")
        .put("click_count", click())

      //step04: use producer object to send message
      producer.send(new KeyedMessage[String, String](topic, event.toString()))
      println("Message sent:" + event)

      Thread.sleep(200)
    }



  }

}
