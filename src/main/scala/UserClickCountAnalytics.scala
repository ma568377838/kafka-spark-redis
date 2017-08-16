import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 描述：实时统计用户点击量，并放入redis中, 使用Scala 2.10.x版本
  * 作者: JinHuaTao
  * 时间：2017/8/15 13:10
  */
object UserClickCountAnalytics {

  def main(args: Array[String]) {
    var masterUrl = "local[1]"
    if(args.length > 0){
      masterUrl = args(0)
    }

    //create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(5))

    //Kafka configurations
    val topics = Set("user-events")
    //val brokers = "10.10.4.126:9092,10.10.4.127:9092"
    val brokers = "192.168.40.128:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder"
    )
    val dbIndex = 1
    val clickHashKey = "app::users::click"

    //Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {
      val data = JSONObject.fromObject(line._2)
      Some(data)
    })

    //Compute user click times
    val userClicks = events.map(x => (x.getString("uid"), x.getInt("click_count"))).reduceByKey(_+_)

    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition( partitionOfRecords => {
        partitionOfRecords.foreach( pair => {
          val uid = pair._1
          val clickCount = pair._2
          val jedis = RedisClient.pool.getResource
          jedis.select(dbIndex)//选择一个数据库
          jedis.hincrBy(clickHashKey, uid, clickCount)//获取某个用户的点击量并增加(或减少)
          RedisClient.pool.returnResource(jedis)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
