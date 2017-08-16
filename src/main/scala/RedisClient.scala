import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * 描述：
  * 作者: JinHuaTao
  * 时间：2017/8/15 16:02
  */
object RedisClient extends Serializable{
  val redisHost = "192.168.40.128"
  val redisPort = 6379
  val redisTimeout = 30000
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  lazy val hook = new Thread{
    override def run(): Unit = {
      println("Execute hook thread:" + this)
      pool.destroy()
    }
  }

  def main(args: Array[String]) {
    val jedis = RedisClient.pool.getResource
    jedis.set("jin", "tank")
    val tt = jedis.get("jin")
    println(tt)
  }

  sys.addShutdownHook(hook.run())
}
