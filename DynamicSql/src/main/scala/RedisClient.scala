import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Created by mahuichao on 16/7/27.
  */
object RedisClient {
  val redisHost = "xxxxxxxx"
  val redisPort = 6379
  val redisTimeout = 30000
  val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(20)
  //  lazy val pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)
  lazy val pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)
}
