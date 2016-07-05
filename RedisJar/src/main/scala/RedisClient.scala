import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
 * Created by Administrator on 2016/6/7.
 */
object RedisClient {
  val redisHost = "sun"
  val redisPort = 6379
  val redisTimeout = 30000
  val poolConfig = new JedisPoolConfig()
  poolConfig.setMaxTotal(20)
  lazy val pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)
}
