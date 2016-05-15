import akka.actor.{Props, ActorSystem, Actor}
import com.typesafe.config.ConfigFactory

/**
 * Created by Administrator on 2016/5/13.
 */
class Master extends Actor {

  override def preStart(): Unit = {
    println("I am in preStart")
  }

  override def receive: Receive = {
    case "connect" => {
      println("client connected")
    }
    case "hello" => {
      println(" hello my friend")
    }
  }
}

object Master {
  def main(args: Array[String]) {
    val host = "192.168.152.13"
    val port = 8888

    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val actorSystem = ActorSystem("MasterSystem", config)
    // 创建Actor
    val master = actorSystem.actorOf(Props[Master], "Master")
    //    master ! "hello"
    actorSystem.awaitTermination()
  }
}
