import akka.actor.{Props, ActorSystem, ActorSelection, Actor}
import com.typesafe.config.ConfigFactory
import workdemo.RemoteMessage.RegisterRequest

/**
 * Created by Administrator on 2016/5/14.
 */
class Worker(val masterHost: String, val masterPort: Int) extends Actor {
  var master: ActorSelection = _


  // 建立连接
  override def preStart(): Unit = {
    master = context.actorSelection(s"akka.tcp://MasterSystem@$masterHost:$masterPort/user/Master")
    master ! "connect"
  }

  override def receive: Receive = {
    case "reply" => {
      println("a reply form master")
    }
  }
}

object Worker {
  def main(args: Array[String]) {
    val host = "127.0.0.1"
    val port = 1234
    val masterHost = "192.168.152.13"
    val masterPort = 8888
    // 准备配置
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    // ActorSystem老大，辅助创建和监控下面的Actor,他是单例的
    val actorSystem = ActorSystem("WorkSystem", config)
    actorSystem.actorOf(Props(new Worker(masterHost, masterPort)), "Worker")
    actorSystem.awaitTermination()
  }


}


