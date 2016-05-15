package workdemo

import java.util.UUID

import akka.actor.{Props, ActorSystem, ActorSelection, Actor}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

/**
 * Created by Administrator on 2016/5/14.
 */
class Worker(val masterHost: String, val masterPort: Int, val memory: Int, val cores: Int) extends Actor with RemoteMessage {

  var master: ActorSelection = _
  val worker_id = UUID.randomUUID().toString
  val HEART_INTERVAL = 10000

  // 注册消息
  override def preStart(): Unit = {
    // 跟master建立连接
    master = context.actorSelection(s"akka.tcp://MasterSystem@$masterHost:$masterPort/user/Master")
    // 向master发送注册信息
    master ! RegisterRequest(worker_id, memory, cores)
  }

  override def receive: Receive = {
    case RegisterResponse(masterInfo: String) => {
      println("successful regester")
      import context.dispatcher
      context.system.scheduler.schedule(0 millis, HEART_INTERVAL millis, self, SendHeartbeat)
    }
    case SendHeartbeat => {
      println("send heartbeat to master")
      master ! Heartbeat(worker_id)
    }
  }
}

object Worker {
  def main(args: Array[String]) {
    val masterHost = "192.168.152.13"
    val masterPort = 8888
    val host = "192.168.152.13"
    val port = 9999
    val memory = 12
    val cores = 4
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val actorSystem = ActorSystem("workerSystem", config)
    actorSystem.actorOf(Props(new Worker(masterHost, masterPort, memory, cores)), "Worker")
    actorSystem.awaitTermination()
  }

}