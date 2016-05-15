package workdemo

import akka.actor.{Props, ActorSystem, Actor}
import com.typesafe.config.ConfigFactory
import scala.collection.mutable
import scala.concurrent.duration._

/**
 * Created by Administrator on 2016/5/14.
 */
class Master(val host: String, val port: Int) extends Actor with RemoteMessage {
  val idToWorker = new mutable.HashMap[String, WorkerInfo]()
  var workers = new mutable.HashSet[WorkerInfo]()
  // 超时时间
  val CHECK_INTERVAL = 15000

  override def preStart(): Unit = {
    println("start hearbeat schedule")
    import context.dispatcher
    context.system.scheduler.schedule(0 millis, CHECK_INTERVAL millis, self, CheckTimeOutWorker)
  }

  override def receive: Receive = {
    case RegisterRequest(id: String, memory: Int, cores: Int) => {
      if (!idToWorker.contains(id)) {
        val workerInfo = new WorkerInfo(id, memory, cores)
        idToWorker(id) = workerInfo
        workers += workerInfo
        sender ! RegisterResponse(s"akka.tcp://MasterSystem@$host:$port/user/Master")
      }
    }
    case CheckTimeOutWorker => {
      val currentTIme = System.currentTimeMillis()
      val toRemove = workers.filter(x => currentTIme - x.lastHeartbeatTime > CHECK_INTERVAL)
      for (w <- toRemove) {
        workers -= w
        idToWorker -= w.id
      }
      println(workers.size)
    }
    case Heartbeat(id) => {
      if (idToWorker.contains(id)) {
        val workerInfo = idToWorker(id)
        //报活
        val currentTime = System.currentTimeMillis()
        workerInfo.lastHeartbeatTime = currentTime
      }
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
    val actorySystem = ActorSystem("MasterSystem", config)
    val master = actorySystem.actorOf(Props[Master], "Master")
    println("Master are ready")
    actorySystem.awaitTermination()
  }


}
