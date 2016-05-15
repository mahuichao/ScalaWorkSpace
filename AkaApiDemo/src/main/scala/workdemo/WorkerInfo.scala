package workdemo

/**
 * Created by Administrator on 2016/5/14.
 */
class WorkerInfo(val id: String, val memory: Int, val cores: Int) {
  //TODO 上一次心跳
  var lastHeartbeatTime: Long = _
}
