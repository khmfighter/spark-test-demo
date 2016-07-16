package origincode

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.DAGSchedulerEvent
import org.apache.spark.util.EventLoop

/**
 * Created by spark on 1/19/16.
 */
class JDAGScheduler {

  val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)

  def runJob[T, U](rdd: RDD[T]): Unit = {
    val waiter = submitJob()
  }

  def submitJob(): Unit = {
    eventProcessLoop.post(JobSubmitted())
  }
  def handleJobSubmitted() :Unit = {
    val finalStage: JResultStage
    finalStage = newResultStage

  }
  private def newResultStage(): {
    var finalStage: JResultStage
    finalStage
  }


}

sealed trait JDAGSchedulerEvent
case class JobSubmitted extends JDAGSchedulerEvent

class DAGSchedulerEventProcessLoop(dagScheduler: JDAGScheduler)extends JEventLoop[JDAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  override def onReceive(event: JDAGSchedulerEvent): Unit = {
    //val timerContext = timer.time()
    doOnReceive(event)
  }
  private def doOnReceive(event: JDAGSchedulerEvent): Unit = event match{
    case JobSubmitted => dagScheduler.handleJobSubmitted()
  }
  override def onStop(): Unit = {}
  override def onError(e: Throwable): Unit = {}
}



















