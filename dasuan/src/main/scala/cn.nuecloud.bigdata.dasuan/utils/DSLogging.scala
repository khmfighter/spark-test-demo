package cn.nuecloud.bigdata.utils

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}

/**
  * Created by Administrator on 2016/10/9 0009.
  */
trait DSLogging extends Serializable {

  // 分析日志
  @transient lazy  final val DEV_ANALYSIS: Logger = Logger.getLogger("analysis")
  //预处理
  @transient lazy private final val DEV_PREPROCESS: Logger = Logger.getLogger("preprocess")
  // 收集日志
  @transient lazy private final val DEV_COLLECT: Logger = Logger.getLogger("collect")
  // 展示
  @transient lazy private final val DEV_SHOWDATA: Logger = Logger.getLogger("visualize")
  // exception
  @transient lazy private final val DEV_EXCEPTION: Logger = Logger.getLogger("exception")

  @transient lazy private final val BUFFER_SIZE: Int = 1024;

  def setOff_AnalysisLog(){
    this.DEV_ANALYSIS.setLevel(Level.OFF)
  }
  def setOff_PreprocessLog(){
    this.DEV_PREPROCESS.setLevel(Level.OFF)
  }
  def setOff_CollectLog(){
    this.DEV_COLLECT.setLevel(Level.OFF)
  }
  def setOff_VisualizeLog(){
    this.DEV_SHOWDATA.setLevel(Level.OFF)
  }
  def setOff_ExceptionLog(){
    this.DEV_EXCEPTION.setLevel(Level.OFF)
  }
  //bigDataLog.postbackLog(needtrack);
  def loginfo_a(smsCenter: String) {
    if (smsCenter != null) {
      val sb = new StringBuffer(BUFFER_SIZE);
      sb.append(getDateFormatString(new Date(), 4));
      sb.append(" ");
      sb.append(this.getClass.getName)
      sb.append(" ");
      sb.append(smsCenter);
      sb.append(" ");
      DEV_ANALYSIS.info(sb.toString());
    }
  }

  def logInfo_p(smsCenter: String) {
    if (smsCenter != null) {
      DEV_PREPROCESS.info(smsCenter);
    }
  }

  //collect
  def logInfo_c(smsCenter: String) {
    if (smsCenter != null) {
      DEV_COLLECT.info(smsCenter);
    }
  }
//show
  def logInfo_v(smsCenter: String) {
    if (smsCenter != null) {
      DEV_SHOWDATA.info(smsCenter);
    }
  }

  def logError(smsCenter: String) {
    if (smsCenter != null) {
      DEV_EXCEPTION.info(smsCenter);
    }
  }

  //private static void errorLog(RequestBean request, ResponseBean response,
  //        String result, long totaltime)
  //{
  //    StringBuffer sb = new StringBuffer(BUFFER_SIZE);
  //    sb.append(OverseaUtil.getDateFormatString(new Date(), 4));
  //    sb.append("|");
  //    sb.append(request.getIp());
  //    sb.append("|");
  //    sb.append(request.getSrc());
  //    sb.append("|");
  //    sb.append(request.getValue(OverseaTag.client_id));
  //    sb.append("|");
  //    sb.append(request.getValue(OverseaTag.channel));
  //    sb.append("|");
  //    sb.append(request.getValue(OverseaTag.net));
  //    sb.append("|");
  //    sb.append(request.getValue(OverseaTag.imei));
  //    sb.append("|");
  //    sb.append(request.getValue(OverseaTag.imsi));
  //    sb.append("|");
  //    sb.append(request.toString());
  //    sb.append("|");
  //    sb.append(response.toString());
  //    sb.append("|");
  //    sb.append(totaltime);
  //    sb.append("|");
  //    sb.append(result);
  //    USER_ERROR_LOG.error(sb.toString());
  //}

  private def getSdf(`type`: Int): SimpleDateFormat = {

    val sdf1 = new SimpleDateFormat("yyyyMMdd");
    val sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
    val sdf3 = new SimpleDateFormat("yyyy年MM月dd日");
    val sdf4 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val sdf5 = new SimpleDateFormat("yyyy");
    val sdf6 = new SimpleDateFormat("MM/dd");
    val sdf7 = new SimpleDateFormat("yyyy.MM.dd")

    (`type`) match  {
      case 1 => sdf1;
      case 2=> sdf2;
      case 3=> sdf3;
      case 4=> sdf4;
      case 5=> sdf5;
      case 6=> sdf6;
      case 7=> sdf7;
      case _  => sdf1;
    }
  }

  def getDateFormatString(date : Date,t :Int) : String = {

    if (date == null) {
      return "";
    }
    val sdf : SimpleDateFormat = getSdf(t);

    sdf.format(date);

  }
}
object Spark_Log {

  def apply() {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.log4j").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark.Logging").setLevel(Level.OFF)
  }
}
