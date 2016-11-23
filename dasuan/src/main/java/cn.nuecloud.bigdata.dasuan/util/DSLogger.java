package cn.nuecloud.bigdata.dasuan.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import scala.reflect.internal.Trees;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Administrator on 2016/11/23 0023.
 */
public class DSLogger {

    String name =null;
    public DSLogger(String name){
        this.name = name;
    }
    // 分析日志
    private final Logger DEV_ANALYSIS = Logger.getLogger("analysis");
    //预处理
    private final Logger DEV_PREPROCESS = Logger.getLogger("preprocess");
    // 收集日志
    private final Logger DEV_COLLECT = Logger.getLogger("collect");
    // 展示
    private final Logger DEV_SHOWDATA = Logger.getLogger("visualize");
    // exception
    private final Logger DEV_EXCEPTION = Logger.getLogger("exception");

    private final int BUFFER_SIZE = 1024;

    public void setOff_AnalysisLog() {
        this.DEV_ANALYSIS.setLevel(Level.OFF);
    }

    public void setOff_PreprocessLog() {
        this.DEV_PREPROCESS.setLevel(Level.OFF);
    }

    public void setOff_CollectLog() {
        this.DEV_COLLECT.setLevel(Level.OFF);
    }

    public void setOff_VisualizeLog() {
        this.DEV_SHOWDATA.setLevel(Level.OFF);
    }

    public void setOff_ExceptionLog() {
        this.DEV_EXCEPTION.setLevel(Level.OFF);
    }

    //bigDataLog.postbackLog(needtrack);
    public void loginfo_a(String smsCenter) {
        if (smsCenter != null) {
            StringBuffer sb = new StringBuffer(BUFFER_SIZE);
            sb.append(getDateFormatString(new Date(), 4));
            sb.append(" ");
            sb.append(name);
            sb.append(" ");
            sb.append(smsCenter);
            sb.append(" ");
            DEV_ANALYSIS.info(sb.toString());
        }
    }

    public void logInfo_p(String smsCenter) {
        if (smsCenter != null) {
            DEV_PREPROCESS.info(smsCenter);
        }
    }

    //collect
    public void logInfo_c(String smsCenter) {
        if (smsCenter != null) {
            DEV_COLLECT.info(smsCenter);
        }
    }

    //show
    public void logInfo_v(String smsCenter) {
        if (smsCenter != null) {
            DEV_SHOWDATA.info(smsCenter);
        }
    }

    public void logError(String smsCenter) {
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

    public SimpleDateFormat getSdf(int t) {

        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
        SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy年MM月dd日");
        SimpleDateFormat sdf4 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sdf5 = new SimpleDateFormat("yyyy");
        SimpleDateFormat sdf6 = new SimpleDateFormat("MM/dd");
        SimpleDateFormat sdf7 = new SimpleDateFormat("yyyy.MM.dd");

        switch (t) {
            case 1:
                return sdf1;
            case 2:
                return sdf2;
            case 3:
                return sdf3;
            case 4:
                return sdf4;
            case 5:
                return sdf5;
            case 6:
                return sdf6;
            case 7:
                return sdf7;
            default:
                return sdf1;
        }
    }

    public String getDateFormatString(Date date, int t)
    {
        if (date == null) {
            return "";
        }
        SimpleDateFormat sdf = getSdf(t);
        return sdf.format(date);
    }
}
