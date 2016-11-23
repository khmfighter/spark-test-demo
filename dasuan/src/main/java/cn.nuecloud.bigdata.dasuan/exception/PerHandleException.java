package cn.nuecloud.bigdata.dasuan.exception;

/**
 * 数据预处理阶段异常
 * @version Neucloud2016
 * @author xuhaifeng
 */
public class PerHandleException extends Exception{
    public PerHandleException(String message){
        super("Data PerHandle Stage Exception CaseBy {" + message + "}");
    }
}
