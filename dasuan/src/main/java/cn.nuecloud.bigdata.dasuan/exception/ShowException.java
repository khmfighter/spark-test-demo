package cn.nuecloud.bigdata.dasuan.exception;

/**
 * 数据展示阶段异常
 * @version Neucloud2016
 * @author xuhaifeng
 */
public class ShowException extends Exception{
    public ShowException(String message){
        super("Show Stage Exception CaseBy { " + message + " }");
    }
}
