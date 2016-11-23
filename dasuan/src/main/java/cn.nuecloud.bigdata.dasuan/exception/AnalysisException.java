package cn.nuecloud.bigdata.dasuan.exception;

/**
 * 数据分析阶段异常
 * @version Neucloud2016
 * @author xuhaifeng
 */
public class AnalysisException extends Exception{
    public AnalysisException(String message){
        super("Data Analysis Stage Exception CaseBy {" + message + "}");
    }
}
