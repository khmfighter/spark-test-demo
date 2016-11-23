package cn.nuecloud.bigdata.dasuan.exception;

/**
 * 数据存储阶段异常
 * @version Neucloud2016
 * @author xuhaifeng
 */
public class StorageException extends Exception{

    public StorageException(String message){

        super("Storage Stage Exception CaseBy { " + message + " }");
    }
}
