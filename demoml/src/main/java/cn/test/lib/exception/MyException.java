package cn.test.lib.exception;

/**
 * 自定义异常类
 * @version Neucloud2016 2016-09-09
 * @author xuhaifeng
 */
public class MyException extends Exception {
    public MyException(String message) {
        super(message);
    }
}
