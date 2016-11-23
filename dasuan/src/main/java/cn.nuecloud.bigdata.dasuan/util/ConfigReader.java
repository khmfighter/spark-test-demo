package cn.nuecloud.bigdata.dasuan.util;


import java.io.InputStream;
import java.util.Properties;

/**
 *
 * @version Neucloud2016
 * @author xuhaifeng Modify on 20160809
 */
public class ConfigReader{

    private static Properties prop = new Properties();

    /**
     * JVM第一次使用类的时候会加载一次类，并执行类的静态初始化代码段，将配置文件以流的方式读取，并将输入流读入配置文件
     * 能够保证在JVM的整个生命周期只被加载一次
     */
    static {
        try {
            InputStream in = ConfigReader.class
                    .getClassLoader().getResourceAsStream("my.properties");
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 获取字符串类型的配置项
     * @param key
     * @return value
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    /**
     * 获取整数类型的配置项
     * @param key
     * @return value
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取布尔类型的配置项
     * @param key
     * @return value
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取Long类型的配置项
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }

    public int add(int num1, int num2){
        return num1 + num2;
    }
}
