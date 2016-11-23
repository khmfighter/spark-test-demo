package cn.nuecloud.bigdata.dasuan.util;

/**
 * 常量接口.
 * <p>程序中使用的常量在配置文件中的key值<br>
 * @version Neucloud2016
 * @author xuhaifeng Modify on 20160809
 */
public interface Constants {

    String SPARK_MASTE_LOCAL = "spark.local";
    String SPARK_MASTE_REMOTE = "spark.remote";
    String SPARK_APP_NAME = "spark.appName";
    String SPARK_DARA_SOURCE = "spark.data.source";
    String SPARK_DATE_FORMAT = "spark.date.format";
    String SPARK_DATE_INTERVAL = "spark.date.inteval";
    String SPARK_DATE_ACCURACY = "spark.date.accuracy";
    String ZOOKEEPER_ADDR = "spark.zookeeper.addr";
    String HBASE_TABLE_NAME = "spark.hbase.tableName";
    String HBASE_COLUMN_NAME = "spark.hbase.columnName";
}
