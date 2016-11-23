package cn.nuecloud.bigdata.dasuan.analysis.cross_correlation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;


/**
 * cross_correlation测试类
 * @author qingda
 * @version Neucloud 2016 2016-11-03
 */

public class cross_correlationTest implements java.io.Serializable{

    @Test
    public void testCross() throws Exception {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        DataFrame df1 = sqlContext.read().json("data/testFile/cross_correlation.json");
        DataFrame reJavaRDD = cross_correlation.cross(jsc, df1);
        reJavaRDD.show();
    }
}