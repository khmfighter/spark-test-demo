package cn.nuecloud.bigdata.dasuan.analysis.normalization;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;


/**
 * Normalization测试类
 * @author qingda
 * @version Neucloud 2016 2016-11-03
 */
public class NormalizationTest {

    @Test
    public void testZscore() throws Exception {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        DataFrame df1 = sqlContext.read().json("data/testFile/normalization.json");
        DataFrame reJavaRDD = Normalization.zscore(jsc,sqlContext, df1);
        reJavaRDD.show();
    }

    @Test
    public void testMinMax() throws Exception {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);
        DataFrame df1 = sqlContext.read().json("data/testFile/normalization.json");
        DataFrame reJavaRDD = Normalization.MinMax(jsc,sqlContext, df1);
        reJavaRDD.show();/**
         * cross_correlation测试类
         * @author qingda
         * @version Neucloud 2016 2016-11-03
         */
    }
}