package cn.nuecloud.bigdata.dasuan.analysis.statistic;

import cn.nuecloud.bigdata.dasuan.analysis.stat.BaseStatistic;
import cn.nuecloud.bigdata.dasuan.exception.MyException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * 基础分析测试类
 * @author xuhaifeng
 * @version Neucloud2016 2016-10-24.
 */
public class BaseStatisticTest {
    SparkConf conf;
    JavaSparkContext jsc;
    SQLContext sqlContext;
    DataFrame dt;
    DataFrame dataFrame;

    @Before
    public void setUp(){
        conf = new SparkConf().setAppName("test").setMaster("local[10]");
        jsc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(jsc);
        dt = sqlContext.read().json("data/testFile/test.json");
    }

    @Test
    public void test(){
        try {
            dataFrame = BaseStatistic.byIntervalWindow(jsc, dt, 2, 1,5000l);
            dataFrame.printSchema();
            dataFrame.show();
            List<Row> res = dataFrame.javaRDD().collect();
            for(int i=0;i<res.size();i++){
                System.out.println(res.get(i).toString());
            }
        }catch(MyException e){
            e.printStackTrace();
        }
    }
    @Test
    public void testVis() {
        try {
            dataFrame = BaseStatistic.byIntervalWindow(jsc, dt, 2, 1,5000l);
            BaseStatistic.visualize(dataFrame);

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}