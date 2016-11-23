package cn.nuecloud.bigdata.dasuan.analysis.sessionize;

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
 * sessionize测试类
 * @author xuhaifeng
 * @version Neucloud2016 2016-10-26
 */
public class SessionizeTest {

    SparkConf conf;
    JavaSparkContext jsc;
    SQLContext sqlContext;
    DataFrame dt;
    DataFrame dataFrame;

    @Before
    public void setUp(){
        conf = new SparkConf().setAppName("test").setMaster("local[3]").set("spark.default.parallelism","1");
        jsc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(jsc);
        dt = sqlContext.read().json("data/testFile/test.json");
    }
    @Test
    public void testByInterval() throws Exception {
        try {
            dataFrame = Sessionize.byInterval(jsc, dt, 3000L, 2L, 20L);
        } catch (MyException e) {
            e.printStackTrace();
        }
        dataFrame.printSchema();
        dataFrame.show();
        List<Row> list = dataFrame.toJavaRDD().collect();
        for(int i=0;i<list.size();i++){
            System.out.println(list.get(i));
        }
    }

    @Test
    public void testByTimeRange() throws Exception {
        try{
            dataFrame = Sessionize.byTimeRange(jsc,dt,2000l,2l,20l);
        } catch (MyException e){
            e.printStackTrace();
        }
        dataFrame.printSchema();
        dataFrame.show();
        List<Row> list = dataFrame.toJavaRDD().collect();
        for(int i=0;i<list.size();i++){
            System.out.println(list.get(i));
        }
    }

    @Test
    public void testByValueRange() throws Exception {
        try{
            dataFrame = Sessionize.byValueRange(jsc,dt,1.0,1l,20l);
        } catch (MyException e){
            e.printStackTrace();
        }
        dataFrame.printSchema();
        dataFrame.show();
        List<Row> list = dataFrame.toJavaRDD().collect();
        for(int i=0;i<list.size();i++){
            System.out.println(list.get(i));
        }
    }

    @Test
    public void testByIntervalAndValueRange() throws Exception {
        try{
            dataFrame = Sessionize.byIntervalAndValueRange(jsc,dt,3000l,2l,1.0,1l,20l);
        } catch (MyException e){
            e.printStackTrace();
        }
        dataFrame.printSchema();
        dataFrame.show();
        List<Row> list = dataFrame.toJavaRDD().collect();
        for(int i=0;i<list.size();i++){
            System.out.println(list.get(i));
        }
    }

    @Test
    public void testByIntervalAndTimeRange() throws Exception {
        try{
            dataFrame = Sessionize.byIntervalAndTimeRange(jsc,dt,5000l,2000l,2l,20l);
        } catch (MyException e){
            e.printStackTrace();
        }
        dataFrame.printSchema();
        dataFrame.show();
        List<Row> list = dataFrame.toJavaRDD().collect();
        for(int i=0;i<list.size();i++){
            System.out.println(list.get(i));
        }
    }
    @Test
    public void testVis(){
        try {
            dataFrame = Sessionize.byInterval(jsc, dt, 3000L, 2L, 20L);
            Sessionize.visualize(dataFrame);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}