package cn.nuecloud.bigdata.dasuan.analysis.movingwindowaverage;

import cn.nuecloud.bigdata.dasuan.exception.MyException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Before;
import org.junit.Test;


/**
 * 滑动窗口求均值测试类
 * @author xuhaifeng
 * @version Neucloud2016 2016-11-17
 */
public class MovingWindowTest implements java.io.Serializable{

    private SparkConf conf;
    private JavaSparkContext jsc;
    private SQLContext sqlContext;
    private DataFrame df;
    @Before
    public void setUp(){
        conf = new SparkConf().setAppName("test").setMaster("local[3]");
        jsc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(jsc);
        df = sqlContext.read().json("data/mllib/movingWindow.json");
    }
    @Test
    public void testMovingWindowAverage() throws Exception {
        try {
            DataFrame dataFrame = MovingWindow.movingWindowAverage(jsc,df,1,4,2);
            dataFrame.show();
            dataFrame.javaRDD().foreach(new VoidFunction<Row>() {
                @Override
                public void call(Row row) throws Exception {
                    System.out.println(row.toString());
                }
            });
        }catch (MyException e){
            e.printStackTrace();
        }
    }
}