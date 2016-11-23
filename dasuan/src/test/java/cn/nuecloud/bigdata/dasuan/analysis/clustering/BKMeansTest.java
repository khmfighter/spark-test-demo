package cn.nuecloud.bigdata.dasuan.analysis.clustering;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.mllib.linalg.Vector;
import org.junit.Before;
import org.junit.Test;


/**
 * Bisecting K-Means测试类
 * @author xuhaifeng
 * @version Neucloud2016 2016-11-08
 */
public class BKMeansTest {

    private SparkConf conf;
    private JavaSparkContext jsc;
    private SQLContext sqlContext;
    private DataFrame df;

    @Before
    public void setUp(){
        conf = new SparkConf().setAppName("test").setMaster("local[5]");
        jsc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(jsc);
        df = sqlContext.read().json("data/mllib/gm.json");
        df.printSchema();
        df.show();
    }

    @Test
    public void testCluster() throws Exception {

        BKMeans bkMeans = new BKMeans();
        BisectingKMeansModel model = bkMeans.cluster(df,3,10,2);
        double cost = bkMeans.computeCost(model,df);
        System.out.println(cost);
        double[] point = new double[3];
        point[0] = 1;
        point[1] = 3;
        point[2] = 7;
        Vector pPoint = Vectors.dense(point);
        int cluster = bkMeans.predict(model,pPoint);
        System.out.println(cluster);
    }

}