package cn.nuecloud.bigdata.dasuan.analysis.clustering;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.GaussianMixtureModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.Before;
import org.junit.Test;

/**
 * 高斯混合测试类
 * @author xuhaifeng
 * @version Neucloud2016 2016-11-08
 */
public class GaussianMixtureTest {

    private SparkConf conf;
    private JavaSparkContext jsc;
    private SQLContext sqlContext;
    private DataFrame df;

    @Before
    public void setUp(){
        conf = new SparkConf().setAppName("testGM").setMaster("local[5]");
        jsc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(jsc);
        df = sqlContext.read().json("data/mllib/gm.json");
        df.printSchema();
        df.show();
    }
    @Test
    public void testCluster() throws Exception {
        GaussianMixture gaussianMixture = new GaussianMixture();
        GaussianMixtureModel model = gaussianMixture.cluster(df,3,10,0.001,2);
        double[] point = new double[3];
        point[0] = 1;
        point[1] = 3;
        point[2] = 7;
        Vector pPoint = Vectors.dense(point);
        int cluster = gaussianMixture.predict(model,pPoint);
        System.out.println(cluster);
        gaussianMixture.printComponents(model,pPoint);
        gaussianMixture.printWeights(model);
    }
}