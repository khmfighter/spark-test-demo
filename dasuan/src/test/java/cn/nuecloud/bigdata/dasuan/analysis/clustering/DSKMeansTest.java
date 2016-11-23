package cn.nuecloud.bigdata.dasuan.analysis.clustering;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Jaryzhen on 2016/11/23 0023.
 */
public class DSKMeansTest {
    private SparkConf conf;
    private JavaSparkContext jsc;
    private SQLContext sqlContext;
    private DataFrame df;
    @Before
    public void setUp() throws Exception {
        conf = new SparkConf().setAppName("kmeans").setMaster("local[4]");
        jsc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(jsc);
    }

    @Test
    public void clusters() throws Exception {
        String datadir = "data/mllib/kmeans_data.txt";
        DSKMeans kms = new DSKMeans(datadir, jsc);
        KMeansModel kmd = kms.clusters(3, 29);
        ////print the centers </p>
        kms.printClusters(kmd);
        //System.out.println("轮廓系数：" + kms.silin(kmd));
        //System.out.println("欧几里得：" + kms.euclid(kmd));
        ////* <p> must stop the sc </p>
        //kms.stop();
    }


}