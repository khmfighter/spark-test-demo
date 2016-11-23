package cn.nuecloud.bigdata.dasuan.analysis.fpgrowth;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.junit.Before;
import org.junit.Test;

/**
 * 频繁项集挖掘测试类
 * @author xuhaifeng
 * @version Neucloud2016 2016-11-08
 */
public class FPGroup_NeuTest {

    private SparkConf conf;
    private JavaSparkContext jsc;
    private String path;

    @Before
    public void setUp(){
        conf = new SparkConf().setAppName("testFPG").setMaster("local[5]");
        jsc = new JavaSparkContext(conf);
        path = "/data/mllib/sample_fpgrowth.txt";
    }

    @Test
    public void testFpGrowth() throws Exception {
        FPGrowth_Neu fpg = new FPGrowth_Neu(jsc);
        FPGrowthModel<String> model = fpg.fpGrowth(path,0.2,10);
        fpg.printFreqItem(model);
        System.out.println("-------------------------");
        fpg.printGenerateAssociation(model,0.8);
    }
}