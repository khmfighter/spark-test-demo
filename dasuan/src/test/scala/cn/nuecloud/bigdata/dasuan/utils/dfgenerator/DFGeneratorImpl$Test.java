package cn.nuecloud.bigdata.dasuan.utils.dfgenerator;

import cn.nuecloud.bigdata.utils.SparkContextImpl;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by Administrator on 2016/10/10 0010.
 */
public class DFGeneratorImpl$Test {
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void dataToDF() throws Exception {
        SparkContext sc  = new SparkContextImpl("dfGenerator");
       // new Spark_Log();
        String datadir = "data/testFile/test.json";
       // assert (dataToDF());
    }

}