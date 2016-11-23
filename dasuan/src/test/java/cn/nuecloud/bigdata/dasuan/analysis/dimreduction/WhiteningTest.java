package cn.nuecloud.bigdata.dasuan.analysis.dimreduction;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * PCA白化测试类
 * @author xuhaifeng
 * @version Neucloud2016 2016-10-27
 */

public class WhiteningTest implements java.io.Serializable{

    @Before
    public void setUp(){
    }

    @Test
    public void testDimReduction() throws Exception {

        SparkConf conf = new SparkConf().setAppName("PCA Example").setMaster("local[5]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext jsql = new SQLContext(jsc);
        JavaRDD<Row> data = jsc.parallelize(Arrays.asList(RowFactory
                .create(Vectors.sparse(5, new int[] { 1, 3 }, new double[] {
                        1.0, 7.0 })), RowFactory.create(Vectors.dense(2.0, 0.0,
                3.0, 4.0, 5.0)), RowFactory.create(Vectors.dense(4.0, 0.0, 0.0,
                6.0, 7.0))));

        StructType schema = new StructType(new StructField[] { new StructField(
                "features", new VectorUDT(), false, Metadata.empty()), });

        DataFrame df = jsql.createDataFrame(data, schema);
        DataFrame dataFrame = Whitening.dimReduction(jsc,df,2,"features");
        dataFrame.printSchema();
        dataFrame.show();
        dataFrame.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row);
            }
        });

    }
}