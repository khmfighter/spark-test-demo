package cn.nuecloud.bigdata.dasuan.analysis.stat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.beans.Transient;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by Administrator on 2016/11/23 0023.
 */
public class CorRelationTest implements Serializable {
    @Test
    public void computOne() throws Exception {
        SparkConf conf;
        JavaSparkContext jsc;
        SQLContext sqlContext;
        DataFrame df;

        conf = new SparkConf().setAppName("kmeans").setMaster("local[4]");
        jsc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(jsc);


        List<String> list = new ArrayList<>();
        list.add("22");
        list.add("33");
        list.add("876");
        JavaRDD<Row> rows = jsc.parallelize(list).map(new Function<String, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Row call(String line) {
                return RowFactory.create(Double.parseDouble(line));
            }
        });
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("key1", DataTypes.DoubleType, true));
        structFields.add(DataTypes.createStructField("value1", DataTypes.DoubleType, true));
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame personDF = sqlContext.createDataFrame(rows, structType);
        double re = DSCorRelation.computOne(personDF,personDF);

        System.out.println(re);
    }

    @Test
    public void computOne1() throws Exception {

    }

}