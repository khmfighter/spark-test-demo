package cn.nuecloud.bigdata.dasuan.visualize;

import cn.nuecloud.bigdata.dasuan.analysis.normalization.Normalization;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 2016/10/27.
 */
public class normalization_test implements java.io.Serializable{
    public DataFrame testFrame() {
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2");
        SparkConf sparkConf = new SparkConf().setAppName("Regression").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);
        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        conf.set("hbase.zookeeper.quorum", "zookeeper1,zookeeper2,zookeeper3");
        DataFrame personDF = null;
        try {
            String tableName = "test";
            // TableInputFormat类主要是构造HTable对象和Scan对象，
            conf.set(TableInputFormat.INPUT_TABLE, tableName);// Job parameter
            // that
            // specifies the
            // input table.
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());// 设置扫描的列
            conf.set(TableInputFormat.SCAN, ScanToString); // Base-64 encoded
            // scanner.
            // 从数据库中获取查询内容生成RDD
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                    ImmutableBytesWritable.class, Result.class);
            JavaRDD<String> data = myRDD.flatMap(new FlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Iterable<String> call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
                    String row_key = Bytes.toString(t._2.getRow());
                    // Bytes.toString(t._2.getRow());Integer.parseInt(numString.trim())
                    String COLLECTDATA = Bytes.toString(t._2.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("COLLECTTIME")));
                    return Arrays.asList(COLLECTDATA);
                }
            });
            JavaRDD<Row> rows = data.map(new Function<String, Row>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Row call(String line) {
                    String[] splited = line.split(",");
                    return RowFactory.create(splited[0]);
                }
            });
            for (Row rdd1 : rows.collect()){
                System.out.println(rdd1);
            }
            List<StructField> fields = new ArrayList<>();
            fields.add(DataTypes.createStructField("value", DataTypes.StringType, true));
            StructType schema = DataTypes.createStructType(fields);
            DataFrame resultDF = sqlContext.createDataFrame(rows,schema);
            resultDF.show();
            DataFrame  aaDataFrame = Normalization. MinMax(sc, sqlContext,resultDF);
            aaDataFrame.show();
            DataFrame  aaDataFrame1 = Normalization. zscore(sc,sqlContext, resultDF);
            aaDataFrame1.show();
            return personDF;
        } catch (Exception e) {
            e.printStackTrace();
        }
        sc.stop();
        return personDF;
    }
    public static void main(String args[]) {
        normalization_test ttHbase = new normalization_test();
        ttHbase.testFrame();
    }
}