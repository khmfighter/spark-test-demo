package cn.nuecloud.bigdata.dasuan.visualize;

import cn.nuecloud.bigdata.dasuan.analysis.stat.DSCorRelation;
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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
import java.util.*;

/**
 * Created by Administrator on 2016/10/24.
 */
public class correlation_test implements java.io.Serializable{

    private static final long serialVersionUID = 1L;

    public DataFrame testFrame() {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.log4j").setLevel(Level.OFF);
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
        Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF);
        Logger.getLogger("org.apache.spark.Logging").setLevel(Level.OFF);
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2");
        SparkConf sparkConf = new SparkConf().setAppName("Regression").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);
        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        conf.set("hbase.zookeeper.quorum", "zookeeper1,zookeeper2,zookeeper3");
        DataFrame personDF = null;
        try {
            String tableName = "dungou";
            // TableInputFormat类主要是构造HTable对象和Scan对象，
            conf.set(TableInputFormat.INPUT_TABLE, tableName);// Job parameter
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());// 设置扫描的列
            conf.set(TableInputFormat.SCAN, ScanToString); // Base-64 encoded
            // scanner.
            // 从数据库中获取查询内容生成RDD
            JavaPairRDD<ImmutableBytesWritable, Result> myRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
                    ImmutableBytesWritable.class, Result.class);
            String [] source={"No_1_Current","No_4_Current","No_12_Current","No_2_Current","No_17_Current","No_13_Current","No_8_Current","No_19_Current","No_10_Current","No_3_Current","No_11_Current","No_9_Current","No_18_Current","No_7_Current","No_14_Current"};
            int  source_length=source.length;
            double[][] result=new double[source_length][source_length];
            for (int i=0;i<source_length-1;i++){
                for (int j=i+1;j<source_length;j++){
                    final String key1 = source[i];
                    final String value1 = source[j];
                    JavaRDD<String> data = myRDD.flatMap(new FlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String>() {
                        private static final long serialVersionUID = 1L;
                        @Override
                        public Iterable<String> call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
                            String row_key = Bytes.toString(t._2.getRow());
                            // Bytes.toString(t._2.getRow());Integer.parseInt(numString.trim())
                            String key2 = Bytes.toString(t._2.getValue(Bytes.toBytes("cf1"), Bytes.toBytes(key1)));
                            String value2 = Bytes
                                    .toString(t._2.getValue(Bytes.toBytes("cf1"), Bytes.toBytes(value1)));
                            return Arrays.asList(key2 + "," + value2);
                        }
                    });
                    JavaRDD<Row> rows = data.map(new Function<String, Row>() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Row call(String line) {
                            String[] splited = line.split(",");
                            return RowFactory.create(Double.valueOf(splited[0]), Double.valueOf(splited[1]));
                        }
                    });
                    List<StructField> structFields = new ArrayList<StructField>();
                    structFields.add(DataTypes.createStructField(key1, DataTypes.DoubleType, true));
                    structFields.add(DataTypes.createStructField(value1, DataTypes.DoubleType, true));
                    StructType structType = DataTypes.createStructType(structFields);
                    personDF = sqlContext.createDataFrame(rows, structType);
              //      personDF.show();
                    DSCorRelation kms = new DSCorRelation();
                    DataFrame sourceDSX = personDF.select(key1).limit(20);
                    DataFrame sourceDSY = personDF.select(value1).limit(20);
                    //     sourceDSX.show();
                    //      sourceDSY.show();
                    String new_key1=key1.substring(0,key1.length()-8);
                    String new_value1=value1.substring(0,value1.length()-8);
                    double  kms1 = kms.computOne(sourceDSX,sourceDSY);
                                //定义一个float类型的2维数组
                    result[i][j]=kms1;
                    result[j][i]=kms1;
                }

            }
            double sum=0;
            double[] sum_c=new double[source.length];
            for (int i = 0; i < result.length; i++) {
                for (int j = 0; j < result[i].length; j++) {
                    //循环遍历数组中的每个元素
                    //       a[i][j]='*';
                    //初始化数组内容
                     sum += result[i][j];
               //     System.out.println(i+","+j+","+result[i][j]+","+sum);
                    //将数组中的元素输出
                }
                String s1 = String.format("%.2f", sum);
                sum_c[i]=Double.valueOf(s1);
            }
         for (int i = 0; i < result.length; i++) {
                for (int j = 0; j < result[i].length; j++) {
                    if(i!=j) {
                        String r = "[" + "\"" + source[i].substring(0,source[i].length()-8) + "\"" + "," + sum_c[i] + "," + "\"" + source[j].substring(0,source[j].length()-8) + "\"" + "," +  result[i][j] + "]" + ",";
                        System.out.println(r);
                    }
                }
                }
//
//            for (final String key : map.keySet()) {
//                final String key1 = key;
//                final String value1 = map.get(key);
//
//                JavaRDD<String> data = myRDD.flatMap(new FlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String>() {
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    public Iterable<String> call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
//                        String row_key = Bytes.toString(t._2.getRow());
//                        // Bytes.toString(t._2.getRow());Integer.parseInt(numString.trim())
//                        String key2 = Bytes.toString(t._2.getValue(Bytes.toBytes("cf1"), Bytes.toBytes(key1)));
//                        String value2 = Bytes
//                                .toString(t._2.getValue(Bytes.toBytes("cf1"), Bytes.toBytes(value1)));
//                        return Arrays.asList(key2 + "," + value2);
//                    }
//                });
//                JavaRDD<Row> rows = data.map(new Function<String, Row>() {
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    public Row call(String line) {
//                        String[] splited = line.split(",");
//                        return RowFactory.create(Double.valueOf(splited[0]), Double.valueOf(splited[1]));
//                    }
//                });
//                List<StructField> structFields = new ArrayList<StructField>();
//                structFields.add(DataTypes.createStructField(key1, DataTypes.DoubleType, true));
//                structFields.add(DataTypes.createStructField(value1, DataTypes.DoubleType, true));
//                StructType structType = DataTypes.createStructType(structFields);
//                personDF = sqlContext.createDataFrame(rows, structType);
// //                    personDF.show();
//                  DataFrame sourceDSX = personDF.select(key1).limit(2000);
//                   DataFrame sourceDSY = personDF.select(value1).limit(2000);
//           //     sourceDSX.show();
//          //      sourceDSY.show();
//                DSCorRelation kms = new DSCorRelation();
//                double  kms1 = kms.computOne(sourceDSX,sourceDSY);
//                String aaa= key1+"|"+value1;
//                System.out.println("++++++++++++++++++");
//                System.out.println(aaa);
//                System.out.println(+kms1);
//                System.out.println("wwwwwwwwwwwwwwwww");
//            }
            return personDF;
        } catch (Exception e) {
            e.printStackTrace();
        }
        sc.stop();
        return personDF;
    }
    public static void main(String args[]) {
        correlation_test ttHbase = new correlation_test();
        ttHbase.testFrame();
    }
}

