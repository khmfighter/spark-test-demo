package cn.nuecloud.bigdata.dasuan.visualize;

import cn.nuecloud.bigdata.dasuan.analysis.stat.BaseStatistic;
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
 * Created by Administrator on 2016/10/20.
 */
public class statistic  implements java.io.Serializable{
    private static final long serialVersionUID = 1L;

    public DataFrame testFrame() {
        SparkConf sparkConf = new SparkConf().setAppName("static").setMaster("local");
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
                    String Time = Bytes.toString(t._2.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("Time")));
                    String Move_Speed = Bytes
                            .toString(t._2.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("Move_Speed")));
                    return Arrays.asList(Time + "," + Move_Speed);
                }
            });
            JavaRDD<Row> rows = data.map(new Function<String, Row>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Row call(String line) {
                    String[] splited = line.split(",");
                    return RowFactory.create(splited[0], splited[1]);
                }
            });
            List<StructField> structFields = new ArrayList<StructField>();
            structFields.add(DataTypes.createStructField("Time", DataTypes.StringType, true));
            structFields.add(DataTypes.createStructField("Move_Speed", DataTypes.StringType, true));
            StructType structType = DataTypes.createStructType(structFields);
            personDF = sqlContext.createDataFrame(rows, structType);
      //             personDF.show();
            DataFrame reJavaRDD = BaseStatistic.byIntervalWindow(sc, personDF, 0, 1,20000l);
            reJavaRDD.show();
            JavaRDD<String> result = reJavaRDD.toJavaRDD().map(new Function<Row, String>() {
                public String call(Row line) {
                    return line.toString();
                }
            });
            List<String> list = result.collect();
            for (int i=0;i<2000;i++) {
                String str =list.get(i).replaceAll("[\\[\\]]", "");
                String[] aa=str.split(",");
                String []bb=aa[0].split("--");
                String cc=bb[0]+","+aa[1]+","+aa[2]+","+aa[3];
       //         System.out.println(str);
                    System.out.println(cc);
            }
//            Connection con = null;
//            Class.forName("org.postgresql.Driver");
//            con = DriverManager.getConnection("jdbc:postgresql://172.24.4.215:5432/test", "pgxl", "123");
//            Statement st = con.createStatement();
//            String sel_id = "select id  from t_algorithm where name='Sessionize';";
//            System.out.println(sel_id);
//            ResultSet rs = st.executeQuery(sel_id);
//            String rid = null;
//            int rowCount = 0;
//            while (rs.next()) {
//                rowCount++;
//                rid = rs.getString(1);
//                System.out.println(rid);
//            }
//            for (String test : result.collect()) {
//                String str = test.replaceAll("[\\[\\]]", "");
//                String[] arrStrings = str.split(",");
//                String time = new String(arrStrings[0]);
//                String Rotate_Speed = new String(arrStrings[1]);
//                String seeiond_id = new String(arrStrings[2]);// 获取编号
//                String session_result=time+"|"+Rotate_Speed+"|"+seeiond_id;
//                String ins_sql = "insert  into  t_task_log (task_id,raw) VALUES ('" + rid + "','" + session_result + "'); ";
//                st.executeUpdate(ins_sql);
//            }
//            String schema="time"+"|"+"Rotate_Speed"+"|"+"seeiond_id";
//            String sql_schema = "insert  into  t_task_log_schema (task_id,shema) VALUES ('" + rid + "','" + schema + "'); ";
//            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            String ss = df.format(new Date());
//            String sql = "insert  into  t_task (algorithm_id,description,createtime ) VALUES ('" + rid + "','聚类','"
//                    + ss + "'); ";
//            //	System.out.println(sql);
//            st.executeUpdate(sql);
            return personDF;
        } catch (Exception e) {
            e.printStackTrace();
        }
        sc.close();
        return personDF;
    }
    public static void main(String args[]) {
        statistic ttHbase = new statistic();
        ttHbase.testFrame();
    }
}


