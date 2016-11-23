package cn.nuecloud.bigdata.dasuan.visualize;

/**
 * Created by Administrator on 2016/10/19.
 */

import cn.neucloud.bigdata.taskserver.bean.Task;
import cn.neucloud.bigdata.taskserver.bean.TaskLog;
import cn.neucloud.bigdata.taskserver.bean.TaskLogSchema;
import cn.neucloud.bigdata.taskserver.service.TaskLogSchemaService;
import cn.neucloud.bigdata.taskserver.service.TaskLogService;
import cn.neucloud.bigdata.taskserver.service.TaskService;
import cn.nuecloud.bigdata.dasuan.analysis.sessionize.Sessionize;
import cn.nuecloud.bigdata.dasuan.server.Run;
import org.apache.derby.impl.sql.catalog.SYSTABLEPERMSRowFactory;
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
import org.apache.spark.api.java.function.VoidFunction;
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
import java.util.Date;
import java.util.List;

/**
 * Created by wangqingda on 2016/10/13.
 */
public class session_test implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    public DataFrame testFrame() {
        //System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2");
        SparkConf sparkConf = new SparkConf().setAppName("Regression").setMaster("local").set("spark.default.parallelism", "1");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);
        Configuration conf = HBaseConfiguration.create();
        Scan scan = new Scan();
        conf.set("hbase.zookeeper.quorum", "zookeeper1,zookeeper2,zookeeper3");
        DataFrame personDF1 = null;
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
                    String Rotate_Speed = Bytes
                            .toString(t._2.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("Rotate_Speed")));
                    return Arrays.asList(Time + "," + Rotate_Speed);
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
            structFields.add(DataTypes.createStructField("Rotate_Speed", DataTypes.StringType, true));
            StructType structType = DataTypes.createStructType(structFields);
            personDF1 = sqlContext.createDataFrame(rows, structType);
            personDF1.show();
            personDF1.registerTempTable("stat");
            DataFrame personDF = sqlContext.sql("select * from stat order by Time");
            personDF.javaRDD().foreach(new VoidFunction<Row>() {
                @Override
                public void call(Row row) throws Exception {
                    System.out.println(row.toString());
                }
            });
            DataFrame reJavaRDD = Sessionize.byInterval(sc, personDF, 100000l, 0l, 1000l);
            reJavaRDD.show();
            Run run = Run.getInstance();
            TaskService taskService = run.getBean(TaskService.class);
            final TaskLogService taskLogService = run.getBean(TaskLogService.class);
            final int id = taskService.addGetId(
                    Task.builder()
                            .algorithmId(2)
                            .createTime(new Date())
                            .description("Sessionize").build());// 为0时表示失败
            int partitions = reJavaRDD.javaRDD().getNumPartitions();
            List<TaskLog> taskLogs = new ArrayList<>();
            List<Row> data1;
            for(int i=0; i < partitions; i++){
                data1  = reJavaRDD.javaRDD().collectPartitions(new int[]{i})[0];
                for (int j = 0; j < data1.size(); j++) {
                    Row row = data1.get(j);
                    TaskLog taskLog = TaskLog.builder().taskId(id).build();
                    String[] arr = row.toString().replaceAll("[\\[\\]]", "").split(",");
                    taskLog.setRaw(arr);
                    taskLogs.add(taskLog);
                    if (taskLogs.size() >= 1000) {
                        taskLogService.add(taskLogs);
                        taskLogs = new ArrayList<>();
                    }
                }
                if (taskLogs.size() > 0) {
                    taskLogService.add(taskLogs);
                }
            }
            TaskLogSchemaService taskLogSchemaService = run.getBean(TaskLogSchemaService.class);
            TaskLogSchema schema = TaskLogSchema.builder().taskId(id).build();
            schema.setSchema(new String[]{"sessionID", "Time", "Rotate_Speed"});
            taskLogSchemaService.add(schema);
            return personDF;
        } catch (Exception e) {
            e.printStackTrace();
        }
        sc.close();
        sc.stop();
        return personDF1;
    }

    public static void main(String args[]) {
        session_test ttHbase = new session_test();
        ttHbase.testFrame();
        System.out.println("done!");
    }
}

