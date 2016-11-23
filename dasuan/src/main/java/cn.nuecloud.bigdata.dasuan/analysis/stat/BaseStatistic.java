package cn.nuecloud.bigdata.dasuan.analysis.stat;

import cn.neucloud.bigdata.taskserver.bean.Task;
import cn.neucloud.bigdata.taskserver.bean.TaskLog;
import cn.neucloud.bigdata.taskserver.bean.TaskLogSchema;
import cn.neucloud.bigdata.taskserver.service.TaskLogSchemaService;
import cn.neucloud.bigdata.taskserver.service.TaskLogService;
import cn.neucloud.bigdata.taskserver.service.TaskService;
import cn.nuecloud.bigdata.dasuan.exception.MyException;
import cn.nuecloud.bigdata.dasuan.server.Run;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @example{{{
 * SparkConf conf = new SparkConf().setAppName("test").setMaster("local");;
 * JavaSparkContext jsc = new JavaSparkContext(conf);
 * SQLContext sqlContext = new SQLContext(jsc);
 * DataFrame dt = sqlContext.read().json("data/testFile/test.json");
 *  try {
 * DataFrame dataFrame = BaseStatistic.byIntervalWindow(jsc, dt, 2, 1,5000l);;
 * dataFrame.printSchema();
 * dataFrame.show();
 * List<Row> res = dataFrame.javaRDD().collect();
 * for(int i=0;i<res.size();i++){
 *   System.out.println(res.get(i).toString());
 * }
 * }catch(MyException e){
 *  e.printStackTrace();
 *  }
 * }}}
 */

/**
 * 计算类
 * 求每个时间段内数据的最大值，最小值，平均值
 * @version Neucloud2016 2016-08-09
 * @author xuhaifeng
 */
public final class BaseStatistic{

    /**
     * 按照时间间隔求最大值，最小值，平均值
     * @param jsc
     * @param dataSource 源数据
     * @param timeColumn 时间列
     * @param valueColumn 所需求值的列
     * @return
     * @exception
     */
    public static DataFrame byIntervalWindow(JavaSparkContext jsc, DataFrame dataSource, int timeColumn,
                                             int valueColumn, Long interval) throws MyException{
        if(timeColumn < 0 || valueColumn < 0){
            throw new MyException("input value illegal");
        }
        JavaRDD<Row> dataRDD = dataSource.javaRDD();
        final Broadcast<Integer> timeColumnFactor = jsc.broadcast(timeColumn);
        final Broadcast<Integer> valueColumnFactor = jsc.broadcast(valueColumn);
        final Broadcast<Long> intervalFactor = jsc.broadcast(interval);
        String sTime = dataRDD.take(1).get(0).getString(timeColumn);
        final Broadcast<String> startTimeFactor = jsc.broadcast(sTime);
        JavaPairRDD<Long,String> groupID2ValueRDD = dataRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, Long, String>() {
            @Override
            public Iterable<Tuple2<Long, String>> call(Iterator<Row> iterator) throws Exception {
                List<Tuple2<Long,String>> list = new ArrayList<>();
                String startTime = startTimeFactor.value();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                while (iterator.hasNext()){
                    Row curRow = iterator.next();
                    String endTime = curRow.getString(timeColumnFactor.getValue());
                    long between = (dateFormat.parse(endTime).getTime() - dateFormat.parse(startTime).getTime())
                            / intervalFactor.getValue();
                    list.add(new Tuple2<>(between,curRow.getString(valueColumnFactor.getValue())+"_"+endTime));
                }
                return list;
            }
        });
        JavaPairRDD<Long,Iterable<String>> groupPairsRDD = groupID2ValueRDD.groupByKey();
        //计算:生成每个时间间隔内的最大值，最小值，平均值
        JavaRDD<Row> resultRDD = groupPairsRDD.map(new Function<Tuple2<Long, Iterable<String>>, Row>() {
            @Override
            public Row call(Tuple2<Long, Iterable<String>> tuple) throws Exception {
                Iterator<String> iterator = tuple._2.iterator();
                double minValue = 0.f;
                double maxValue = 0.f;
                double meanValue;
                double sum = 0.f;
                int size = 0;
                long startTime = 0;
                long endTime = 0;
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                while(iterator.hasNext()){
                    size++;
                    String strValue = iterator.next();
                    double curValue = Double.valueOf(strValue.split("_")[0]);
                    long curTime = dateFormat.parse(strValue.split("_")[1]).getTime();
                    sum+=curValue;
                    if(1 == size){
                        startTime = curTime;
                        endTime = curTime;
                        minValue = curValue;
                        maxValue = curValue;
                        continue;
                    }
                    if (curValue > maxValue){
                        maxValue = curValue;
                    }
                    if (curValue < minValue){
                        minValue = curValue;
                    }
                    if (curTime < startTime){
                        startTime = curTime;
                    }
                    if(curTime > endTime){
                        endTime = curTime;
                    }
                }
                Date dStartTime = new Date(startTime);
                Date dEndTime = new Date(endTime);
                DecimalFormat df   = new   java.text.DecimalFormat("#.00");
                meanValue = Double.valueOf(df.format(sum /size));
                return RowFactory.create(dateFormat.format(dStartTime) + "--"+dateFormat.format(dEndTime),minValue,maxValue,meanValue);
            }});
        String schemaString = "time min max mean";
        List<StructField> fields = new ArrayList<>();
        String[] schemaArray = schemaString.split(" ");
        for(int i=0;i<schemaArray.length;i++) {
            if(0 == i){
                fields.add(DataTypes.createStructField(schemaArray[i], DataTypes.StringType, true));
            }else{
                fields.add(DataTypes.createStructField(schemaArray[i], DataTypes.DoubleType, true));
            }
        }
        StructType schema = DataTypes.createStructType(fields);
        SQLContext sqlContext = new SQLContext(jsc);
        DataFrame resultDF = sqlContext.createDataFrame(resultRDD,schema);
        resultDF.registerTempTable("stat");
        DataFrame temp = sqlContext.sql("select * from stat order by time");

        return temp;

    }

    /**
     *  将数据存入postgres
     * @param df 数据源
     * @throws Exception
     */
    public static void visualize(DataFrame df) throws Exception{
        JavaRDD<Row> rowJavaRDD = df.javaRDD();
        Run run = Run.getInstance();
        TaskService taskService = run.getBean(TaskService.class);
        final TaskLogService taskLogService = run.getBean(TaskLogService.class);
        final int id = taskService.addGetId(
                Task.builder()
                        .algorithmId(5)
                        .createTime(new Date())
                        .description("baseStatic").build());
        int partitions = rowJavaRDD.getNumPartitions();
        TaskLogSchemaService taskLogSchemaService = run.getBean(TaskLogSchemaService.class);
        TaskLogSchema schema = TaskLogSchema.builder().taskId(id).build();
        schema.setSchema(new String[]{"time", "min","max","mean"});
        taskLogSchemaService.add(schema);
        List<TaskLog> taskLogs = new ArrayList<>();
        List<Row> list;
        for (int i = 0; i < partitions; i++) {
            list = rowJavaRDD.collectPartitions(new int[]{i})[0];
            for (int j = 0; j < list.size(); j++) {
                Row row = list.get(j);
                TaskLog taskLog = TaskLog.builder().taskId(id).build();
                String[] arr = row.toString().substring(1,row.toString().length()-1).split(",");
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
    }
}

