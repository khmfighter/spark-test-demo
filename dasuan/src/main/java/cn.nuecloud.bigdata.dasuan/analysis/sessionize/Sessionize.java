package cn.nuecloud.bigdata.dasuan.analysis.sessionize;

import cn.neucloud.bigdata.taskserver.bean.Task;
import cn.neucloud.bigdata.taskserver.bean.TaskLog;
import cn.neucloud.bigdata.taskserver.bean.TaskLogSchema;
import cn.neucloud.bigdata.taskserver.service.TaskLogSchemaService;
import cn.neucloud.bigdata.taskserver.service.TaskLogService;
import cn.neucloud.bigdata.taskserver.service.TaskService;
import cn.nuecloud.bigdata.dasuan.exception.MyException;
import cn.nuecloud.bigdata.dasuan.server.Run;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;

import java.util.*;

/**
 * @example{{{
 * SparkConf conf new SparkConf().setAppName("test").setMaster("local[20]").set("spark.default.parallelism","1");
 * JavaSparkContext jsc new JavaSparkContext(conf);
 * SQLContext sqlContext = new SQLContext(jsc);
 * DataFrame dt = sqlContext.read().json("data/testFile/test.json");
 * try {
 * DataFrame dataFrame = Sessionize.byInterval(jsc, dt, 3000L, 2L, 20L);
 * } catch (MyException e) {
 * e.printStackTrace();
 * }
 * dataFrame.printSchema();
 * dataFrame.show();
 * List<Row> list = dataFrame.toJavaRDD().collect();
 * for(int i=0;i<list.size();i++){
 * System.out.println(list.get(i));
 * }
 * }}}
 */

/**
 * 按照时间，维度值增量划分session
 *
 * @version Neucloud2016 2016-09-05
 * @auctor xuhaifeng
 */
public final class Sessionize{

    /**
     * 按照时间间隔划分session
     *
     * @param jsc
     * @param df          数据源
     * @param interval    间隔的毫秒数
     * @param timeColumn  时间列的索引
     * @param maxLength   每个session最大长度
     * @return
     * @exception
     */
    public static DataFrame byInterval(JavaSparkContext jsc, DataFrame df, Long interval,
                                               Long timeColumn, Long maxLength) throws MyException {
        if (interval <= 0 || timeColumn < 0 || maxLength < 0) {
            throw new MyException("Illegal input value");
        }
        JavaRDD<Row> sourceData = df.javaRDD();
        final Broadcast<Long> timeFactor = jsc.broadcast(interval);
        final Broadcast<Long> timeColumnFactor = jsc.broadcast(timeColumn);
        final Broadcast<Long> limitLengthFactor = jsc.broadcast(maxLength);
        JavaRDD<Row> sessionRDD = sourceData.mapPartitions(new FlatMapFunction<Iterator<Row>, Row>() {
            @Override
            public Iterable<Row> call(Iterator<Row> iterator) throws Exception {
                Long sessionID = 0L;
                long sessionLen = 0;
                String startTime = null;
                long between;
                List<Row> retList = new ArrayList<>();
                while (iterator.hasNext()) {
                    Row curRow = iterator.next();
                    String curTime = curRow.get(timeColumnFactor.getValue().intValue()).toString();
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    //初始化每个session的起始时间
                    if (null == startTime) {
                        startTime = curTime;
                        sessionID = dateFormat.parse(startTime).getTime();
                    }
                    //获取时间间隔
                    between = dateFormat.parse(curTime).getTime() - dateFormat.parse(startTime).getTime();
                    if ((between > timeFactor.getValue())) {
                        sessionID = dateFormat.parse(curTime).getTime();
                        if (sessionLen > limitLengthFactor.getValue()) {
                            throw new MyException("session length more than the input max length");
                        }
                        startTime = curTime;
                        sessionLen = 1L;
                    } else {
                        sessionLen++;
                    }
                    retList.add(RowFactory.create(sessionID.toString(),curRow.toString().substring(1,curRow.toString().length()-1)));
                }
                return retList;
            }
        });
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("sessionID", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("value", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        SQLContext sqlContext = new SQLContext(jsc);
        DataFrame resultDF = sqlContext.createDataFrame(sessionRDD,schema);

        return resultDF;
    }

    /**
     * 按照时间增量划分session
     *
     * @param jsc
     * @param df          数据源
     * @param range       时间增量值
     * @param timeColumn  时间列索引
     * @param maxLength  每个session最大长度
     * @return
     * @throws
     */
    public static DataFrame byTimeRange(JavaSparkContext jsc, DataFrame df, Long range, Long timeColumn,
                                                Long maxLength) throws MyException {
        if (range <= 0 || timeColumn < 0 || maxLength < 0) {
            throw new MyException("Illegal input value");
        }
        JavaRDD<Row> sourceData = df.javaRDD();
        final Broadcast<Long> rangeFactor = jsc.broadcast(range);
        final Broadcast<Long> timeColumnFactor = jsc.broadcast(timeColumn);
        final Broadcast<Long> limitLengthFactor = jsc.broadcast(maxLength);
        JavaRDD<Row> sessionRDD = sourceData.mapPartitions(new FlatMapFunction<Iterator<Row>,Row>() {
            @Override
            public Iterable<Row> call(Iterator<Row> iterator) throws Exception {
                Long sessionID = 0L;
                String preTime = null;
                long sessionLen = 0;
                long between;
                List<Row> retList = new ArrayList<>();
                while (iterator.hasNext()) {
                    Row curRow = iterator.next();
                    String curTime = curRow.get(timeColumnFactor.getValue().intValue()).toString();
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    if (null == preTime) {
                        preTime = curTime;
                        sessionID = dateFormat.parse(preTime).getTime();
                    }
                    between = dateFormat.parse(curTime).getTime() - dateFormat.parse(preTime).getTime();
                    if (between > rangeFactor.getValue()) {
                        sessionID = dateFormat.parse(curTime).getTime();
                        if (sessionLen > limitLengthFactor.getValue()) {
                            throw new MyException("session length more than the input max length");
                        }
                        sessionLen = 1L;
                    } else {
                        sessionLen++;
                    }
                    preTime = curTime;
                    retList.add(RowFactory.create(sessionID.toString(),curRow.toString().substring(1,curRow.toString().length()-1)));
                }
                return retList;
            }
        });
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("sessionID", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("value", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        SQLContext sqlContext = new SQLContext(jsc);
        DataFrame resultDF = sqlContext.createDataFrame(sessionRDD,schema);

        return resultDF;
    }

    /**
     * 按照维度值变化划分session
     *
     * @param jsc
     * @param df          数据源
     * @param range       维度值变化
     * @param valueColumn 维度列索引
     * @param maxLength   每个session最大长度
     * @return
     * @throws
     */
    public static DataFrame byValueRange(JavaSparkContext jsc, DataFrame df, double range, Long valueColumn,
                                               Long maxLength) throws MyException {
        if (range <= 0 || valueColumn < 0 || maxLength < 0) {
            throw new MyException("Illegal input value");
        }
        JavaRDD<Row> sourceData = df.javaRDD();
        final Broadcast<Double> rangeFactor = jsc.broadcast(range);
        final Broadcast<Long> dimColumnFactor = jsc.broadcast(valueColumn);
        final Broadcast<Long> limitLengthFactor = jsc.broadcast(maxLength);
        JavaRDD<Row> sessionRDD = sourceData.mapPartitions(new FlatMapFunction<Iterator<Row>,Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<Row> call(Iterator<Row> iterator) throws Exception {
                String sessionID = null;
                double preDim = Double.MAX_VALUE;
                long sessionLen = 0;
                List<Row> retList = new ArrayList<>();
                while (iterator.hasNext()) {
                    Row curRow = iterator.next();
                    double curDim = Double.valueOf(curRow.get(dimColumnFactor.getValue().intValue()).toString());
                    if (Double.MAX_VALUE == preDim) {
                        preDim = curDim;
                        sessionID = UUID.randomUUID().toString();
                    }
                    if (Math.abs(preDim - curDim) > rangeFactor.getValue()) {
                        sessionID = UUID.randomUUID().toString();
                        if (sessionLen > limitLengthFactor.getValue()) {
                            throw new MyException("session length more than the input max length");
                        }
                        sessionLen = 1L;
                    } else {
                        sessionLen++;
                    }
                    preDim = curDim;
                    retList.add(RowFactory.create(sessionID,curRow.toString().substring(1,curRow.toString().length()-1)));
                }
                return retList;
            }
        });
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("sessionID", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("value", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        SQLContext sqlContext = new SQLContext(jsc);
        DataFrame resultDF = sqlContext.createDataFrame(sessionRDD,schema);

        return resultDF;
    }

    /**
     * 按照时间间隔和维度值变化划分session
     *
     * @param jsc
     * @param df          数据源
     * @param interval    每个session最大时间间隔
     * @param timeColumn  时间列索引
     * @param range       每个session中，维度值变化
     * @param valueColumn 维度列索引
     * @param maxLength   每个session的最大长度
     * @return
     * @throws
     */
    public static DataFrame byIntervalAndValueRange(JavaSparkContext jsc, DataFrame df, Long interval, Long timeColumn,
                                                          double range, Long valueColumn, Long maxLength) throws MyException {
        if (interval <= 0 || timeColumn < 0 || range <= 0 || valueColumn < 0 || maxLength <= 0) {
            throw new MyException("Illegal input value");
        }
        JavaRDD<Row> sourceData = df.javaRDD();
        final Broadcast<Long> timeFactor = jsc.broadcast(interval);
        final Broadcast<Long> timeColumnFactor = jsc.broadcast(timeColumn);
        final Broadcast<Double> rangeFactor = jsc.broadcast(range);
        final Broadcast<Long> dimColumnFactor = jsc.broadcast(valueColumn);
        final Broadcast<Long> limitLengthFactor = jsc.broadcast(maxLength);
        JavaRDD<Row> sessionRDD = sourceData.mapPartitions(new FlatMapFunction<Iterator<Row>,Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<Row> call(Iterator<Row> iterator) throws Exception {
                Long sessionID = 0L;
                String startTime = null;
                long sessionLen = 0L;
                long between;
                double span;
                double preDim = Double.MAX_VALUE;
                List<Row> retList = new ArrayList<>();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                while (iterator.hasNext()) {
                    Row curRow = iterator.next();
                    String curTime = curRow.get(timeColumnFactor.getValue().intValue()).toString();
                    double curDim = Double.valueOf(curRow.get(dimColumnFactor.getValue().intValue()).toString());
                    if (null == startTime) {
                        startTime = curTime;
                        sessionID = dateFormat.parse(startTime).getTime();
                    }
                    if (preDim == Double.MAX_VALUE) {
                        preDim = curDim;
                    }
                    between = dateFormat.parse(curTime).getTime() - dateFormat.parse(startTime).getTime();
                    span = Math.abs(curDim - preDim);
                    if (between > timeFactor.getValue() || span > rangeFactor.getValue()) {
                        sessionID = dateFormat.parse(curTime).getTime();
                        if (sessionLen > limitLengthFactor.getValue()) {
                            throw new MyException("session length more than the input max length");
                        }
                        startTime = curTime;
                        sessionLen = 1L;
                    } else {
                        sessionLen++;
                    }
                    preDim = curDim;
                    retList.add(RowFactory.create(sessionID.toString(),curRow.toString().substring(1,curRow.toString().length()-1)));
                }
                return retList;
            }
        });
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("sessionID", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("value", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        SQLContext sqlContext = new SQLContext(jsc);
        DataFrame resultDF = sqlContext.createDataFrame(sessionRDD,schema);

        return resultDF;
    }

    /**
     * 按照时间间隔和时间增量划分session
     *
     * @param jsc
     * @param df          数据源
     * @param interval    每个session最大时间间隔
     * @param range       每个session中，时间增量
     * @param timeColumn  时间列索引
     * @param maxLength   每个session最大长度
     * @return
     * @throws
     */
    public static DataFrame byIntervalAndTimeRange(JavaSparkContext jsc, DataFrame df, Long interval, Long range,
                                                       Long timeColumn, Long maxLength) throws MyException {
        if (interval <= 0 || range <= 0 || timeColumn < 0 || maxLength <= 0) {
            throw new MyException("Illegal input value");
        }
        JavaRDD<Row> sourceData = df.javaRDD();
        final Broadcast<Long> secondFactor = jsc.broadcast(interval);
        final Broadcast<Long> rangeFactor = jsc.broadcast(range);
        final Broadcast<Long> timeColumnFactor = jsc.broadcast(timeColumn);
        final Broadcast<Long> limitColumnFactor = jsc.broadcast(maxLength);
        JavaRDD<Row> sessionRDD = sourceData.mapPartitions(new FlatMapFunction<Iterator<Row>,Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<Row> call(Iterator<Row> iterator) throws Exception {
                Long sessionID = 0l;
                String startTime = null;
                String preTime = null;
                long between;
                long span;
                long sessionLen = 0l;
                List<Row> retList = new ArrayList<>();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                while (iterator.hasNext()) {
                    Row curRow = iterator.next();
                    String curTime = curRow.getString(timeColumnFactor.getValue().intValue());
                    if (null == startTime) {
                        startTime = curTime;
                        sessionID = dateFormat.parse(startTime).getTime();
                    }
                    if (null == preTime) {
                        preTime = curTime;
                    }
                    between = (dateFormat.parse(curTime).getTime() - dateFormat.parse(startTime).getTime());
                    span = (dateFormat.parse(curTime).getTime() - dateFormat.parse(preTime).getTime());
                    if ((between > secondFactor.getValue()) || span > rangeFactor.getValue()) {
                        sessionID = dateFormat.parse(curTime).getTime();
                        if (sessionLen > limitColumnFactor.getValue()) {
                            throw new MyException("session length more than the input max length");
                        }
                        startTime = curTime;
                        sessionLen = 1L;
                    } else {
                        sessionLen++;
                    }
                    preTime = curTime;
                    retList.add(RowFactory.create(sessionID.toString(),curRow.toString().substring(1,curRow.toString().length()-1)));
                }
                return retList;
            }
        });
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("sessionID", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("value", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        SQLContext sqlContext = new SQLContext(jsc);
        DataFrame resultDF = sqlContext.createDataFrame(sessionRDD,schema);

        return resultDF;
    }

    public static void visualize(DataFrame df) throws Exception{
        JavaRDD<Row> rowJavaRDD = df.javaRDD();
        Run run = Run.getInstance();
        TaskService taskService = run.getBean(TaskService.class);
        final TaskLogService taskLogService = run.getBean(TaskLogService.class);
        final int id = taskService.addGetId(
                Task.builder()
                        .algorithmId(2)
                        .createTime(new Date())
                        .description("sessionize").build());
        int partitions = rowJavaRDD.getNumPartitions();
        TaskLogSchemaService taskLogSchemaService = run.getBean(TaskLogSchemaService.class);
        TaskLogSchema schema = TaskLogSchema.builder().taskId(id).build();
        schema.setSchema(new String[]{"sessionID","time","value"});
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
