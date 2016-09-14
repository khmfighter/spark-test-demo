package cn.test.lib.sessionize;

import cn.test.lib.exception.MyException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;
import utils.Spark_Log;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * 按照时间，维度值增量划分session
 * @version Neucloud2016 2016-09-05
 * @auctor xuhaifeng
 */
public class Sessionize {

    public static void main(String[] args){
        Spark_Log.apply();
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[8]");//.setMaster("spark://hadoop1:7077");//.setMaster("local[8]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        // /data/tools/spark-1.6.2-bin-hadoop2.6  data/testFile
        DataFrame df = sqlContext.read().json("data/test.json");
        JavaRDD<Row> rdd=null;
        try {
            rdd = timeSpacingSession(jsc, df, 3000L, 2L, 20L);
        }catch(MyException e){
            e.printStackTrace();
        }
        rdd.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.toString());
            }
        });
    }

    /**
     * 按照时间间隔划分sessize
     * @param jsc
     * @param df 数据源
     * @param second 间隔的毫秒数 3000L
     * @param timeColumn 时间列的索引 2L ？？？ 为啥不用int（32）
     * @param limitLength 每个session最大长度 20L
     * @return
     * @throws MyException
     */
    public  static JavaRDD<Row> timeSpacingSession(JavaSparkContext jsc, DataFrame df, final Long second,
                                                   final Long timeColumn, final Long limitLength) throws MyException {

        if(second <= 0 || timeColumn < 0 || limitLength < 0 ){
            throw new MyException("Illegal input value");
        }
        JavaRDD<Row> sourceData = df.javaRDD();
        final Broadcast<Long> timeFactor = jsc.broadcast(second);
        final Broadcast<Long> timeColumnFactor = jsc.broadcast(timeColumn);
        final Broadcast<Long> limitLengthFactor = jsc.broadcast(limitLength);
        JavaPairRDD<String,Row> sessionTuple = sourceData.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
                //Long second = timeFactor.getValue();
                //Long timeColumn = timeColumnFactor.getValue();
               // Long limitLength = limitLengthFactor.getValue();
                Long sessionID = 0L;
                long sessionLen = 0;
                String startTime = null;
                long between;
                List<Tuple2<String,Row>> retList = new ArrayList<>();
                while(iterator.hasNext()){
                    Row curRow = iterator.next();
                    String curTime = curRow.get(timeColumn.intValue()).toString();
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    //初始化每个session的起始时间
                    if(null == startTime){
                        startTime = curTime;
                    }
                    //获取时间间隔
                    between  = dateFormat.parse(curTime).getTime() - dateFormat.parse(startTime).getTime();
                    if((between > second)){
                        sessionID ++;
                        if(sessionLen > limitLength){
                            throw new MyException("session length more than the input max length");
                        }
                        startTime = curTime;
                        sessionLen=1L;
                    }else{
                        sessionLen++;
                    }
                    retList.add(new Tuple2<>(sessionID.toString(),curRow));
                }

                return retList;
            }
        });

        JavaRDD<Row> sessionRDD = sessionTuple.map(new Function<Tuple2<String, Row>, Row>() {
            @Override
            public Row call(Tuple2<String, Row> tuple) throws Exception {

                return RowFactory.create(tuple._1 + "," + tuple._2.toString());
            }
        });
        return sessionRDD;
    }

    /**
     * 按照时间增量划分session
     * @param jsc
     * @param df
     * @param range
     * @param timeColumn
     * @param limitLength
     * @return
     * @throws MyException
     */
    public static JavaRDD<Row> timeRangeSession(JavaSparkContext jsc, DataFrame df, Long range, Long timeColumn,
                                                Long limitLength)throws MyException{
        if(range <= 0 || timeColumn < 0 || limitLength < 0){
            throw new MyException("Illegal input value");
        }
        JavaRDD<Row> sourceData = df.javaRDD();
        final Broadcast<Long> rangeFactor = jsc.broadcast(range);
        final Broadcast<Long> timeColumnFactor = jsc.broadcast(timeColumn);
        final Broadcast<Long> limitLengthFactor = jsc.broadcast(limitLength);
        JavaPairRDD<String,Row> sessionTuple = sourceData.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
                Long range = rangeFactor.getValue();
                Long timeColumn = timeColumnFactor.getValue();
                Long limitLength = limitLengthFactor.getValue();
                Long sessionID = 0L;
                String preTime = null;
                long sessionLen = 0;
                Long between = 0L;
                List<Tuple2<String,Row>> retList = new ArrayList<>();
                while(iterator.hasNext()){
                    Row curRow = iterator.next();
                    String curTime = curRow.get(timeColumn.intValue()).toString();
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    if(null == preTime){
                        preTime = curTime;
                    }
                    between  = dateFormat.parse(curTime).getTime() - dateFormat.parse(preTime).getTime();
                    if(between > range){
                        sessionID ++;
                        if(sessionLen > limitLength){
                            throw new MyException("session length more than the input max length");
                        }
                        sessionLen=1L;
                    }else {
                        sessionLen++;
                    }
                    preTime = curTime;
                    retList.add(new Tuple2<>(sessionID.toString(),curRow));
                }
                return retList;
            }
        });

        JavaRDD<Row> sessionRDD = sessionTuple.map(new Function<Tuple2<String, Row>, Row>() {
            @Override
            public Row call(Tuple2<String, Row> tuple) throws Exception {
                return RowFactory.create(tuple._1+","+tuple._2.toString());
            }
        });
        return sessionRDD;
    }

    /**
     * 按照维度增量值划分session
     * @param jsc
     * @param df
     * @param range
     * @param dimColumn
     * @param limitLength
     * @return
     * @throws MyException
     */
    public static JavaRDD<Row> dimRangeSession(JavaSparkContext jsc, DataFrame df, double range, Long dimColumn,
                                               Long limitLength) throws MyException{

        if(range <= 0 || dimColumn < 0 || limitLength < 0){
            throw new MyException("Illegal input value");
        }
        JavaRDD<Row> sourceData = df.javaRDD();
        final Broadcast<Double> rangeFactor = jsc.broadcast(range);
        final Broadcast<Long> dimColumnFactor = jsc.broadcast(dimColumn);
        final Broadcast<Long> limitLengthFactor = jsc.broadcast(limitLength);
        JavaPairRDD<String,Row> sessionTuple = sourceData.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
                double range = rangeFactor.getValue();
                Long dimColumn = dimColumnFactor.getValue();
                Long limitLength = limitLengthFactor.getValue();
                Long sessionID = 0L;
                double preDim = 0.0;
                long sessionLen = 0;
                List<Tuple2<String,Row>> retList = new ArrayList<>();
                while (iterator.hasNext()){
                    Row curRow = iterator.next();
                    double curDim = Double.valueOf(curRow.get(dimColumn.intValue()).toString());
                    if(0.0 == preDim){
                        preDim = curDim;
                    }
                    if(Math.abs(preDim - curDim) > range){
                        sessionID ++;
                        if(sessionLen > limitLength){
                            throw new MyException("session length more than the input max length");
                        }
                        sessionLen=1L;
                    }else{
                        sessionLen++;
                    }
                    preDim = curDim;
                    retList.add(new Tuple2<>(sessionID.toString(),curRow));
                }
                return retList;
            }
        });
        JavaRDD<Row> sessionRDD = sessionTuple.map(new Function<Tuple2<String, Row>, Row>() {
            @Override
            public Row call(Tuple2<String, Row> tuple) throws Exception {
                return RowFactory.create(tuple._1+","+tuple._2.toString());
            }
        });
        return sessionRDD;
    }

    /**
     * 按照时间间隔和维度增量划分session
     * @param jsc
     * @param df
     * @param second
     * @param timeColumn
     * @param range
     * @param dimColumn
     * @param limitLength
     * @return
     * @throws MyException
     */
    public static JavaRDD<Row> timeSpacingDimRangeSession(JavaSparkContext jsc, DataFrame df, Long second, Long timeColumn,
                                                          double range, Long dimColumn, Long limitLength) throws MyException{

        if(second <= 0 || timeColumn < 0 || range <=0 || dimColumn < 0 || limitLength <= 0){
            throw new MyException("Illegal input value");
        }

        JavaRDD<Row> sourceData = df.javaRDD();
        final Broadcast<Long> timeFactor = jsc.broadcast(second);
        final Broadcast<Long> timeColumnFactor = jsc.broadcast(timeColumn);
        final Broadcast<Double> rangeFactor = jsc.broadcast(range);
        final Broadcast<Long> dimColumnFactor = jsc.broadcast(dimColumn);
        final Broadcast<Long> limitLengthFactor = jsc.broadcast(limitLength);
        JavaPairRDD<String,Row> sessionTuple = sourceData.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {

                Long second = timeFactor.getValue();
                double range = rangeFactor.getValue();
                Long timeColumn = timeColumnFactor.getValue();
                Long dimColumn = dimColumnFactor.getValue();
                Long limitLength = limitLengthFactor.getValue();
                Long sessionID = 0L;
                String startTime = null;
                Long sessionLen = 0L;
                long between;
                double span;
                double preDim = 0.0;
                List<Tuple2<String,Row>> retList = new ArrayList<>();
                while(iterator.hasNext()){
                    Row curRow = iterator.next();
                    String curTime = curRow.get(timeColumn.intValue()).toString();
                    double curDim = Double.valueOf(curRow.get(dimColumn.intValue()).toString());
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    if(null == startTime){
                        startTime = curTime;
                    }
                    if(preDim == 0.0){
                        preDim = curDim;
                    }
                    between  = dateFormat.parse(curTime).getTime() - dateFormat.parse(startTime).getTime();
                    span = Math.abs(curDim - preDim);
                    if(between > second || span > range){
                        sessionID ++;
                        if(sessionLen > limitLength){
                            throw new MyException("session length more than the input max length");
                        }
                        startTime = curTime;
                        sessionLen=1L;
                    }else{
                        sessionLen++;
                    }
                    preDim = curDim;
                    retList.add(new Tuple2<>(sessionID.toString(),curRow));
                }
                return retList;
            }
        });
        JavaRDD<Row> sessionRDD = sessionTuple.map(new Function<Tuple2<String,Row>, Row>() {
            @Override
            public Row call(Tuple2<String, Row> tuple) throws Exception {

                return RowFactory.create(tuple._1+","+tuple._2.toString());
            }
        });
        return sessionRDD;
    }

    /**
     *按照时间间隔和时间增量划分session
     * @param jsc
     * @param df
     * @param second
     * @param range
     * @param timeColumn
     * @param limitLength
     * @return
     * @throws MyException
     */
    public static JavaRDD<Row> timeSpacingRangeSession(JavaSparkContext jsc, DataFrame df, Long second, Long range,
                                                       Long timeColumn, Long limitLength) throws MyException{
        if(second <= 0 || range <= 0 || timeColumn < 0 || limitLength <= 0){
            throw new MyException("Illegal input value");
        }

        JavaRDD<Row> sourceData = df.javaRDD();
        final Broadcast<Long> secondFactor = jsc.broadcast(second);
        final Broadcast<Long> rangeFactor = jsc.broadcast(range);
        final Broadcast<Long> timeColumnFactor = jsc.broadcast(timeColumn);
        final Broadcast<Long> limitColumnFactor = jsc.broadcast(limitLength);
        JavaPairRDD<String,Row> sessionTuple = sourceData.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
                Long second = secondFactor.getValue();
                Long range = rangeFactor.getValue();
                Long timeColumn = timeColumnFactor.getValue();
                Long limitLength = limitColumnFactor.getValue();
                String startTime = null;
                String preTime = null;
                Long between;
                Long span;
                Long sessionID = 0L;
                Long sessionLen = 0L;
                List<Tuple2<String,Row>> retList = new ArrayList<>();
                while(iterator.hasNext()){
                    Row curRow = iterator.next();
                    String curTime = curRow.getString(timeColumn.intValue());
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    if(null == startTime){
                        startTime = curTime;
                    }
                    if(null == preTime){
                        preTime = curTime;
                    }
                    between  = (dateFormat.parse(curTime).getTime() - dateFormat.parse(startTime).getTime());
                    span = (dateFormat.parse(curTime).getTime() - dateFormat.parse(preTime).getTime());
                    if((between > second) || span > range){
                        sessionID ++;
                        if(sessionLen > limitLength){
                            throw new MyException("session length more than the input max length");
                        }
                        startTime = curTime;
                        sessionLen=1L;
                    }else {
                        sessionLen++;
                    }
                    preTime = curTime;
                    retList.add(new Tuple2<>(sessionID.toString(),curRow));
                }
                return retList;
            }
        });

        JavaRDD<Row> sessionRDD = sessionTuple.map(new Function<Tuple2<String, Row>, Row>() {
            @Override
            public Row call(Tuple2<String, Row> tuple) throws Exception {

                return RowFactory.create(tuple._1+","+tuple._2.toString());
            }
        });
        return sessionRDD;
    }
}
