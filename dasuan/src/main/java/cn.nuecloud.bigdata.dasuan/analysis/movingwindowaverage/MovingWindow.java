package cn.nuecloud.bigdata.dasuan.analysis.movingwindowaverage;

import cn.nuecloud.bigdata.dasuan.exception.MyException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 按照滑动窗口求平均值
 *
 * @author xuhaifeng
 * @version Neucloud2016 2016-11-09
 */

public class MovingWindow implements java.io.Serializable{

    /**
     *
     * @param jsc
     * @param data 数据源
     * @param valueColumn 值所在列索引
     * @param windowSize 窗口大小
     * @param step 滑动step大小
     * @return
     * @throws MyException
     */
    public static DataFrame movingWindowAverage(JavaSparkContext jsc,DataFrame data,int valueColumn,
                                                int windowSize,int step) throws MyException{
        if(valueColumn < 0 || windowSize < step ){
            throw new MyException("input value illegal");
        }
        JavaRDD<Row> dataRDD = data.javaRDD();
        final Broadcast<Integer> valueColumnFactor = jsc.broadcast(valueColumn);
        final Broadcast<Integer> windowSizeFactor = jsc.broadcast(windowSize);
        final Broadcast<Integer> stepFactor = jsc.broadcast(step);
        JavaPairRDD<Integer,Row> repartitionRDD = rePartition(dataRDD,windowSizeFactor);

        JavaRDD<List<Double>> num2AverageRDD = getWindowList(jsc,repartitionRDD,stepFactor,windowSizeFactor,valueColumnFactor);

        DataFrame resDF = windowAverage(jsc,num2AverageRDD,windowSizeFactor);
        return resDF;
    }

    /**
     *
     * @param dataRDD 源数据
     * @param windowSizeFactor window大小
     * @return
     */
    private static JavaPairRDD<Integer,Row> rePartition(JavaRDD<Row> dataRDD,final Broadcast<Integer> windowSizeFactor){

        //将除了第0个partition外的每个partition的前window-1个数据，key赋值为partition-1
        JavaRDD<Tuple2<Integer,Row>> partition2ValueRDD = dataRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Row>,
                Iterator<Tuple2<Integer, Row>>>() {
            @Override
            public Iterator<Tuple2<Integer, Row>> call(Integer index, Iterator<Row> iterator) throws Exception {
                List<Tuple2<Integer,Row>> list = new ArrayList<>();
                int critical = windowSizeFactor.getValue().intValue() - 1;
                while (iterator.hasNext()){
                    Row curRow = iterator.next();
                    list.add(new Tuple2<>(index,curRow));
                    if(critical > 0 && index != 0){
                        list.add(new Tuple2<>(index -1,curRow));
                        critical--;
                    }
                }
                return list.iterator();
            }
        },false).cache();

        //对所有数据按照key重新进行partition
        JavaPairRDD<Integer,Row> repartitionRDD = partition2ValueRDD.mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<Tuple2<Integer, Row>>, Integer, Row>() {
                    @Override
                    public Iterable<Tuple2<Integer, Row>> call(Iterator<Tuple2<Integer, Row>> iterator) throws Exception {
                        List<Tuple2<Integer,Row>> list = new ArrayList<>();
                        while (iterator.hasNext()){
                            list.add(iterator.next());
                        }
                        return list;
                    }
                }).partitionBy(new MyPartitioner(partition2ValueRDD.getNumPartitions()));

        return repartitionRDD;
    }

    /**
     *
     * @param jsc
     * @param repartitionRDD
     * @param stepFactor 滑动step大小
     * @param windowSizeFactor window大小
     * @param valueColumnFactor value所在列索引
     * @return
     */
    private static JavaRDD<List<Double>> getWindowList(JavaSparkContext jsc,JavaPairRDD<Integer,Row> repartitionRDD,
                                                       final Broadcast<Integer> stepFactor, final Broadcast<Integer> windowSizeFactor,
                                                       final Broadcast<Integer> valueColumnFactor){
        //获取每个partition中元素个数
        List<Integer> list = repartitionRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Row>>, Integer>() {
            @Override
            public Iterable<Integer> call(Iterator<Tuple2<Integer, Row>> iterator) throws Exception {
                int size = 0;
                while (iterator.hasNext()){
                    iterator.next();
                    size++;
                }
                List<Integer> list = new ArrayList<>();
                list.add(size);
                return list;
            }
        }).collect();
        final Broadcast<List<Integer>> partitionSizeFactor = jsc.broadcast(list);
        //对每个partition中元素按滑动窗口划分list
        JavaRDD<List<Double>> num2AverageRDD = repartitionRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Row>>,
                List<Double>>() {
            @Override
            public Iterable<List<Double>> call(Iterator<Tuple2<Integer, Row>> iterator) throws Exception {
                List<List<Double>> valueList = new ArrayList<>();
                List<Integer> list = partitionSizeFactor.getValue();
                int step = stepFactor.getValue();
                int window = windowSizeFactor.getValue();
                int offset=0;
                int curOffset = 0;
                int index = 0;
                int partitionNum;
                boolean bFirst=true;
                //将partition交界处的重复部分偏移
                while (iterator.hasNext()){
                    Tuple2<Integer,Row> curTuple = iterator.next();
                    partitionNum=curTuple._1;
                    if(0 != partitionNum && bFirst){
                        for(int i=0;i < partitionNum;i++){
                            offset+=list.get(i);
                        }
                        offset = offset % step;
                        offset = step - offset;
                        bFirst = false;
                    }
                    if(curOffset >= offset){
                        valueList.add(0,new ArrayList<Double>());
                        valueList.get(0).add(Double.valueOf(curTuple._2.getString(valueColumnFactor.getValue())));
                        index++;
                        break;
                    }
                    curOffset++;
                }
                //将每个元素添加到所属的window list中
                int iFlag = window;
                while(iterator.hasNext()){
                    int start = 0;
                    if(iFlag > 1){
                        start = index / window;
                        iFlag--;
                    }
                    else{
                        start = (index - window)/step+1;
                    }
                    int end = index / step;
                    double value = Double.valueOf(iterator.next()._2.getString(valueColumnFactor.getValue()));
                    for(int i = start;i <= end;i++){
                        if(valueList.size() <= i){
                            valueList.add(i,new ArrayList<Double>());
                        }
                        valueList.get(i).add(value);
                    }
                    index++;
                }
                return valueList;
            }
        });
        return num2AverageRDD;
    }

    /**
     *
     * @param jsc
     * @param num2AverageRDD
     * @param windowSizeFactor window大小
     * @return
     */
    private static DataFrame windowAverage(JavaSparkContext jsc,JavaRDD<List<Double>> num2AverageRDD,
                                           final Broadcast<Integer> windowSizeFactor){
        //过滤掉长度小于window的数据
        JavaRDD<List<Double>> filterRDD = num2AverageRDD.filter(new Function<List<Double>, Boolean>() {
            @Override
            public Boolean call(List<Double> list) throws Exception {

                return list.size() == windowSizeFactor.getValue();
            }
        });

        //对每个窗口求平均值
        JavaRDD<Row> rowRDD = filterRDD.map(new Function<List<Double>, Double>() {
            @Override
            public Double call(List<Double> values) throws Exception {
                double sum = 0.f;
                int length = values.size();
                for(int i = 0;i < length;i++){
                    sum+=values.get(i);
                }
                return sum/values.size();
            }
        }).zipWithIndex().map(new Function<Tuple2<Double, Long>, Row>() {
            @Override
            public Row call(Tuple2<Double, Long> v1) throws Exception {
                return RowFactory.create(v1._2,v1._1);
            }
        });
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("index", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("average", DataTypes.DoubleType, true));
        StructType schema = DataTypes.createStructType(fields);
        SQLContext sqlContext = new SQLContext(jsc);
        DataFrame dataFrame = sqlContext.createDataFrame(rowRDD,schema);
        return dataFrame;
    }
}
