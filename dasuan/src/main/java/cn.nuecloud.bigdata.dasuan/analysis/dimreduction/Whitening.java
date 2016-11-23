package cn.nuecloud.bigdata.dasuan.analysis.dimreduction;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

/**
 * PCA降维，对降维后特征进行方差为1的白化
 * @author xuhaifeng
 * @version Neucloud2016 2016-10-27
 */
public final class Whitening{

    /**
     * @param jsc
     * @param df 数据源
     * @param k 将原始数据降到k维
     * @param inputFeatures 数据所在列列名
     * @return
     */
    public static DataFrame dimReduction(JavaSparkContext jsc,DataFrame df,int k,String inputFeatures){
        if(k <= 0 || inputFeatures.isEmpty()){
            //TODO: throw exception
        }
        PCAModel pca = new PCA().setInputCol(inputFeatures)
                .setOutputCol("pcaFeatures").setK(k).fit(df);
        JavaRDD<Row> rdd = pca.transform(df).select("pcaFeatures").toJavaRDD();

        JavaRDD<Tuple3<Integer,Integer,Double>> index2ValueRDD = rdd.zipWithIndex().flatMap(new FlatMapFunction<Tuple2<Row, Long>, Tuple3<Integer,Integer,Double>>() {
            @Override
            public Iterable<Tuple3<Integer,Integer,Double>> call(Tuple2<Row, Long> tuple) throws Exception {
                List<Tuple3<Integer,Integer,Double>> resList = new ArrayList<>();
                String[] arrays = tuple._1.toString().substring(2, tuple._1.toString().length()-2).split(",");
                int length = arrays.length;
                for(int i=0;i<length;i++){
                    resList.add(new Tuple3<>(tuple._2.intValue(),Integer.valueOf(i),Double.valueOf(arrays[i])));
                }
                return resList;
            }
        });
        JavaPairRDD<Integer,Tuple2<Integer,Double>> col2TupleRDD = index2ValueRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple3<Integer, Integer, Double>>, Integer, Tuple2<Integer, Double>>() {
            @Override
            public Iterable<Tuple2<Integer, Tuple2<Integer, Double>>> call(Iterator<Tuple3<Integer, Integer, Double>> iterator) throws Exception {
                List<Tuple2<Integer,Tuple2<Integer,Double>>> list = new ArrayList<>();
                while(iterator.hasNext()){
                    Tuple3<Integer, Integer, Double> tuple3 = iterator.next();
                    list.add(new Tuple2<>(tuple3._2(),new Tuple2<>(tuple3._1(),tuple3._3())));
                }
                return list;
            }
        }).cache();
        JavaPairRDD<Integer, Iterable<Tuple3<Integer, Integer, Double>>> col2GroupRDD = index2ValueRDD.groupBy(
                new Function<Tuple3<Integer,Integer,Double>, Integer>() {
            @Override
            public Integer call(Tuple3<Integer, Integer, Double> tuple3) throws Exception {
                return tuple3._2();
            }
        });
        JavaPairRDD<Integer,Double> clo2VarianceRDD = col2GroupRDD.mapToPair(new PairFunction<Tuple2<Integer,
                Iterable<Tuple3<Integer,Integer,Double>>>, Integer, Double>() {
            @Override
            public Tuple2<Integer, Double> call(
                    Tuple2<Integer, Iterable<Tuple3<Integer, Integer, Double>>> tuple)
                    throws Exception {
                List<Double> list = new ArrayList<>();
                double variance = 0.f;
                double mean = 0.f;
                Iterator<Tuple3<Integer, Integer, Double>> iter = tuple._2.iterator();
                while(iter.hasNext()){
                    double temp = iter.next()._3();
                    list.add(temp);
                    mean += temp;
                }
                mean=mean/list.size();
                int size = list.size();
                for(int i=0;i<size;i++){
                    variance+=Math.pow(mean-list.get(i),2);
                }
                variance = Math.sqrt(variance/list.size());
                return new Tuple2<>(tuple._1,variance);
            }
        });
        JavaPairRDD<Integer, Tuple2<Double, Tuple2<Integer, Double>>> col2RowRDD = clo2VarianceRDD.join(col2TupleRDD);

        JavaRDD<Row> resRDD = col2RowRDD.map(new Function<Tuple2<Integer,Tuple2<Double,Tuple2<Integer,Double>>>, Row>() {

            @Override
            public Row call(
                    Tuple2<Integer, Tuple2<Double, Tuple2<Integer, Double>>> tuple)
                    throws Exception {

                return RowFactory.create(tuple._2._2._1,tuple._1,tuple._2._2._2/tuple._2._1);
            }
        });
        JavaPairRDD<Integer, Iterable<Row>> valueRDD = resRDD.groupBy(new Function<Row, Integer>() {

            @Override
            public Integer call(Row row) throws Exception {

                return row.getInt(0);
            }
        });
        JavaRDD<Row> retRDD = valueRDD.map(new Function<Tuple2<Integer,Iterable<Row>>, Row>() {

            @Override
            public Row call(Tuple2<Integer, Iterable<Row>> tuple) throws Exception {
                Iterator<Row> iterator = tuple._2.iterator();
                Map<Integer, Double> map = new TreeMap<>(new Comparator() {
                    @Override
                    public int compare(Object o1, Object o2) {
                        return (int)o1 - (int)o2;
                    }
                });
                Vector<Double> vector = new Vector<>();
                while(iterator.hasNext()){
                    Row row = iterator.next();
                    map.put(row.getInt(1), row.getDouble(2));
                }
                double[] array = new double[map.size()];
                int size = map.size();
                for(int i=0;i<size;i++){
                    vector.add(map.get(i));
                    array[i] = map.get(i);
                }
                return RowFactory.create(Vectors.dense(array));
            }
        });
        StructType schema = new StructType(new StructField[]{
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });
        SQLContext jsql = new SQLContext(jsc);
        DataFrame dt = jsql.createDataFrame(retRDD, schema);

        return dt;
    }
}
