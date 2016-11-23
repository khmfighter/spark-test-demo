package cn.nuecloud.bigdata.dasuan.analysis.normalization;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.Accumulator;
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

/**
 * @example{{{ SparkConf conf new SparkConf().setAppName("test").setMaster("local[20]").set("spark.default.parallelism","1");
 * JavaSparkContext jsc new JavaSparkContext(conf);
 * SQLContext sqlContext = new SQLContext(jsc);
 * DataFrame dt = sqlContext.read().json("data/testFile/normalization.json");
 * DataFrame  aaDataFrame = Normalization. MinMax(sc,sqlContext,resultDF);
 * aaDataFrame.show();
 * DataFrame  aaDataFrame1 = Normalization. zscore(sc, sqlContext,resultDF);
 * <p/>
 * }}}
 */

/**
 * 将传入数据标准化
 *
 * @version Neucloud2016 2016-11-03
 * @auctor qingda
 */
public class Normalization {
    @SuppressWarnings("unchecked")
    /**
     * 按照(x-mean)/std标准化，mean为平均值，std为方差
     * @param jsc
     * @param df         数据源
     * @param sqlContext
     * @return
     * @exception
     */
    public static DataFrame zscore(JavaSparkContext jsc, SQLContext sqlContext,
                                   DataFrame df) {
        Map<String, String> map = new HashMap<String, String>();
        DataFrame resultDF = null;
        //dataFrame格式转换string->double
        JavaRDD<Row> rows = df.toJavaRDD();
        JavaRDD<Row> rows1 = rows.map(new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(Row line) {
                String line1 = line.toString().replaceAll("[\\[\\]]", "");
                return RowFactory.create(Double.valueOf(line1));
            }
        });
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("value",
                DataTypes.DoubleType, true));
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame personDF = sqlContext.createDataFrame(rows1, structType);
        personDF.registerTempTable("df");
        DataFrame sum_df = sqlContext.sql("SELECT sum(value) FROM df");
        put_map(sum_df, map, "sum");
        DataFrame count_df = sqlContext.sql("SELECT count(value) FROM df");
        put_map(count_df, map, "count");
        final double sum = Double.valueOf(map.get("sum"));
        final double count = Double.valueOf(map.get("count"));
        final double average = sum / count;
        final Accumulator<Double> sum_variance = jsc.accumulator(0.0);
        JavaRDD<String> rows_result = rows1.map(new Function<Row, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Row line) {
                String line1 = line.toString().replaceAll("[\\[\\]]", "");
                sum_variance.add((Double.valueOf(line1) - average) * (Double.valueOf(line1) - average));
                return line.toString();
            }
        });
        rows_result.count();
        DecimalFormat df1 = new DecimalFormat("0.0000");
        String db = df1.format(sum_variance.value());
        double variance = Double.valueOf(db);
        final double standard = Double.valueOf(df1.format((Math.sqrt(variance / count))));
        JavaRDD<Row> rows_result1 = rows1.map(new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(Row line) {
                String line1 = line.toString().replaceAll("[\\[\\]]", "");
                double temp = Double.valueOf(line1);
                double result = (temp - average) / standard;
                DecimalFormat df = new DecimalFormat("0.0000");
                String db = df.format(result);
                return RowFactory.create(Double.valueOf(db));
            }
        });
        List<StructField> structFields_result = new ArrayList<StructField>();
        structFields_result.add(DataTypes.createStructField("value",
                DataTypes.DoubleType, true));
        StructType structType_result = DataTypes.createStructType(structFields_result);
        resultDF = sqlContext.createDataFrame(rows_result1, structType_result);
        resultDF.show();
        return resultDF;
    }

    /**
     * 按照（x-min）/(max-min)标准化
     *
     * @param jsc
     * @param df 数据源
     * @param sqlContext
     * @return
     * @exception
     */
    public static DataFrame MinMax(JavaSparkContext jsc, SQLContext sqlContext,
                                   DataFrame df) {
        Map<String, String> map = new HashMap<String, String>();
        DataFrame resultDF = null;
        //dataFrame格式转换string->double
        JavaRDD<Row> rows = df.toJavaRDD();
        JavaRDD<Row> rows1 = rows.map(new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(Row line) {
                String line1 = line.toString().replaceAll("[\\[\\]]", "");
                return RowFactory.create(Double.valueOf(line1));
            }
        });
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("value",
                DataTypes.DoubleType, true));
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame personDF = sqlContext.createDataFrame(rows1, structType);
        //	personDF1.show();
        personDF.registerTempTable("df");
        // ArrayList list = new ArrayList();
        // SQL statements can be run by using the sql methods provided by
        DataFrame max_df = sqlContext.sql("SELECT max(value) FROM df");
        max_df.show();
        put_map(max_df, map, "max");
        DataFrame min_df = sqlContext.sql("SELECT min(value) FROM df");
        put_map(min_df, map, "min");
        final double max = Double.valueOf(map.get("max"));
        final double min = Double.valueOf(map.get("min"));
        JavaRDD<Row> rows_result = rows1.map(new Function<Row, Row>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Row call(Row line) {
                String line1 = line.toString().replaceAll("[\\[\\]]", "");
                double temp = Double.valueOf(line1);
                double result = (temp - min) / (max - min);
                DecimalFormat df = new DecimalFormat("0.0000");
                String db = df.format(result);
                return RowFactory.create(Double.valueOf(db));
            }
        });
        List<StructField> structFields_result = new ArrayList<StructField>();
        structFields_result.add(DataTypes.createStructField("value",
                DataTypes.DoubleType, true));
        StructType structType_result = DataTypes.createStructType(structFields_result);
        resultDF = sqlContext.createDataFrame(rows_result, structType_result);
        resultDF.show();
        return resultDF;
    }

    public static void put_map(DataFrame df1, Map<String, String> map,
                               String key) {
        JavaRDD<Row> df1_temp = df1.toJavaRDD();
        for (Row rdd1 : df1_temp.collect()) {
            String str = rdd1.toString().replaceAll("[\\[\\]]", "");
            map.put(key, str.toString());
        }

    }
}
