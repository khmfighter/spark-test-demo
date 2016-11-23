package cn.nuecloud.bigdata.dasuan.analysis.cross_correlation;

import cn.nuecloud.bigdata.dasuan.analysis.stat.DSCorRelation;
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
import java.util.*;
import java.util.List;
/**
 * @example{{{
 * SparkConf conf= new SparkConf().setAppName("test").setMaster("local");
 * JavaSparkContext jsc= new JavaSparkContext(conf);
 * SQLContext sqlContext = new SQLContext(jsc);
 * DataFrame df1 = sqlContext.read().json("data/testFile/cross_correlation.json");
 *  DataFrame reJavaRDD = cross_correlation.cross(jsc, df1);
 * }}}
 */

/**
 * 算出所有列的correlation相关性。
 *
 * @version Neucloud2016 2016-11-03
 * @auctor qingda
 */
public class cross_correlation  {
    /**
     * 按照时间间隔划分sessize
     *
     * @param jsc
     * @param df1          数据源
     * @return
     * @exception
     */
    private static final long serialVersionUID = 1L;
    public static DataFrame cross(JavaSparkContext jsc, DataFrame df1) {
        SQLContext sqlContext = new SQLContext(jsc);
        String[] schema1 = df1.columns();  //获取所有schema数
        List<String> kms_column=new ArrayList<String>(); //schema list
        DataFrame personDF = null;
        List<String> kms_result=new ArrayList<String>(); //结果list
        String d=null;
        for(int i =0;i<schema1.length-1;i++){
            for (int j=i+1;j<schema1.length;j++) {
                final String value1 = schema1[i];
                final String value2 = schema1[j];
                DataFrame sourceDSX = df1.select(value1);
                DataFrame sourceDSY = df1.select(value2);
                DSCorRelation kms = new DSCorRelation();
                double  kms1 = kms.computOne(sourceDSX,sourceDSY);
                String str_kms=String.valueOf(kms1);
                if(j==1){
                    d=str_kms+",";

                }else if (i==schema1.length-2){
                    d += str_kms ;
                }else{
                    d += str_kms + ",";
                }
                String column=value1+"|"+value2;
                kms_column.add(column);
            }
        }
        kms_result.add(d);    //生成结果list
        List<StructField> structFields = new ArrayList<>();
        for (int i=0;i<kms_column.size();i++){
            System.out.println(kms_column.get(i));
            String c=kms_column.get(i);
            structFields.add(DataTypes.createStructField(c, DataTypes.StringType,
                       true));
        }
        JavaRDD<String> rdd = jsc.parallelize(kms_result);
        JavaRDD<Row> personRDD = rdd.map(new Function<String, Row>() {
            private static final long serialVersionUID = 1L;
            public Row call(String line) throws Exception {
               String[] splited = line.split(",");
                return RowFactory.create(splited);
            }
        });
       StructType structType = DataTypes.createStructType(structFields);
       personDF = sqlContext.createDataFrame(personRDD, structType);
        return personDF;
    }

}


