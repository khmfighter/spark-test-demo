package cn.nuecloud.bigdata.dasuan.analysis.stat;


import cn.nuecloud.bigdata.dasuan.analysis.correlation.CorRelations;
import org.apache.spark.SparkConf;
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
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a
 * method is not specified, Pearson's method will be used by default.
 * Created by Administrator on 2016/8/30 0030.
 */
public class DSCorRelation implements Serializable {
    /**
     * @param sourceDSX
     * @param sourcdDSY
     * @return
     */
    public static double computOne(DataFrame sourceDSX, DataFrame sourcdDSY){
        double a = CorRelations.relationDouble(sourceDSX,sourcdDSY);
        return a;
    }
    /**
     * @param sourceDSX
     * @param sourcdDSY
     * @param method "spearman" or "pearson"
     * @return
     */
    public static double computOne(DataFrame sourceDSX, DataFrame sourcdDSY,String method){
        double a = CorRelations.relationDouble(sourceDSX,sourcdDSY,method);
        return a;
    }

    public static void main(String [] a){


        //Assert.assertEquals(re,1.0);

    }

}
