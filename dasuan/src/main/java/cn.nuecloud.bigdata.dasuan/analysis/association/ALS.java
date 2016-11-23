package cn.nuecloud.bigdata.dasuan.analysis.association;

import cn.nuecloud.bigdata.dasuan.analysis.als.ModelALS;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 2016/11/7 0007.
 */
public class ALS {
    ModelALS ui =null;

    public ALS(){
        ui = new ModelALS();
    }

    public List getRecommendation(JavaSparkContext jsc, String rating, String action, String item, List ranks, List lambdas, List numIters) {
        SparkContext sc = JavaSparkContext.toSparkContext(jsc);
        List lsss = ui.tranALS(sc, rating, action, item, ranks, lambdas, numIters);
        return lsss;
    }
    public static void main(String[] a) {
        ALS als = new ALS();

        String userRatingsData = "data/mllib/als/movielens/medium/sample_movielens_ratings.txt";
        String allratingsData = "data/mllib/als/movielens/medium/ratings.dat";
        String itemsData = "data/mllib/als/movielens/medium/movies.dat";
        JavaSparkContext jsc = new JavaSparkContext(new SparkConf().setMaster("local[4]").setAppName("als"));

        ArrayList ranks = new ArrayList() {{
            add(8);
            add(12);
        }};
        ArrayList lambdas = new ArrayList() {{
            add(0.1);
            add(10.0);
        }};
        ArrayList numIters = new ArrayList() {{
            add(10);
            add(20);
        }};
        List list = als.getRecommendation(jsc, allratingsData, userRatingsData, itemsData, ranks, lambdas, numIters);
    }
}
