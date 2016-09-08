package ml.k_means;

import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;

import java.util.List;

/**
 * Created by Jary on 2016/9/2 0002.
 */
public class KMeans_Java {

    public static void main(String [] a){
        String datadir = "data/mllib/kmeans_data.txt";

        KMeans_Java kms =  new KMeans_Java(datadir);
        KMeansModel kmd =  kms.clusterS(3,29);
        kms.printClusters(kmd);
        System.out.println("欧几里得："+kms.euclid(kmd));
        System.out.println(kms.SCall(kmd));
        kms.stop();
    }
    static K_Means km=null;
    /**
     * 构造方法 初始化数据源
     @param datadir datasource
     */
    public KMeans_Java(String datadir){
        km = new K_Means(datadir);
    }
    /** 聚类
     @param k 分类数
     @param numitarator 最大迭代次数
     @return KMeansModel 训练后的模型
     */
    public static  KMeansModel clusterS (int k , int numitarator){
        KMeansModel kmd =  km.clusterCenters(k,numitarator);
        return kmd;
    }
    /**
     * 打印所有中心点
     @param kmd 模型
     */
    public static void printClusters (KMeansModel kmd){
        List<String> clusterS = km.printCenters(kmd);
        for (String i :clusterS) {
            System.out.println(i);
        }
    }

    /**预测
     @param v 向量点
     @param kmd 模型
     @return 该向量点属于的类
     */
    public static int prodict(Vector v,KMeansModel kmd){
        int pro = kmd.predict(v);
        return pro;
    }

    /** 欧几里得 它越小，说明聚类越好
     * @param kmd 模型
     * @return 欧几里得距离
     */
    public static double euclid (KMeansModel kmd ){
       double oji =  km.ouj(kmd);
        return oji;
    }

    /**
     * @param kmd 模型
     * @return 轮廓系数  [-1,1] 越大分类越好
     */
    public static double SCall(KMeansModel kmd){
        double d = km.SCall(kmd);
        return d;
    }

    /**
     * 结束计算
     */
    public static void  stop (){
        km.stop();
    }

}
