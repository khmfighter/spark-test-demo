package mllib.k_means;

import org.apache.spark.mllib.clustering.KMeansModel;

import java.util.List;

/**
 * Created by Administrator on 2016/9/2 0002.
 */
public class KMeans_Java {


    public static void main(String [] a){
        String datadir = "data/mllib/kmeans_data.txt";
        KMeans_Java kmsjava =  new KMeans_Java(datadir);
        KMeansModel kmd =  kmsjava.clusterS(2,29);
        // kmsjava.printClusters(kmd);
        //System.out.println("欧几里得："+kmsjava.euclid(kmd));
        System.out.println(kmsjava.SCall(kmd));
        kmsjava.stop();
    }

    static K_Means km=null;
    public KMeans_Java(String datadir){
        km = new K_Means(datadir);
    }

    //中心点
    public static  KMeansModel clusterS (int k , int numitarator){
        KMeansModel kmd =  km.clusterCenters(k,numitarator);
        return kmd;
    }
    public static void printClusters (KMeansModel kmd){
        List<String> clusterS = km.printCenters(kmd);
        for (String i :clusterS) {
            System.out.println(i);
        }
    }

    // 欧几里得
    public static double euclid (KMeansModel kmd ){
       double oji =  km.ouj(kmd);
        return oji;
    }
    //轮廓系数
    public static double SCall(KMeansModel kmd){
        double d = km.SCall(kmd);
        return d;
    }
    public static void  stop (){
        km.stop();
    }

}
