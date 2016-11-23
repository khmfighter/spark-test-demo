package cn.nuecloud.bigdata.dasuan.analysis.clustering;

import cn.nuecloud.bigdata.dasuan.analysis.bisectingkmeans.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;

/**
 * Bisecting K-Means聚类
 * @author xuhaifeng
 * @version Neucloud2016 2016-10-24
 */
public class BKMeans {

    static BisectingKMeans bkm;

    public BKMeans(){
        bkm = new BisectingKMeans();
    }

    /**
     * 训练模型
     * @param df 数据源
     * @param k 聚类数
     * @param numIterations 迭代次数
     * @param minDivisibleClusterSize >= 1.0代表每个类中最小的点数，< 1.0代表每个类最小点数占总点数的比例
     * @return 训练的模型
     */
    public BisectingKMeansModel cluster(DataFrame df,int k, int numIterations, double minDivisibleClusterSize){
       return bkm.cluster(df,k,numIterations,minDivisibleClusterSize);
    }

    /**
     * 预测
     * @param model 经过训练的model
     * @param points 待预测的点
     * @return 该点所在的类
     */
    public int predict(BisectingKMeansModel model, Vector points){
        return bkm.predict(model,points);
    }

    /**
     * 计算误差
     * @param model 经过训练的model
     * @param df 训练数据
     * @return 误差值
     */
    public double computeCost(BisectingKMeansModel model,DataFrame df){
        return bkm.computeCost(model,df);
    }

}
