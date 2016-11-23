package cn.nuecloud.bigdata.dasuan.analysis.clustering;

import cn.nuecloud.bigdata.dasuan.analysis.gaussianmixture.GaussianMixture_N;
import org.apache.spark.mllib.clustering.GaussianMixtureModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;

/**
 * 高斯混合聚类
 * @author xuhaifeng
 * @version Neucloud2016 2016-10-24
 */
public class GaussianMixture {

    public static GaussianMixture_N gm = null;

    /**
     * 构造函数
     */
    public GaussianMixture(){
        gm = new GaussianMixture_N();
    }

    /**
     * 聚类函数
     * @param df 数据源
     * @param k 聚类数
     * @param numIterations 迭代次数
     * @param convergenceTol 收敛误差
     * @param seed
     * @return
     */
    public GaussianMixtureModel cluster(DataFrame df,int k,int numIterations,double convergenceTol,long seed){
        return gm.cluster(df,k,numIterations,convergenceTol,seed);
    }

    /**
     * 单点预测
     * @param model 高斯混合模型
     * @param points 待预测的点
     * @return 该点所在类
     */
    public int predict(GaussianMixtureModel model, Vector points){
        return gm.predict(model,points);
    }

    /**
     * 多点预测
     * @param model 高斯混合模型
     * @param points 待分类的点
     * @return 个点所在类组成的rdd
     */
    public RDD predict(GaussianMixtureModel model,RDD points){
        return gm.predict(model,points);
    }

    /**
     * 打印各高斯函数投影的参数
     * @param model 高斯混合模型
     * @param points 待分类的点
     */
    public void printComponents(GaussianMixtureModel model,Vector points){
        gm.printComponents(model,points);
    }

    /**
     * 打印各分类的权重
     * @param model 高斯混合模型
     */
    public void printWeights(GaussianMixtureModel model){
        gm.printWeights(model);
    }
}
