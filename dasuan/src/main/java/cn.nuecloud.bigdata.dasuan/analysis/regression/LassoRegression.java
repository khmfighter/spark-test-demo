package cn.nuecloud.bigdata.dasuan.analysis.regression;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LassoModel;

/**
 * Lasso Regression
 * @author xuhaifeng
 * @version Neucloud2016 2016-11-08
 */
public class LassoRegression {

    public static LassoRegression lassoRegression = null;

    public LassoRegression(){
        lassoRegression = new LassoRegression();
    }

    /**
     * lasso 模型训练
     * @param numIterations 迭代次数
     * @param stepSize  迭代步长
     * @param regParam 正则化参数
     * @param miniBatchFraction 每次参与迭代的样本比例
     * @return
     */
    public LassoModel lasso(JavaRDD<LabeledPoint> dataSource,int numIterations,double stepSize,double regParam,double miniBatchFraction){
        return lassoRegression.lasso(dataSource,numIterations,stepSize,regParam,miniBatchFraction);
    }

    /**
     * 单点预测
     * @param model lasso model
     * @param testData 待预测数据
     * @return
     */
    public double predict(LassoModel model,Vector testData){
        return lassoRegression.predict(model,testData);
    }

    /**
     * 多点预测
     * @param model lasso model
     * @param testData 待预测数据
     * @return
     */
    public JavaRDD<Double> predict(LassoModel model, JavaRDD<Vector> testData){
        return lassoRegression.predict(model,testData);
    }

    /**
     * @param model
     */
    public void printWeights(LassoModel model){
        lassoRegression.printWeights(model);
    }
}
