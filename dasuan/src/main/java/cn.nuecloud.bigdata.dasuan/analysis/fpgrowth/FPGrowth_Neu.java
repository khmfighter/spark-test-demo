package cn.nuecloud.bigdata.dasuan.analysis.fpgrowth;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.FPGrowthModel;

/**
 * 频繁项集与关联规则挖掘
 * @author xuhaifeng
 * @version Neucloud2016 2016-11-08
 */
public class FPGrowth_Neu {

    public static FPGrowth fpGrowth = null;

    /**
     * RDD类型数据源
     * @param jsc
     */
    public FPGrowth_Neu(JavaSparkContext jsc){
        fpGrowth = new FPGrowth(JavaSparkContext.toSparkContext(jsc));
    }

    /**
     * 模型训练
     * @param minSupport 最小支持度
     * @param numPartitions partitions数
     * @return
     */
    public FPGrowthModel<String> fpGrowth(String path,double minSupport, int numPartitions){

        return fpGrowth.fpGrowth(path,numPartitions,minSupport);
    }

    /**
     * 打印符合最小支持度的频繁项集
     * @param model FPG模型
     */
    public void printFreqItem(FPGrowthModel<String> model){
        fpGrowth.printFreqItem(model);
    }

    /**
     * 打印符合最小支持度和最小置信度的关联项
     * @param model FPG模型
     * @param minConfidence 最小置信度
     */
    public void printGenerateAssociation(FPGrowthModel<String> model,Double minConfidence){
        fpGrowth.printGenerateAssociation(model,minConfidence);
    }

}
