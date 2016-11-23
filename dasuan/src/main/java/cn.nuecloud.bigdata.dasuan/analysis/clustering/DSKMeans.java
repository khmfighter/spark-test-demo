package cn.nuecloud.bigdata.dasuan.analysis.clustering;

import cn.neucloud.bigdata.taskserver.bean.Task;
import cn.neucloud.bigdata.taskserver.bean.TaskLog;
import cn.neucloud.bigdata.taskserver.bean.TaskLogSchema;
import cn.neucloud.bigdata.taskserver.bean.TaskPlayLoad;
import cn.neucloud.bigdata.taskserver.service.TaskLogSchemaService;
import cn.neucloud.bigdata.taskserver.service.TaskLogService;
import cn.neucloud.bigdata.taskserver.service.TaskPlayLoadService;
import cn.neucloud.bigdata.taskserver.service.TaskService;
import cn.nuecloud.bigdata.dasuan.analysis.Basis;
import cn.nuecloud.bigdata.dasuan.analysis.DSTag;
import cn.nuecloud.bigdata.dasuan.analysis.k_means.K_Means;
import cn.nuecloud.bigdata.dasuan.server.Run;
import cn.nuecloud.bigdata.dasuan.util.DSLogger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.util.*;

/**
 * Created by Jary on 2016/9/2 0002.
 *
 * @example {{{
 * String datadir = "data/mllib/kmeans_data.txt";
 * <p> create the SparkContext
 * SparkContext sc = new SparkContextImpl("this.name");
 * DSKMeans kms = new DSKMeans(datadir,sc);
 * KMeansModel kmd = kms.clusters(3, 29);
 * <p>print the centers </p>
 * kms.printClusters(kmd);
 * System.out.println("轮廓系数：" + kms.silin(kmd));
 * System.out.println("欧几里得：" + kms.euclid(kmd));
 * <p> must stop the sc </p>
 * kms.stop();
 * <p>
 * }}}
 */
public class DSKMeans implements Basis{

    DSLogger loger = new DSLogger(this.getClass().getName().toString());

    static K_Means km_scala = null;
    KMeansModel kmd = null;

    /**
     * @param datadir
     * @param jsc
     */
    public DSKMeans(String datadir, JavaSparkContext jsc) {

        SparkContext sc = JavaSparkContext.toSparkContext(jsc);
        km_scala = new K_Means(datadir, sc);
    }

    /**
     * @param df
     * @param jsc
     */
    public DSKMeans(DataFrame df, JavaSparkContext jsc) {
        SparkContext sc = JavaSparkContext.toSparkContext(jsc);
        km_scala = new K_Means(df, sc);
    }

    /**
     * 聚类
     *
     * @param k           分类数
     * @param numitarator 最大迭代次数
     * @return KMeansModel 训练后的模型
     */
    public KMeansModel clusters(int k, int numitarator) {
        kmd = km_scala.clusterCenters(k, numitarator);
        return kmd;
    }

    public DataFrame getAnalyizedData(KMeansModel kmm) {

        return km_scala.getAnalizedData(kmm);
    }

    public DataFrame getAnalyizedData() {

        return km_scala.getAnalizedData(kmd);
    }
    @Override
    public boolean toVisualize(float rate)  {
        Run run = Run.getInstance();
        TaskService taskService = run.getBean(TaskService.class);
        TaskLogService taskLogService = run.getBean(TaskLogService.class);
        TaskLogSchemaService taskLogSchemaService = run.getBean(TaskLogSchemaService.class);

        //add task
        Task task = new Task();
        task.setAlgorithmId(DSTag.DS_KMEANS);
        task.setCreateTime(new Date());
        task.setDescription("test in proframming");
        int taskId = taskService.addGetId(task);//should reture task id
        //add log schema
        TaskLogSchema schema = TaskLogSchema.builder().taskId(taskId).build();
        schema.setSchema(new String[]{"x", "y","z"});
        taskLogSchemaService.add(schema);

        JavaRDD<Row> javaRDD = getAnalyizedData().javaRDD();
        int partitions = javaRDD.getNumPartitions();
        List<Row> list = new ArrayList<>();
        List<TaskLog> taskLogs = new ArrayList<>();
        // add tasklog

        for (int i = 0; i < partitions; i++) {
            list = javaRDD.collectPartitions(new int[]{i})[0];
            for (int j = 0; j < list.size(); j++) {
                Row row = list.get(j);
                TaskLog taskLog = TaskLog.builder().taskId(taskId).build();
                String[] arr = row.toString().replaceAll("[\\[\\]]", "").split(",");
                taskLog.setRaw(arr);
                taskLogs.add(taskLog);
                if (taskLogs.size() >= 1000) {
                    try {
                        taskLogService.add(taskLogs);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                    taskLogs = new ArrayList<>();
                }
            }
        }

        TaskPlayLoadService taskPlayLoadService = run.getBean(TaskPlayLoadService.class);
        TaskPlayLoad playLoad = TaskPlayLoad.builder().taskId(taskId).build();
        Map<String, Object> map = new HashMap<>();
        map.put("中心点",getClusters());
        map.put("欧几里得距离",getEuclid());
        map.put("轮廓系数",  getSilin());
        playLoad.setPlayload(map);
        taskPlayLoadService.add(playLoad);

        return true;
    }

    /**
     * 打印所有中心点
     *
     * @param kmd 模型
     */
    public void printClusters(KMeansModel kmd) {
        List<String> clusterS = km_scala.printCenters(kmd);
        for (String i : clusterS) {
            System.out.println(i);
        }
    }
    public List getClusters(){
        List<String> clusterS = km_scala.printCenters(kmd);
        return clusterS;
    }
    /**
     * 预测
     *
     * @param v   向量点
     * @param kmd 模型
     * @return 该向量点属于的类
     */
    public int predict(Vector v, KMeansModel kmd) {
        int predict = kmd.predict(v);
        return predict;
    }

    /**
     * 欧几里得 它越小，说明聚类越好
     *
     * @param kmd 模型
     * @return 欧几里得距离
     */
    public double euclid(KMeansModel kmd) {
        double oji = km_scala.ouj(kmd);
        return oji;
    }
    public Double getEuclid() {
        double oji = km_scala.ouj(kmd);
        return oji;
    }

    /**
     * @param kmd 模型
     * @return 轮廓系数  [-1,1] 越大分类越好
     */
    public double silin(KMeansModel kmd) {
        double d = km_scala.silin(kmd);
        return d;
    }
    public Double getSilin() {
        double d = km_scala.silin(kmd);
        return d;
    }
    /**
     * 结束计算
     */
    public void stop() {
        km_scala.stop();
    }

    public static void main(String[] a) {
        String datadir = "data/mllib/kmeans_data.txt";
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("sss").setMaster("local[4]"));
        DSKMeans kms = new DSKMeans(datadir, sc);
        //KMeansModel kmd = kms.clusters(3, 29);
        ////print the centers </p>
        //kms.printClusters(kmd);
        //System.out.println("轮廓系数：" + kms.silin(kmd));
        //System.out.println("欧几里得：" + kms.euclid(kmd));
        ////* <p> must stop the sc </p>
        //kms.stop();

        kms.tolog();
    }
    void tolog (){
        //loger.setOff_AnalysisLog();
        loger.loginfo_a("haaaaaaaa23");
    }
}
