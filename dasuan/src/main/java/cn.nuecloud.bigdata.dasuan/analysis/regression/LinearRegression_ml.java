package cn.nuecloud.bigdata.dasuan.analysis.regression;


import cn.neucloud.bigdata.taskserver.bean.Task;
import cn.neucloud.bigdata.taskserver.bean.TaskLog;
import cn.neucloud.bigdata.taskserver.bean.TaskLogSchema;
import cn.neucloud.bigdata.taskserver.bean.TaskPlayLoad;
import cn.neucloud.bigdata.taskserver.service.TaskLogSchemaService;
import cn.neucloud.bigdata.taskserver.service.TaskLogService;
import cn.neucloud.bigdata.taskserver.service.TaskPlayLoadService;
import cn.neucloud.bigdata.taskserver.service.TaskService;
import cn.nuecloud.bigdata.dasuan.analysis.Basis;
import cn.nuecloud.bigdata.dasuan.analysis.bean.LRBean;
import cn.nuecloud.bigdata.dasuan.exception.MyException;

import cn.nuecloud.bigdata.dasuan.server.Run;
import cn.nuecloud.bigdata.utils.DSLogging;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.*;

/**
 * @example{{{ SparkConf conf new SparkConf().setAppName("test").setMaster("local[20]").set("spark.default.parallelism","1");
 * JavaSparkContext jsc =new JavaSparkContext(conf);
 * SQLContext sqlContext = new SQLContext(jsc);
 * DataFrame dt = sqlContext.read().json("data/testFile/normalization.json");
 * LRBean bb= new LRBean();
 * bb.setRegParam(2);
 * LinearRegression_ml lr = new LinearRegression_ml();
 * LinearRegressionModel aa=lr.LinearRegression_ml(dt,bb,jsc);
 * double[] d = new double[]{4.0};
 * Vector v = Vectors.dense(d);
 * lr.predict(aa,v);
 * }}}
 */

/**
 * 线性回归
 *
 * @version Neucloud2016 2016-11-03
 * @auctor qingda
 */
public class LinearRegression_ml implements java.io.Serializable {
    DataFrame source;
    public LinearRegressionModel LinearRegression_ml(DataFrame data_DF, LRBean bean, JavaSparkContext jsc) throws MyException {
        //    data_DF.show();
        source=data_DF;
        data_DF.persist(StorageLevel.DISK_ONLY());
        data_DF.unpersist();
        JavaRDD<Row> rows = data_DF.javaRDD();
        JavaRDD<Row> row1 = rows.map(new Function<Row, Row>() {
            public Row call(Row line) {
                String y = line.getString(0);
                String x = line.getString(1);
                double[] lr_dense = new double[1];
                lr_dense[0] = Double.valueOf(x);
                double label = Double.valueOf(y);
                return RowFactory.create(label, Vectors.dense(lr_dense).toSparse());
            }
        });
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false,
                        Metadata.empty()),
                new StructField("features", new VectorUDT(), false,
                        Metadata.empty())});
        SQLContext sqlContext1 = new SQLContext(jsc);
        DataFrame resultDF = sqlContext1.createDataFrame(row1, schema);
        //    resultDF.show();
        if (bean.getElasticNetParam() < 0 || bean.getElasticNetParam() > 1 || bean.getMaxIter() <= 0 || bean.getRegParam() < 0 || bean.getRegParam() > 1) {
            throw new MyException("Illegal input value");
        }
        LinearRegression lr = new LinearRegression()
                /**
                 * Set the ElasticNet mixing parameter.
                 * For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty.
                 * For 0 < alpha < 1, the penalty is a combination of L1 and L2.
                 * Default is 0.0 which is an L2 penalty.
                 * @group setParam
                 */
                .setElasticNetParam(bean.getElasticNetParam())     //value: Double elasticNetParam -> 0.0
                /**
                 * Set if we should fit the intercept
                 * Default is true.
                 * @group setParam
                 */
                .setFitIntercept(bean.isFitIntercept())       //fitIntercept -> true
                /**
                 * Set the maximum number of iterations.
                 * Default is 100.
                 * @group setParam
                 */
                .setMaxIter(bean.getMaxIter())        //value: Int  maxIter -> 100    迭代步长
                /**
                 * Set the regularization parameter.
                 * Default is 0.0.
                 * @group setParam
                 */
                .setRegParam(bean.getRegParam())      //value: Double  regParam -> 0.0
                /**
                 * Set the solver algorithm used for optimization.
                 * In case of linear regression, this can be "l-bfgs", "normal" and "auto".
                 * "l-bfgs" denotes Limited-memory BFGS which is a limited-memory quasi-Newton
                 * optimization method. "normal" denotes using Normal Equation as an analytical
                 * solution to the linear regression problem.
                 * The default value is "auto" which means that the solver algorithm is
                 * selected automatically.
                 * @group setParam
                 */
                .setSolver(bean.getSolver())      //value: String  solver -> "auto"
                /**
                 * Whether to standardize the training features before fitting the model.
                 * The coefficients of models will be always returned on the original scale,
                 * so it will be transparent for users. Note that with/without standardization,
                 * the models should be always converged to the same solution when no regularization
                 * is applied. In R's GLMNET package, the default behavior is true as well.
                 * Default is true.
                 * @group setParam
                 */
                .setStandardization(bean.isStandardization())   //value: Boolean  standardization -> true
                /**
                 * Set the convergence tolerance of iterations.
                 * Smaller value will lead to higher accuracy with the cost of more iterations.
                 * Default is 1E-6.
                 * @group setParam
                 */
                .setTol(bean.getTol())           //value: Double  tol -> 1E-6
                /**
                 * Whether to over-/under-sample training instances according to the given weights in weightCol.
                 * If empty, all instances are treated equally (weight 1.0).
                 * Default is empty, so all instances have weight one.
                 * @group setParam
                 */
                .setWeightCol(bean.getWeightCol());   //value: String weightCol -> ""
        // Fit the model.
        LinearRegressionModel lrModel = lr.fit(resultDF);
        return lrModel;
    }

    Map<String, String> map = new HashMap<String, String>();

    public void predict(LinearRegressionModel lrModel, double d[]) {
        for (int i = 0; i < d.length; i++) {
            Vector v = Vectors.dense(d[i]);
            double result = lrModel.predict(v);
            map.put(String.valueOf(d[i]), String.valueOf(result));
        }
        for (Map.Entry<String, String> entry : map.entrySet()) {

            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }

    public void visualize() throws Exception {
        JavaRDD<Row> reJavaRDD = source.javaRDD();
        Run run = Run.getInstance();
        TaskService taskService = run.getBean(TaskService.class);
        final TaskLogService taskLogService = run.getBean(TaskLogService.class);
        final int id = taskService.addGetId(
                Task.builder()
                        .algorithmId(6)
                        .createTime(new Date())
                        .description("LinearRegression").build());// 为0时表示失败
        int partitions = reJavaRDD.getNumPartitions();
        TaskLogSchemaService taskLogSchemaService = run.getBean(TaskLogSchemaService.class);
        TaskLogSchema schema = TaskLogSchema.builder().taskId(id).build();
        schema.setSchema(new String[]{"Move_Speed", "Rotate_Speed"});
        taskLogSchemaService.add(schema);
        List<TaskLog> taskLogs = new ArrayList<>();
        List<Row> list = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            list = reJavaRDD.collectPartitions(new int[]{i})[0];
            for (int j = 0; j < list.size(); j++) {
                Row row = list.get(j);
                TaskLog taskLog = TaskLog.builder().taskId(id).build();
                String[] arr = row.toString().replaceAll("[\\[\\]]", "").split(",");
                taskLog.setRaw(arr);
                taskLogs.add(taskLog);
                if (taskLogs.size() >= 1000) {
                    taskLogService.add(taskLogs);
                    taskLogs = new ArrayList<>();
                }
            }
            list = null;
            if (taskLogs.size() > 0) {
                taskLogService.add(taskLogs);
            }
            TaskPlayLoadService taskPlayLoadService = run.getBean(TaskPlayLoadService.class);
            TaskPlayLoad playLoad = TaskPlayLoad.builder().taskId(id).build();
            if (map.size() != 0) {
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    String predict_x = entry.getKey();
                    String predict_y = entry.getValue();
                    Map<String, Object> map = new HashMap<>();
                    map.put("predict", predict_x + "|" + predict_y);
                    playLoad.setPlayload(map);
                    taskPlayLoadService.add(playLoad);
                    System.out.println("done!!!!!!!!!!!!");
                }
            }
        }
    }
}