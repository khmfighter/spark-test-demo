package cn.nuecloud.bigdata.dasuan.analysis.stat;


import cn.nuecloud.bigdata.dasuan.analysis.bean.HtResultBean;
import cn.nuecloud.bigdata.dasuan.exception.MyException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.sql.*;

/**
 * 假设检验类，包括适合性检验，独立性检验，均值检验，方差检验
 *
 * @author xuhaifeng
 * @version Neucloud@2016 2016-09-01
 */
public class HypothesisTesting {

    public static void main(String[] args) {

        java.util.Vector<Double> vector = new java.util.Vector<>();
        vector.add(6.00);
        vector.add(5.00);
        vector.add(6.00);
        vector.add(5.00);
        vector.add(5.00);
        vector.add(6.00);
        vector.add(6.00);
        vector.add(6.00);
        vector.add(5.00);
        vector.add(6.00);
        try {
            HtResultBean ht = uTest(vector, 5.5, 0.5, 0.005);
        } catch (MyException e) {
            e.printStackTrace();
        }
    }

    /**
     * 独立性检验
     *
     * @param name   检验算法（默认为Peason卡方）
     * @param counts 频数矩阵
     * @return
     * @throws MyException
     */
    public static ChiSqTestResult independence(String name, Matrix counts) throws MyException {

        if (name != "Pearson") {
            throw new MyException("The name of the algorithm that is not valid");
        }
        if (0 == counts.numRows() || 0 == counts.numCols()) {
            throw new MyException("Matrix row or column length cannot be 0");
        }
        return Statistics.chiSqTest(counts);
    }

    /**
     * 独立性检验
     *
     * @param name 检验算法
     * @param data 检验数据
     * @return
     * @throws MyException
     */
    public static ChiSqTestResult[] labeledPointIndependence(String name, JavaRDD<LabeledPoint> data) throws MyException {
        if ("Pearson" != name) {
            throw new MyException("The name of the algorithm that is not vali");
        }

        return Statistics.chiSqTest(data);
    }

    /**
     * 适合性检验
     *
     * @param name      算法（默认为Peason 卡方）
     * @param observed  观测值
     * @param exception 理论值
     * @return
     * @throws MyException
     */
    public static ChiSqTestResult goodnessOffit(String name, Vector observed, Vector exception) throws MyException {

        if ("Pearson" != name) {
            throw new MyException("The name of the algorithm that is not valid");
        }
        if (0 == observed.size() || 0 == exception.size() || observed.size() != exception.size()) {
            throw new MyException("Illegal observed or expected value");
        }
        return Statistics.chiSqTest(observed, exception);
    }

    /**
     * 独立性检验
     *
     * @param name 算法(默认Peason卡方)
     * @param data 原始数据
     * @return
     * @throws MyException
     */
    public static ChiSqTestResult[] independence(String name, JavaRDD<Row> data) throws MyException {

        if (name != "Pearson") {
            throw new MyException("The name of the algorithm that is not valid");
        }
        JavaRDD<LabeledPoint> labeledPointJavaRDD = data.map(new Function<Row, LabeledPoint>() {
            @Override
            public LabeledPoint call(Row row) throws Exception {
                double labeled = Double.valueOf(row.get(0).toString());
                double[] arr = new double[row.size() - 1];

                for (int i = 0; i < row.size() - 1; i++) {
                    arr[i] = Double.valueOf(row.get(i + 1).toString());
                }
                Vector vector = Vectors.dense(arr);
                return new LabeledPoint(labeled, vector);
            }
        });

        return Statistics.chiSqTest(labeledPointJavaRDD.rdd());
    }

    /**
     * u均值检验
     *
     * @param observed
     * @param checkMean
     * @param sVariance
     * @param significance
     * @return
     * @throws MyException
     */
    public static HtResultBean uTest(java.util.Vector<Double> observed, double checkMean, double sVariance,
                                     double significance) throws MyException {

        if (observed.isEmpty() || sVariance < 0.0 || significance < 0.0) {
            throw new MyException("The observed value is empty or the standard deviation is less than 0 or the significance level is less than 0");
        }
        //计算u值
        double sum = 0.0;
        for (int i = 0; i < observed.size(); i++) {
            sum += observed.get(i);
        }
        double mean = sum / observed.size();
        double u = (mean - checkMean) / sVariance;

        //查表
        HtResultBean htResultBean = new HtResultBean();
        htResultBean.setPvalue(u);
        htResultBean.setFreedom(observed.size() - 1);
        //取前四位数作为估计值
        double uTemp = (int) (u * 1000) / 1000.0;
        //取前两位作为行
        int rowNum = ((int) (uTemp * 10)) / 10;
        if (rowNum >= 49) {
            //拒绝假设
            htResultBean.setConclusion("Reject null hypothesis");
            return htResultBean;
        }
        //取第三位作为列
        int colNum = ((int) (uTemp * 100) % 10);
        double pValue = 0.0;
        try {
            pValue = ReadCSV.read(rowNum + 1, colNum + 1, "data/statisticChart/u-Test.csv");
        } catch (MyException e) {
            e.printStackTrace();
        }

        if (2 * (1 - pValue) < 1 - significance) {
            htResultBean.setConclusion("Reject null hypothesis");
        } else {
            htResultBean.setConclusion("Accept null hypothesis");
        }
        return htResultBean;
    }

    /**
     * t均值检验
     *
     * @param observed     观测值（样本）
     * @param checkMean    待检验的均值
     * @param significance 显著性
     * @return
     * @throws MyException
     */

    public static HtResultBean tTest(java.util.Vector<Double> observed, double checkMean, double significance) throws MyException {
        if (observed.isEmpty() || significance < 0) {
            throw new MyException("Illegal input value");
        }
        //计算t值
        double sampleMean = 0.0;
        double sVariance = 0.0;
        for (int i = 0; i < observed.size(); i++) {
            sampleMean += observed.get(i);
        }
        sampleMean = sampleMean / observed.size();

        for (int i = 0; i < observed.size(); i++) {
            sVariance += Math.pow(observed.get(i) - sampleMean, 2);
        }
        sVariance = Math.sqrt(sVariance / observed.size());

        double t = Math.abs(sampleMean - checkMean) / (sVariance / Math.sqrt(observed.size() - 1));

        //查表
        HtResultBean result = new HtResultBean();
        result.setStatistic(t);
        result.setPvalue(significance);
        int sigTemp = (int) (significance * 10000);
        int col;
        switch (sigTemp) {
            case 2500:
                col = 1;
                break;
            case 2000:
                col = 2;
                break;
            case 1500:
                col = 3;
                break;
            case 1000:
                col = 4;
                break;
            case 500:
                col = 5;
                break;
            case 250:
                col = 6;
                break;
            case 100:
                col = 7;
                break;
            case 50:
                col = 8;
                break;
            case 25:
                col = 9;
                break;
            case 10:
                col = 10;
                break;
            case 5:
                col = 11;
                break;
            default:
                throw new MyException("significant illegal");
        }
        double tValue = 0.0;
        try {
            tValue = ReadCSV.read(observed.size() - 1, col, "data/statisticChart/T-Test.csv");
        } catch (MyException e) {
            e.printStackTrace();
        }

        if (tValue < t) {
            result.setConclusion("Reject null hypothesis");
        } else {
            result.setConclusion("Accept null hypothesis");
        }
        return result;
    }

    /**
     * 卡方方差检验
     *
     * @param observed      样本
     * @param checkVariance 待检验的方差
     * @param significance  显著性
     * @return
     * @throws MyException
     */
    public static HtResultBean kfTest(java.util.Vector<Double> observed, double checkVariance, double significance)
            throws MyException {

        if (observed.isEmpty() || checkVariance < 0 || significance < 0) {
            throw new MyException("Illegal input value");
        }
        double sampleMean = 0.0;
        double variance = 0.0;
        for (int i = 0; i < observed.size(); i++) {
            sampleMean += observed.get(i);
        }
        sampleMean = sampleMean / observed.size();
        for (int i = 0; i < observed.size(); i++) {
            variance += Math.pow(observed.get(i) - sampleMean, 2);
        }
        variance = variance / observed.size();
        double kf = (observed.size() - 1) * variance / checkVariance;

        int col1 = 0;
        int col2 = 0;
        int sigTemp1 = (int) (significance * 1000);
        switch (sigTemp1) {
            case 995:
                col1 = 1;
                col2 = 13;
                break;
            case 990:
                col1 = 2;
                col2 = 12;
                break;
            case 975:
                col1 = 3;
                col2 = 11;
                break;
            case 950:
                col1 = 4;
                col2 = 10;
                break;
            case 900:
                col1 = 5;
                col2 = 9;
                break;
            case 750:
                col1 = 6;
                col2 = 8;
                break;
            case 500:
                col1 = 7;
                col2 = 7;
                break;
            case 250:
                col1 = 8;
                col2 = 6;
                break;
            case 100:
                col1 = 9;
                col2 = 5;
                break;
            case 50:
                col1 = 10;
                col2 = 4;
                break;
            case 25:
                col1 = 11;
                col2 = 3;
                break;
            case 10:
                col1 = 12;
                col2 = 2;
                break;
            case 5:
                col1 = 13;
                col2 = 1;
                break;
            default:
                throw new MyException("significant illegal");
        }
        //查表
        HtResultBean zResult = new HtResultBean();
        zResult.setFreedom(observed.size() - 1);
        zResult.setPvalue(kf);
        double kfValue1 = 0.0;
        double kfValue2 = 0.0;
        try {
            kfValue1 = ReadCSV.read(observed.size() - 1, col1, "data/statisticChart/kf-Test.csv");
            kfValue2 = ReadCSV.read(observed.size() - 1, col2, "data/statisticChart/kf-Test.csv");
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (kfValue1 < kf && kf < kfValue2) {
            zResult.setConclusion("Accept null hypothesis");
        } else {
            zResult.setConclusion("Reject null hypothesis");
        }
        return zResult;
    }

    /**
     * F方差检验
     *
     * @param vector1
     * @param vector2
     * @return
     * @throws MyException
     */
    public static String fTest(java.util.Vector<Double> vector1, java.util.Vector<Double> vector2) throws MyException {

        if (vector1.isEmpty() || vector2.isEmpty()) {
            throw new MyException("Illegal input parameters");
        }
        double mean1 = 0.0;
        double mean2 = 0.0;
        double sVariance1 = 0.0;
        double sVariance2 = 0.0;
        for (int i = 0; i < vector1.size(); i++) {
            mean1 += vector1.get(i);
        }
        mean1 = mean1 / (vector1.size() - 1);
        for (int i = 0; i < vector1.size(); i++) {
            sVariance1 += Math.pow(sVariance1 - mean1, 2);
        }
        sVariance1 = sVariance1 / (vector1.size() - 1);

        for (int i = 0; i < vector2.size(); i++) {
            mean2 += vector2.get(i);
        }
        mean2 = mean2 / vector2.size();
        for (int i = 0; i < vector2.size(); i++) {
            sVariance2 += Math.pow(sVariance2 - mean2, 2);
        }
        sVariance2 = sVariance2 / (vector2.size() - 1);

        double f;
        if (sVariance1 > sVariance2) {
            f = sVariance1 / sVariance2;
        } else {
            f = sVariance2 / sVariance1;
        }
        //查表
        String result;
        double pValue = ReadCSV.read(vector1.size() - 1, vector2.size() - 1, "data/statisticChart/f-Test.csv");
        if (pValue > f) {
            result = "There is no significant difference";
        } else {
            result = "There were significant difference";
        }
        return result;
    }
}
