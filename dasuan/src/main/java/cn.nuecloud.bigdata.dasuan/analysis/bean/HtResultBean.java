package cn.nuecloud.bigdata.dasuan.analysis.bean;

/**
 * 假设检验返回值类
 *
 * @author xuhaifeng
 * @version Neucloud2016 2016-09-09
 */
public class HtResultBean {

    private int freedom;
    private double pValue;
    private double statistic;
    private String conclusion;

    public int getFreedom() {
        return freedom;
    }

    public void setFreedom(int freedom) {
        this.freedom = freedom;
    }

    public double getPvalue() {
        return pValue;
    }

    public void setPvalue(double pValue) {
        this.pValue = pValue;
    }

    public double getStatistic() {
        return statistic;
    }

    public void setStatistic(double statistic) {
        this.statistic = statistic;
    }

    public String getConclusion() {
        return conclusion;
    }

    public void setConclusion(String conclusion) {
        this.conclusion = conclusion;
    }
}
