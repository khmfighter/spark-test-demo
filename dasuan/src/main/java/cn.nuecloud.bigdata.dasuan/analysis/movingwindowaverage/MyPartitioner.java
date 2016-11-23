package cn.nuecloud.bigdata.dasuan.analysis.movingwindowaverage;

import org.apache.spark.Partitioner;

/**
 * 自定义partitioner 以key值作为partition索引
 *
 * @author xuhaifeng
 * @version Neucloud2016 2016-11-09
 */
public class MyPartitioner extends Partitioner {

    public MyPartitioner(int num) {
        super();
        numPartition = num;
    }
    private  int numPartition;
    @Override
    public int numPartitions() {
        return numPartition;
    }

    @Override
    public int getPartition(Object key) {
        return ((Integer)key).intValue();
    }
}
