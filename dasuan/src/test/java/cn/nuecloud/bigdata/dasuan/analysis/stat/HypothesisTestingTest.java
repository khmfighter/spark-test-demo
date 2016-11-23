package cn.nuecloud.bigdata.dasuan.analysis.stat;

import cn.nuecloud.bigdata.dasuan.analysis.bean.HtResultBean;
import org.junit.Before;
import org.junit.Test;

/**
 * @author xuhaifeng
 * @version Neucloud2016 2016-10-28
 */
public class HypothesisTestingTest {

    private java.util.Vector<Double> vector = new java.util.Vector(){};

    @Before
    public void setUp(){
        //生成一组样本 均值为4 标准差为1.7
        vector.add(0,1.0);
        vector.add(1,2.0);
        vector.add(2,7.0);
        vector.add(3,3.0);
        vector.add(4,3.0);
        vector.add(5,4.0);
        vector.add(6,4.0);
        vector.add(7,5.0);
        vector.add(8,5.0);
        vector.add(9,6.0);
    }

    @Test
    public void testIndependence() throws Exception {

    }

    @Test
    public void testLabeledPointIndependence() throws Exception {

    }

    @Test
    public void testGoodnessOffit() throws Exception {

    }

    @Test
    public void testIndependence1() throws Exception {

    }

    @Test
    public void testUTest() throws Exception {

        HtResultBean htResultBean1 = HypothesisTesting.uTest(vector,3.0,0.01,1.7);
        System.out.println(htResultBean1.getConclusion());
        System.out.println(htResultBean1.getFreedom());
        System.out.println(htResultBean1.getStatistic());
        System.out.println(htResultBean1.getPvalue());
        HtResultBean htResultBean2 = HypothesisTesting.uTest(vector,3.9,0.01,1.7);
        System.out.println(htResultBean2.getConclusion());
        System.out.println(htResultBean2.getFreedom());
        System.out.println(htResultBean2.getStatistic());
        System.out.println(htResultBean2.getPvalue());

    }

    @Test
    public void testTTest() throws Exception {
        HtResultBean htResultBean = HypothesisTesting.tTest(vector,3.9,0.01);
        System.out.println(htResultBean.getConclusion());
        System.out.println(htResultBean.getFreedom());
        System.out.println(htResultBean.getStatistic());
        System.out.println(htResultBean.getPvalue());
    }

    @Test
    public void testKfTest() throws Exception {

    }

    @Test
    public void testFTest() throws Exception {

    }
}