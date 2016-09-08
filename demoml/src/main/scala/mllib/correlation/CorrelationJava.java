package mllib.correlation;

import java.util.Arrays;

/**
 * Created by Administrator on 2016/8/30 0030.
 */
public class CorrelationJava {
    public static double Correlation(double[] sourceDSX,  double[] sourcdDSY){
        double a = Correlations.makeCorrelationDouble(sourceDSX,sourcdDSY);
        return a;
    }
    public static void main(String [] a){
        double[] xData = new double[]{1.0, 0.0, -2.0};

        double[] yData = new double[]{4.0, 5.0, 3.0};

        System.out.println("相关系数："+Correlation(xData,yData));
    }
}
