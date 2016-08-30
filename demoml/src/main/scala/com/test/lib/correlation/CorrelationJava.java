package com.test.lib.correlation;

import java.text.DecimalFormat;

/**
 * Created by Administrator on 2016/8/30 0030.
 */
public class CorrelationJava<V> {
    public V Correlation(V sourceDSX, V sourcdDSY){
        V a = null;
            //a= Correlations.main();
        return a;
    }
    public static void main(String [] a){

        DecimalFormat df=new DecimalFormat("#.#");

        for(int i =0 ;i<1000;i++) {
            Double x = (Math.random() * 10.0);
            Double y = (Math.random() * 10.0);
            Double z = (Math.random() * 10.0);
            System.out.println(df.format(x) + " " + df.format(y) + " " + df.format(z));
        }
    }
}
