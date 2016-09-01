package utils;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/9/1 0001.
 */
public class Test {
    public static void main(String[] a){
        List<String> ab = new ArrayList();
        ab.add("afa");
        ab.add(0,"Dd");
        ab.add(2,"111");
        ab.add(5,"adsfaf");



        System.out.println(ab);

    }

    public void  makeArry(){

        DecimalFormat df=new DecimalFormat("#.#");

        for(int i =0 ;i<1000;i++) {
            Double x = (Math.random() * 10.0);
            Double y = (Math.random() * 10.0);
            Double z = (Math.random() * 10.0);
            System.out.println(df.format(x) + " " + df.format(y) + " " + df.format(z));
        }
    }
}
