package utils;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/9/1 0001.
 */
public class Test {
    public static void main(String[] a){
        DecimalFormat df=new DecimalFormat("#.#");
        try {
            //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
            FileWriter writer = new FileWriter("data/mllib/test.txt", true);
            for(int i =0 ;i<Integer.MAX_VALUE/10000;i++) {
                Double x = (Math.random() * 10.0);
                Double y = (Math.random() * 10.0);
                Double z = (Math.random() * 10.0);
                System.out.println();
                writer.write(df.format(x) + " " + df.format(y) + " " + df.format(z)+"\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void  makeArry(){

        DecimalFormat df=new DecimalFormat("#.#");

        for(int i =0 ;i<Integer.MAX_VALUE;i++) {
            Double x = (Math.random() * 10.0);
            Double y = (Math.random() * 10.0);
            Double z = (Math.random() * 10.0);
            System.out.println(df.format(x) + " " + df.format(y) + " " + df.format(z));
        }
    }
}
