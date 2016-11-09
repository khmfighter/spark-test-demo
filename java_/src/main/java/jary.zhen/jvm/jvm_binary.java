package jary.zhen.jvm;


/**
 * Created by Administrator on 2016/11/9 0009.
 */
class Value{int val;}
public class jvm_binary {
    public static void main(String [] a){
        int b = Integer.MAX_VALUE+2;
        for (int i = 0 ; i<32 ; i++){
            int t=(b & 0x80000000>>>i)>>>(31-i);
            System.out.println(t);
        }

        int i1 = 3;
        int i2 = i1;
        i2 = 4;
        System.out.print("i1==" + i1);
        System.out.println(" but i2==" + i2);

        Value v1 = new Value();
        v1.val = 5;
        Value v2 = new Value();
        v2.val = 5;
        System.out.print("v1.val==" + v1.val);
        System.out.println(" and v2.val==" + v2.val);
        System.out.println( v2.val==v1.val);
    }
}
