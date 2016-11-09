package com.jary;


/**
 * Created by Administrator on 2016/11/8 0008.
 */
public class JVM {

    public static void main(String [] a){
        xmxXms();
    }
    public static void xmxXms(){
        //-Xmx2G -Xms1G
        //
        System.out.println(Runtime.getRuntime().maxMemory()/1024.0/1024+" M");
        System.out.println(Runtime.getRuntime().freeMemory()/1024.0/1024+" M");
        System.out.println(Runtime.getRuntime().totalMemory()/1024.0/1024+" M");
        System.out.println();
        byte[] b = new byte[2*1024*1024]; //1 M

        System.out.println(Runtime.getRuntime().maxMemory()/1024.0/1024+" M");
        System.out.println(Runtime.getRuntime().freeMemory()/1024.0/1024+" M");
        System.out.println(Runtime.getRuntime().totalMemory()/1024.0/1024+" M");
        System.out.println();
        System.gc();
        System.out.println(Runtime.getRuntime().maxMemory()/1024.0/1024+" M");
        System.out.println(Runtime.getRuntime().freeMemory()/1024.0/1024+" M");
        System.out.println(Runtime.getRuntime().totalMemory()/1024.0/1024+" M");
        System.out.println();
    }
}
