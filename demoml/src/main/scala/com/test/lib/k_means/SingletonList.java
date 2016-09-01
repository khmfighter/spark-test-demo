package com.test.lib.k_means;

/**
 * Created by Administrator on 2016/8/31 0031.
 */
public class SingletonList {
    private SingletonList(){

    }

    private static SingletonList instance;
    public static synchronized SingletonList getInstance(){
        if(instance == null){
            return instance = new SingletonList();
        }else{
            return instance;
        }
    }
}
