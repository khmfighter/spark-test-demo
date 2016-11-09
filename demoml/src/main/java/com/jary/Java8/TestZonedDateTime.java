package com.jary.Java8;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Created by Administrator on 2016/11/8 0008.
 */
public class TestZonedDateTime {
    public static void main(String [] a){

        System.out.println(ZonedDateTime.now() +"  id :"+ ZoneId.systemDefault());
        ZonedDateTime dateTime = ZonedDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy :HH:mm:ss");
        String date = dateTime.format(formatter);
        System.out.println(date);


        System.out.println(new Date());
        Date date1 = new Date();
        DateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(format2.format(date1));
    }
}
