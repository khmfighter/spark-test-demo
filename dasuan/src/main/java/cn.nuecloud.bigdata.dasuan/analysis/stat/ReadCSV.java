package cn.nuecloud.bigdata.dasuan.analysis.stat;


import cn.nuecloud.bigdata.dasuan.exception.MyException;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * 按照给定的行和列返回对应的csv文件中的值
 *
 * @Author xuhaifeng
 * @Version Neucloud@2016 2016-09-07
 */
public class ReadCSV {

    public static double read(int row, int col, String path) throws MyException {

        if (row <= 0 || col <= 0) {
            throw new MyException("illegal row or column");
        }
        double value = 0.0;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line;
            int index = 0;
            while ((line = reader.readLine()) != null) {
                if (index == row) {
                    String item[] = line.split(",");
                    if (item.length >= col - 1) {
                        String last = item[col];
                        value = Double.valueOf(last);
                        break;
                    }
                }
                index++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (0.0 == value) {
            throw new MyException("Row or column exceeds the maximum allowed value");
        }
        return value;
    }
}
