package cn.nuecloud.bigdata.dasuan.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Administrator on 2016/10/8 0008.
 */
public class ConfigReaderTest {
    @Test
    public void add() throws Exception {
        ConfigReader configReader = new ConfigReader();
        int result = configReader.add(10,10);
        Assert.assertEquals(30, result);
    }

    public void add2() throws Exception {
        ConfigReader configReader = new ConfigReader();
        int result = configReader.add(10,10);
        Assert.assertEquals(30, result);
    }

}