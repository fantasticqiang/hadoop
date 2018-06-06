package com.lqq.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * 测试压缩
 */
public class TestCompress {

    @Test
    public void test() throws Exception {
        Class clazz = DefaultCodec.class;
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(clazz,new Configuration());
        FileOutputStream fos = new FileOutputStream("h:/payorderJson/1.deflate");
        //得到压缩流
        CompressionOutputStream zipOut = codec.createOutputStream(fos);
        IOUtils.copyBytes(new FileInputStream("h:/payorderJson/0-400000_pay_order.json"),zipOut,4096);
        zipOut.close();
    }
}
