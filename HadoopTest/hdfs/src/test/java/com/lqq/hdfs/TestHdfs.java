package com.lqq.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.InputStream;

/**
 */
public class TestHdfs {


    @Test
    public void testReadFromHdfs() {

        InputStream in = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path("/user/hadoop/ff.txt");
            in = fs.open(path);
            IOUtils.copyBytes(in, System.out, conf, false);
        } catch (Exception e) {}
        finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * 上传文件定制副本数和块大小
     */
    @Test
    public void putFile2Hdfs() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/hadoop/ff.txt");
        FSDataOutputStream out = fs.create(path, true, 1024, (short) 1, 1024);
        out.write("hello 123".getBytes());
        out.close();
    }
}
