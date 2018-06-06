package com.lqq.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * 测试序列文件
 */
public class TestSeqFile {

    @Test
    public void testWrite() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path("d:/seq/1.seq");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, p, IntWritable.class, Text.class);
        IntWritable key;
        Text value;
        for(int i = 0;i< 10;i++){
            key = new IntWritable(i);
            value = new Text("tom"+i);
            writer.append(key,value);
        }
        for(int i = 0;i< 10;i++){
            key = new IntWritable(i);
            value = new Text("tom"+i);
            writer.append(key,value);
        }

        writer.close();
    }
}
