package com.lqq.hdfs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * word count mapper类
 */
public class WcMapper extends Mapper<LongWritable,Text,Text,IntWritable>{


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text keyOut = new Text();
        IntWritable valueOut = new IntWritable();
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while(tokenizer.hasMoreTokens()){
            keyOut.set(tokenizer.nextToken());
            valueOut.set(1);
            context.write(keyOut,valueOut);
        }
    }
}
