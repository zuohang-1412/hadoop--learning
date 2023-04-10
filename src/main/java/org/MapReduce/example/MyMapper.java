package org.MapReduce.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    // 因为需要排序，排序分为： 字典序、数值序
    // 数据传输 涉及序列化和反序列化

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    // 默认使用文本
    // key： 是每一行字符串中第一个字符面向文件的偏移量
    // value：每一行字符串
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
