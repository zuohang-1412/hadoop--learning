package org.MapReduce.topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class TestTopN {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        String[] others = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf);
        job.setJarByClass(TestTopN.class);
        job.setJar("target/hadoop-learning-1.0-SNAPSHOT.jar");
        job.setJobName("TopN");

        //
        TextInputFormat.addInputPath(job, new Path(others[0]));
        Path outPut = new Path(others[1]);
        if(outPut.getFileSystem(conf).exists(outPut)) {
            System.out.println("已存在");
            boolean del = outPut.getFileSystem(conf).delete(outPut, true);
            System.out.println(del);
        }
        TextOutputFormat.setOutputPath(job, outPut);

        // MapTask
        job.setMapperClass(TMapper.class);

        job.setMapOutputKeyClass(TKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 分区
        job.setPartitionerClass(TPartitioner.class);
        // 比较器
        job.setSortComparatorClass(TSortComparator.class);

        // ReduceTask
        job.setGroupingComparatorClass(TGroupingComparator.class);
        job.setReducerClass(TReducer.class);
        job.waitForCompletion(true);

    }
}
