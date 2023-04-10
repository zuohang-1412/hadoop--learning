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

        job.setJobName("TopN");
        job.setJar("target/hadoop-learning-1.0-SNAPSHOT.jar");

        //客户端规划的时候讲join的右表cache到mapTask出现的节点上
        job.addCacheFile(new Path("/data/topn/dict/dict.txt").toUri());

        // MapTask
        TextInputFormat.addInputPath(job, new Path(others[0]));

        Path outPath = new Path(others[1]);

        if (outPath.getFileSystem(conf).exists(outPath)) outPath.getFileSystem(conf).delete(outPath, true);
        TextOutputFormat.setOutputPath(job, outPath);
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
