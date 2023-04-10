package org.MapReduce.topN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TPartitioner extends Partitioner<TKey, IntWritable> {

    @Override
    public int getPartition(TKey tKey, IntWritable intWritable, int numPartitions) {
        // todo 会产生数据倾斜 tKey.getYear()%numPartitions
        return tKey.getYear()%numPartitions;
    }
}
