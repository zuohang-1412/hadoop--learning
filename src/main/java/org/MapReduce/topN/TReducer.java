package org.MapReduce.topN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TReducer extends Reducer<TKey, IntWritable, Text, IntWritable> {
    Text rkey = new Text();
    IntWritable rval = new IntWritable();

    @Override
    protected void reduce(TKey key, Iterable<IntWritable> values, Reducer<TKey, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iter = values.iterator();
        int flag = 0;
        int day = 0;
        while (iter.hasNext()) {
            if (flag == 0) {
                rkey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + "@" + key.getWd());
                rval.set(key.getWd());
                context.write(rkey, rval);
                flag++;
                day = key.getDay();
            }

            if (flag != 0 && day != key.getDay()) {
                rkey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + "@" + key.getWd());
                rval.set(key.getWd());
                context.write(rkey, rval);
                break;
            }
        }
    }
}
