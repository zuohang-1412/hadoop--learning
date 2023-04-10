package org.MapReduce.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//hadoop jar hadoop-learning/target/hadoop-learning-1.0-SNAPSHOT.jar org.MapReduce.example.TestWordCount

/* 如果想要参数个性化
* GenericOptionsParser parser = new GenericOptionsParser(conf, args);
* String[] remainingArgs = parser.getRemainingArgs();
* genericOptionsParser 为-D 后的参数 k，v
* remainingArgs 为 input 和 output 路径
* */
public class TestWordCount {
    public static void main(String[] args) throws Exception {
        // Create a new Job
        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf);

        //  这个步骤是必须有的，是为了 MyJob = TestWordCount， 通过jar 包反推程序使用的类
        job.setJarByClass(TestWordCount.class);

        // Specify various job-specific parameters
        job.setJobName("wc");
        // 报错：Error: java.lang.RuntimeException: java.lang.ClassNotFoundException: Class org.MapReduce.example.MyMapper not found
        job.setJar("target/hadoop-learning-1.0-SNAPSHOT.jar");

        // 这个已经过时了
        // job.setInputPath(new Path("in"));
        // job.setOutputPath(new Path("out"));

        Path input = new Path("/data/hello.txt");
        TextInputFormat.addInputPath(job, input);

        Path output = new Path("/data/1.txt");
        if (output.getFileSystem(conf).exists(output)) {
            boolean delete = output.getFileSystem(conf).delete(output, true);
            System.out.println(delete);
        }
        TextOutputFormat.setOutputPath(job, output);

        // 计算向数据移动
        job.setMapperClass(MyMapper.class);
        // 添加序列化类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
