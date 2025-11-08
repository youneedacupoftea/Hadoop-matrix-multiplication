package dataPretreat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class myMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //创建job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        System.setProperty("user.name", "root");
        System.setProperty("HADOOP_USER_NAME", "root");

        //配置job类
        job.setJarByClass(myMain.class);

        //配置map reduce
        job.setMapperClass(myMapper.class);
        job.setReducerClass(myReduce.class);

        //配置输入格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //配置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //配置文件路径
        FileInputFormat.setInputPaths(job, new Path("D:\\hello world\\java practice\\hadoopDemo1\\input\\ma.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\hello world\\java practice\\hadoopDemo1\\output\\out5_3"));

        //调用job
        boolean req = job.waitForCompletion(true);

        System.exit(req ? 0 : 1);
    }
}
