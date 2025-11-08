package hadoopTest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;

import java.io.IOException;

public class myReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);

        //遍历values
        Long sum = 0l;
        for (LongWritable value : values) {
            sum += value.get();
        }
        //输出，单词+次数
        context.write(key, new LongWritable(sum));
    }
}
