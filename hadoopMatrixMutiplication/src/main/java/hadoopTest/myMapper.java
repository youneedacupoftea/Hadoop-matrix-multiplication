package hadoopTest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class myMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        //获取一行文字
        String line = value.toString();
        //分割文本
        String[] vals = line.split(" ");
        //写出
        for (String val : vals) {
            Text text = new Text();
            text.set(val);
            LongWritable longw = new LongWritable(1);
            context.write(text, longw);
        }

    }
}
