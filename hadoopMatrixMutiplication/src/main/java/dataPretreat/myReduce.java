package dataPretreat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.lang.Math;
import java.io.IOException;

public class myReduce extends Reducer<Text, Text, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long tot = 0;  //六个最接近的数之和
        double m = 6;
        long initialValue = 0; //中间的数据
        long[] x = new long[8];
        int index = 0;
        for (Text value : values) {
            String[] val = value.toString().split(",");
            // -1 表示初始值
            // 1 表示周围的值
            if ("-1".equals(val[0])) {
                initialValue = Long.parseLong(val[1]);
                //context.write(key, new LongWritable(initialValue));
            } else {
                //存周边的数据
                x[index] = Long.parseLong(val[1]);
                index++;
            }
        }
        //剔除差别大的数据
        long c = -1;
        int flag = -1;
        for (int i = 0; i < 8; i++) {
            long t = Math.abs(x[i] - initialValue);
            if (t > c) {
                flag = i;
                c = t;
            }
        }
        x[flag] = -1;

        c = -1;
        flag = -1;
        for (int i = 0; i < 8; i++) {
            long t = Math.abs(x[i] - initialValue);
            if (t > c && x[i] != (-1)) {
                flag = i;
                c = t;
            }
        }
        x[flag] = -1;

        //求和,平均
        for (int i = 0; i < 8; i++) {
            if (x[i] != (-1)) {
                tot += x[i];
            }
        }
        double avaValue = (double) tot / m;

        //误差估计
        if (Math.abs(avaValue - initialValue) / (double) Math.max(avaValue, initialValue) >= 0.5) {
            context.write(key, new LongWritable(Math.round(avaValue)));
        } else {
            context.write(key, new LongWritable(initialValue));
        }
    }
}
