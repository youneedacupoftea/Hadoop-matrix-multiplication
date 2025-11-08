package matrixMultip;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class myMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String flag = null; //数据集名称
    private int row_A = 1000; //A的行数
    private int col_B = 1000; //B的列数
    private int currow_A = 1; //当前所在的行数
    private int currow_B = 1;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flag = ((FileSplit) context.getInputSplit()).getPath().getName();// 获取文件名称
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取并分割数据
        String[] tokens = value.toString().split(",");
        //分别对A,B矩阵进行map操作
        if ("biga.txt".equals(flag)) {
            for (int i = 1; i <= col_B; i++) {
                Text k = new Text(currow_A + "," + i);
                for (int j = 0; j < tokens.length; j++) {
                    Text v = new Text("a," + (j + 1) + "," + tokens[j]);
                    context.write(k, v);
                }
            }
            currow_A++; //矩阵下移一行
        } else if ("bigb.txt".equals(flag)) {
            for (int i = 1; i <= row_A; i++) {
                for (int j = 0; j < tokens.length; j++) {
                    Text k = new Text(i + "," + (j + 1));
                    Text v = new Text("b," + currow_B + "," + tokens[j]);
                    context.write(k, v);
                }
            }
            currow_B++;
        }
    }
}
