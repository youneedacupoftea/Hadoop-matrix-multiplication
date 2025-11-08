package dataPretreat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class myMapper extends Mapper<LongWritable, Text, Text, Text> {
    //预处理矩阵的行列数
    private int row = 3;
    private int col = 11;
    //行索引
    private int rowIndex = 1;
    //8个方向的方向数组
    final int[][] dirStep = {{-1, -1}, {1, 1}, {-1, 1}, {1, -1}, {1, 0}, {-1, 0}, {0, 1}, {0, -1}};

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String[] tokens = value.toString().split(",");
        //枚举元素
        for (int j = 0; j < tokens.length; j++) {
            int dirRow = -1;
            int dirCol = -1;
            //记录斜方向的位置
            for (int v = 0; v < 4; v++) {
                dirRow = rowIndex + dirStep[v][0];
                dirCol = j + 1 + dirStep[v][1];
                // 在矩阵范围内就添加标记为1的值
                if (dirRow < 1)
                    dirRow = row;
                if (dirCol < 1)
                    dirCol = col;
                if (dirRow > row)
                    dirRow = 1;
                if (dirCol > col)
                    dirCol = 1;
                context.write(new Text(dirRow + "," + dirCol), new Text("1," + tokens[j]));
            }
            // 处理其余位置
            for (int v = 4; v < 8; v++) {
                dirRow = rowIndex + dirStep[v][0];
                dirCol = j + 1 + dirStep[v][1];
                if (dirRow < 1)
                    dirRow = row;
                if (dirCol < 1)
                    dirCol = col;
                if (dirRow > row)
                    dirRow = 1;
                if (dirCol > col)
                    dirCol = 1;
                context.write(new Text(dirRow + "," + dirCol), new Text("1," + tokens[j]));
            }
            Text k = new Text(rowIndex + "," + (j + 1));
            Text v = new Text("-1," + tokens[j]);
            context.write(k, v);
        }
        rowIndex++;
    }
}
