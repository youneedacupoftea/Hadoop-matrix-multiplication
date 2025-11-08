package matrixMultip;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class myReduce extends Reducer<Text, Text, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, String> mapA = new HashMap<String, String>();
        Map<String, String> mapB = new HashMap<String, String>();
        //提取矩阵A的行和B的列
        for (Text value : values) {
            String[] val = value.toString().split(",");
            if ("a".equals(val[0])) {
                mapA.put(val[1], val[2]);
            } else if ("b".equals(val[0])) {
                mapB.put(val[1], val[2]);
            }
        }

        int result = 0; //A行 B列乘积
        Iterator<String> myKeys = mapA.keySet().iterator();//新建一个迭代器,获得所有key的值
        while (myKeys.hasNext()) {
            String myKey = myKeys.next();

            // 因为mkey取的是mapA的key集合，所以只需要判断mapB是否存在即可。
            if (mapB.get(myKey) == null) {
                continue;
            }
            result += Integer.parseInt(mapA.get(myKey)) * Integer.parseInt(mapB.get(myKey));
        }
        context.write(key, new IntWritable(result));
    }
}
