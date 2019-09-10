package bigdata.lib;

import bigdata.io.D2_AdMetricWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class D4_TerminalTypePartitioner extends Partitioner<Text, D2_AdMetricWritable> {
    public static HashMap<String,Integer> terminalTaskMap = new HashMap<>();
    static {
        /**
         1.1 安卓手机
         1.2 iphone
         2.1 安卓平板
         2.2 ipad
         3.1 电脑
         4.1 H5
         5.1 小程序
         6.1 微信公众号

         //Mobile移动端的数据发送到0号Reduce Task
         //PC端的数据发送到1号Reduce Task
         //WeChat的数据发送到2号Reduce Task
         //其他未知端的数据发送到3号Reduce Task
         */
        terminalTaskMap.put("1.1",0);
        terminalTaskMap.put("1.2",0);
        terminalTaskMap.put("2.1",0);
        terminalTaskMap.put("2.2",0);
        terminalTaskMap.put("3.1",1);
        terminalTaskMap.put("4.1",0);
        terminalTaskMap.put("5.1",2);
        terminalTaskMap.put("6.1",2);
    }
    @Override
    public int getPartition(Text key, D2_AdMetricWritable value, int numReduceTasks) {
        String terminalId = key.toString();
        return terminalTaskMap.get(terminalId) == null ? 3 : terminalTaskMap.get(terminalId);
    }
}
