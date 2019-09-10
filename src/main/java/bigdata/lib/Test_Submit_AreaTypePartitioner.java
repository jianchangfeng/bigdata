package bigdata.lib;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Test_Submit_AreaTypePartitioner extends Partitioner<Text,Text> {
    @Override
    public int getPartition(Text key, Text values, int reduceTaskNums) {
        String[] date_area =  key.toString().split("\t");
        String date = date_area[0];
        int hash = date.hashCode()&Integer.MAX_VALUE;
        System.out.println(hash+"--------------");
        System.out.println(hash%reduceTaskNums);
        return hash%reduceTaskNums;
    }
}
