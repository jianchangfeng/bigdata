package bigdata.lib;

import bigdata.io.Test_tmp_ByAreaDate_Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Test_tmp_FilePartitioner extends Partitioner<Text, Test_tmp_ByAreaDate_Writable> {
    @Override
    public int getPartition(Text key, Test_tmp_ByAreaDate_Writable values, int reduceTaskNums) {
        String date = key.toString();
        int hash = date.hashCode()&Integer.MAX_VALUE;
        System.out.println(hash+"--------------");
        System.out.println(hash%reduceTaskNums);
        return hash%reduceTaskNums;
    }
}
