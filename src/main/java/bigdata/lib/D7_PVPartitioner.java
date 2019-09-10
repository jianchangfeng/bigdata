package bigdata.lib;

import bigdata.io.D7_ComplexKeyWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class D7_PVPartitioner extends Partitioner<D7_ComplexKeyWritable, Text> {
    public int getPartition(D7_ComplexKeyWritable key, Text value, int numReduceTasks) {
        return (int)key.getPv() % numReduceTasks ;
    }
}
