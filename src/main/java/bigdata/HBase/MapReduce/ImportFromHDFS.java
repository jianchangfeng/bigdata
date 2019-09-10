package bigdata.HBase.MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.io.IOException;

public class ImportFromHDFS {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://roc15:9000/");
        conf.set("hbase.zookeeper.quorum", "apple:2181,roc15:2181,cedar:2181");

        Job job = Job.getInstance(conf);
        job.setJarByClass(ImportFromHDFS.class);
        job.setMapperClass(HDFSToHBaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        TableMapReduceUtil.initTableReducerJob("student", HDFSToHBaseReducer.class, job, null,null,null,null,false);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);
        Path inputPath = new Path("/student/input");
        FileInputFormat.addInputPath(job, inputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Mapper read from HDFS
    public static class HDFSToHBaseMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }
    // Reducer write to HBase
    public static class HDFSToHBaseReducer extends TableReducer<Text, NullWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String[] split = key.toString().split(",");
            Put put = new Put(split[0].getBytes());
            put.addColumn("info".getBytes(), "name".getBytes(), split[1].getBytes());
            put.addColumn("info".getBytes(), "sex".getBytes(), split[2].getBytes());
            put.addColumn("info".getBytes(), "age".getBytes(), split[3].getBytes());
            put.addColumn("info".getBytes(), "department".getBytes(), split[4].getBytes());
            context.write(NullWritable.get(), put);
        }
    }
}
