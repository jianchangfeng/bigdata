package bigdata.HBase.MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

public class HBaseToHDFS {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
//        conf.set("fs.defaultFS", "hdfs://roc15:9000/");
//        conf.set("hbase.zookeeper.quorum", "apple:2181,roc15:2181,cedar:2181");
        conf.set("fs.defaultFS", "hdfs://node01:9000/");
        conf.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");

        FileSystem fs = FileSystem.get(conf);

        Job job = Job.getInstance(conf);
        job.setJarByClass(HBaseToHDFS.class);

        Scan scan = new Scan();
        scan.addColumn("info".getBytes(), "age".getBytes());

        TableMapReduceUtil.initTableMapperJob(
                "student".getBytes(),
                scan,
                HBaseToHDFSMapper.class,
                Text.class,
                IntWritable.class,
                job,
                false
        );

        job.setReducerClass(HBaseToHDFSReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        Path outputPath = new Path("/student/avgresult");

        if(fs.exists(outputPath)) {
            fs.delete(outputPath);
        }

        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Mapper read from Hbase to HDFS
    public static class HBaseToHDFSMapper extends TableMapper<Text, IntWritable> {
        Text outKey = new Text("age");
        IntWritable outValue = new IntWritable();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            boolean isContainsColumn = value.containsColumn("info".getBytes(), "age".getBytes());
            if (isContainsColumn) {
                List<Cell> listCells = value.getColumnCells("info".getBytes(), "age".getBytes());
                Cell cell = listCells.get(0);
                byte[] cloneValue = CellUtil.cloneValue(cell);
                String ageValue = Bytes.toString(cloneValue);
                outValue.set(Integer.parseInt(ageValue));
                context.write(outKey, outValue);
            }
        }
    }

    // Reducer read from Hbase to HDFS
    public static class HBaseToHDFSReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        DoubleWritable outValue = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int sum = 0;

            for(IntWritable value: values) {
                count ++;
                sum += value.get();
            }

            double avgAge = sum * 1.0 / count;
            outValue.set(avgAge);
            context.write(key, outValue);
        }
    }

}
