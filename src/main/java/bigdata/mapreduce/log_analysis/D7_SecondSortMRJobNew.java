package bigdata.mapreduce.log_analysis;

import bigdata.io.D7_ComplexKeyWritable;
import bigdata.io.D7_PVGroupComparator;
import bigdata.lib.D7_PVPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class D7_SecondSortMRJobNew extends Configured implements Tool {
    //Map阶段
    public static class SecondSortMapper extends Mapper<LongWritable, Text, D7_ComplexKeyWritable, Text> {
        private int mapCounter = 0;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("第" + mapCounter + "次被调用 map！");
            String line = value.toString();
            String[] fields = line.split("\t");
            long pv = Long.valueOf(fields[2]);
            long click = Long.valueOf(fields[3]);
            D7_ComplexKeyWritable complexKey = new D7_ComplexKeyWritable(pv,click);
            String outputValue = fields[0] + "\t" + fields[1];
            context.write(complexKey,new Text(outputValue));
            System.out.println(outputValue);
            mapCounter += 1;
        }
    }

    //Reduce阶段
    public static class SecondSortReducer extends Reducer<D7_ComplexKeyWritable, Text, Text, D7_ComplexKeyWritable> {
        private int reduceCounter = 0;
        @Override
        protected void reduce(D7_ComplexKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("第" + reduceCounter + "次被调用！");
            for(Text value : values){
                System.out.println(key.toString());
                context.write(value,key);
            }
            reduceCounter += 1;
        }

    }

    @Override
    public int run(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = this.getConf();
        Job job = null;

        //2.创建job
        try {
            job = Job.getInstance(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.setJarByClass(D7_SecondSortMRJobNew.class);

        //3.给job添加执行流程

        //3.1 HDFS中需要处理的文件路径
        Path path = new Path(args[0]);

        try {
            //job添加输入路径
            FileInputFormat.addInputPath(job,path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //3.2设置map执行阶段
        job.setMapperClass(SecondSortMapper.class);
        job.setMapOutputKeyClass(D7_ComplexKeyWritable.class);//map输出key类型
        job.setMapOutputValueClass(Text.class); //map输出value类型

        job.setPartitionerClass(D7_PVPartitioner.class);//自定义Partitioner使用mapper输出key的一部分（pv）进行分区

        //3.3设置reduce执行阶段
        job.setReducerClass(SecondSortReducer.class);
        job.setOutputKeyClass(Text.class);//reduce输出key类型
        job.setOutputValueClass(D7_ComplexKeyWritable.class);//reduce输出value类型

        //job.setNumReduceTasks(3);//硬编码，不灵活

        job.setGroupingComparatorClass(D7_PVGroupComparator.class);//默认通过key的compareTo方法比较key，使用自定义的分组比较器

        //3.4设置job计算结果输出路径
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job,output);

        //4. 提交job，并等待job执行完成

        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    public static void main(String[] args) {
        //用于本地测试
        if(args.length == 0){
            args = new String[]{
                    "hdfs://ns/mr_project/log_analysis/output6/MapSideJoin",
                    "hdfs://ns/mr_project/log_analysis/output7/SecondSortJoinMRJob"
            };
        }
        //1.配置job
        Configuration conf = new Configuration();
        Path hdfsOutputPath = new Path(args[1]);//mr在hdfs上的输出路径
        try {
            //如果mr的输出结果路径存在，则删除
            FileSystem fileSystem = FileSystem.get(conf);
            if(fileSystem.exists(hdfsOutputPath)){
                fileSystem.delete(hdfsOutputPath,true);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            int status = ToolRunner.run(conf,new D7_SecondSortMRJobNew(),args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
