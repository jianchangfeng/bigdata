package bigdata.mapreduce.log_analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class D1_Get_detail_of_one_advertisement extends Configured implements Tool {
    //Map阶段
    public static class SelectWhereMapper extends Mapper<LongWritable,Text, Text, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            if(fields != null && fields.length == 9){
                if(fields[1].equals("apple")){
                    context.write(new Text(line), NullWritable.get());
                }
            }
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
        job.setJarByClass(D1_Get_detail_of_one_advertisement.class);

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
        job.setMapperClass(SelectWhereMapper.class);
        job.setMapOutputKeyClass(Text.class);//map输出key类型
        job.setMapOutputValueClass(NullWritable.class); //map输出value类型

        job.setNumReduceTasks(0);//MR默认是1 Reduce task


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
                    "hdfs://ns/mr_project/ad_log/ad_20190107.txt",
                    "hdfs://ns/mr_project/log_analysis/output1"
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
            int status = ToolRunner.run(conf,new D1_Get_detail_of_one_advertisement(),args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
