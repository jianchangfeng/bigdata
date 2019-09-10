package bigdata.mapreduce.log_analysis;

import bigdata.io.D5_ReduceSideJoinWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class D5_ReduceSideJoinMRJobNew extends Configured implements Tool {

    //Map阶段
    public static class ReduceSideJoinMapper extends Mapper<LongWritable,Text, Text, D5_ReduceSideJoinWritable>{

        private int mapCounter = 0;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("第" + mapCounter + "次被调用 map！");
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();//获取文件名称
            String line = value.toString();
            String[] fields = line.split("\t");
            //2个表的公共变量
            String date = fields[0];
            String areaId = fields[1];
            long userCount = 0;
            long pv = 0;
            long click = 0;
            String flag = "";

            if(fileName.startsWith("sum_user_count")){
                //处理的是按照日期和地域统计的用户数
                userCount = Long.valueOf(fields[2]);
                flag = "1";
            }else {
                pv = Long.valueOf(fields[2]);
                click = Long.valueOf(fields[3]);
                flag = "2";
            }
            D5_ReduceSideJoinWritable outputValue = new D5_ReduceSideJoinWritable(date,areaId,pv,click,userCount,flag);
            context.write(new Text(areaId),outputValue);
            mapCounter += 1;
        }
    }

    //Reduce阶段
    public static class ReduceSideJoinReducer extends Reducer<Text, D5_ReduceSideJoinWritable, NullWritable, D5_ReduceSideJoinWritable>{
        private int reduceCounter = 0;
        @Override
        protected void reduce(Text key, Iterable<D5_ReduceSideJoinWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("第" + reduceCounter + "次被调用 reduce！");
            String date = "";
            String areaId = key.toString();
            long userCount = 0;
            long pv = 0;
            long click = 0;

            for(D5_ReduceSideJoinWritable value : values){
                if(value.getFlag().equals("1")){
                    date = value.getDate();
                    userCount = value.getUserCount();
                }else if(value.getFlag().equals("2")){
                    pv = value.getPv();
                    click = value.getClick();
                }
            }
            D5_ReduceSideJoinWritable outputValue = new D5_ReduceSideJoinWritable(date,areaId,pv,click,userCount,"");
            context.write(NullWritable.get(),outputValue);
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
        job.setJarByClass(D5_ReduceSideJoinMRJobNew.class);

        //3.给job添加执行流程

        //3.1 HDFS中需要处理的文件路径
        //给job添加多输入路径
        Path[] inputPath = new Path[args.length-1];
        for(int i=0;i<inputPath.length;i++){
            inputPath[i] = new Path(args[i]);
        }
        FileInputFormat.setInputPaths(job,inputPath);

        //3.2设置map执行阶段
        job.setMapperClass(ReduceSideJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);//map输出key类型
        job.setMapOutputValueClass(D5_ReduceSideJoinWritable.class); //map输出value类型

        //3.3设置reduce执行阶段
        job.setReducerClass(ReduceSideJoinReducer.class);
        job.setOutputKeyClass(NullWritable.class);//reduce输出key类型
        job.setOutputValueClass(D5_ReduceSideJoinWritable.class);//reduce输出value类型

        //job.setNumReduceTasks(4);

        //3.4设置job计算结果输出路径
        Path output = new Path(args[args.length-1]);
        FileOutputFormat.setOutputPath(job,output);

        //4. 提交job，并等待job执行完成

        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    public static void main(String[] args) {
        //用于本地测试
        if(args.length == 0){
            args = new String[]{
                    "hdfs://ns/mr_project/report/sum_user_count_by_area_20190107.txt",
                    "hdfs://ns/mr_project/report/sum_group_by_area",
                    "hdfs://ns/mr_project/log_analysis/output5/ReduceSideJoin"
            };
        }
        //1.配置job
        Configuration conf = new Configuration();
        Path hdfsOutputPath = new Path(args[args.length-1]);//mr在hdfs上的输出路径
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
            int status = ToolRunner.run(conf,new D5_ReduceSideJoinMRJobNew(),args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
