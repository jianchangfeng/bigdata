package bigdata.mapreduce.log_analysis;

import bigdata.lib.Test_Submit_AreaTypePartitioner;
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

public class Test_Submit_SelectUVByAreaAndDate extends Configured implements Tool {

    public static class MyMapper extends Mapper<LongWritable,Text, Text,Text> {
        private int mapCounter = 0;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("第" + mapCounter + "次被调用 map！");
            String line = value.toString();
            String[] fields = line.split("\t");
            if(fields!=null&&!fields[6].equals("")){
                String date = fields[8];
                String view = "0";
                String click = "0";
                String area = fields[4];
                String view_type = fields[6];
                if(view_type.equals("1")){
                    view = "1";
                }else if(view_type.equals("2")){
                    click = "1";
                }
                context.write(new Text(date+"\t"+area), new Text(view+"\t"+click));
                mapCounter += 1;
            }

        }
    }

    public static class MyReducer extends Reducer<Text,Text,Text,Text>{
        private int reduceCounter = 0;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("第" + reduceCounter + "次被调用 reduce！");
            int view = 0;
            int click = 0;
            for(Text value:values){
                String[] view_pv = value.toString().split("\t");
                view+=Integer.parseInt(view_pv[0]);
                click+=Integer.parseInt(view_pv[1]);
            }
            context.write(key,new Text(view+"\t"+click));
            reduceCounter += 1;
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        //1.获取配置文件信息
        Configuration conf = getConf();

        //2.创建job对象
        Job job = null;
        job = Job.getInstance(conf);
        job.setJarByClass(Test_Submit_SelectUVByAreaAndDate.class);

        //3.给job添加执行过程
        //3.1HDFS中需要处理的文件路径
        Path inputPath = new Path(strings[0]);
        //3.2给job添加输入路径
        FileInputFormat.addInputPath(job,inputPath);
        //3.3设置Mapper输入类
        job.setMapperClass(MyMapper.class);
        //3.4设置Mapper输出键和值的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MyReducer.class);

        //3.4设置Reducer输出键和值的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(7);
        job.setPartitionerClass(Test_Submit_AreaTypePartitioner.class);

        //设置输出路径
        Path outputPath = new Path(strings[1]);
        FileOutputFormat.setOutputPath(job,outputPath);

        //4.提交job等待job完成
        boolean result = job.waitForCompletion(true);
        return result?0:1;
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        if(args.length==0){
            args = new String[2];
            args[0] = "hdfs://ns/mr_project/ad_log/";
            args[1] = "hdfs://ns/mr_project/log_analysis/Test";
        }
        Path outputPath = new Path(args[1]);
        try {
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(outputPath)){
                fs.delete(outputPath,true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            int status = ToolRunner.run(conf,new Test_Submit_SelectUVByAreaAndDate(),args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
