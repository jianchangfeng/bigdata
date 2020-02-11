package bigdata.mapreduce.log_analysis;

import bigdata.io.D2_AdMetricWritable;
import bigdata.io.D3_PVKeyWritable;
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

public class D3_OrderByDescMRJobNew extends Configured implements Tool {

    //Map阶段
    public static class OrderByDescMapper extends Mapper<LongWritable,Text, D3_PVKeyWritable, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //20190102        666     334     0.5015015
            String line = value.toString();
            String[] fields = line.split("\t");
            String date = fields[0];
            long pv = Long.valueOf(fields[1]);
            long click = Long.valueOf(fields[2]);
            float clickRate = Float.valueOf(fields[3]);
            D3_PVKeyWritable pvKey = new D3_PVKeyWritable();//自定义的key类型，可以按照pv降序排列
            pvKey.setPv(pv);

            String val = date + "\t" + click + "\t" + clickRate;
            context.write(pvKey,new Text(val));

        }
    }

    //Reduce阶段
    public static class OrderByDescReducer extends Reducer<D3_PVKeyWritable, Text, Text, D2_AdMetricWritable>{
        @Override
        protected void reduce(D3_PVKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value : values){
                String[] valStrs = value.toString().split("\t");
                D2_AdMetricWritable ad = new D2_AdMetricWritable();
                ad.setPv(key.getPv());
                ad.setClick(Long.valueOf(valStrs[1]));
                ad.setClickRate(Float.valueOf(valStrs[2]));
                context.write(new Text(valStrs[0]),ad);
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
        job.setJarByClass(D3_OrderByDescMRJobNew.class);

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
        job.setMapperClass(OrderByDescMapper.class);
        job.setMapOutputKeyClass(D3_PVKeyWritable.class);//map输出key类型
        job.setMapOutputValueClass(Text.class); //map输出value类型

        //3.3设置reduce执行阶段
        job.setReducerClass(OrderByDescReducer.class);
        job.setOutputKeyClass(Text.class);//reduce输出key类型
        job.setOutputValueClass(D2_AdMetricWritable.class);//reduce输出value类型

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
                    "hdfs://ns/mr_project/log_analysis/output2-1",
                    "hdfs://ns/mr_project/log_analysis/output3"
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
            int status = ToolRunner.run(conf,new D3_OrderByDescMRJobNew(),args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
