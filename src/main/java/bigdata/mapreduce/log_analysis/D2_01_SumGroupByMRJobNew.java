package bigdata.mapreduce.log_analysis;

import bigdata.io.D2_AdMetricWritable;
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

public class D2_01_SumGroupByMRJobNew extends Configured implements Tool {

    //Map阶段
    public static class SumGroupByMapper extends Mapper<LongWritable,Text, Text, D2_AdMetricWritable>{
        private int mapCounter = 0;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //id,advertiser_id,duration,position,area_id,terminal_id,view_type,device_id,date
            System.out.println("第" + mapCounter + "次被调用 map！");
            String line = value.toString();
            String[] fields = line.split("\t");

            String date = fields[8];
            String viewType = fields[6];
            if(viewType !=null && !viewType.equals("")){
                D2_AdMetricWritable adMetric = new D2_AdMetricWritable();
                int viewTypeInt = Integer.parseInt(viewType);
                if(viewTypeInt == 1){//曝光
                    adMetric.setPv(1);
                }else if(viewTypeInt == 2){
                    adMetric.setClick(1);
                }
                context.write(new Text(date),adMetric);
                mapCounter += 1;
            }
            }

    }

    //Reduce阶段
    public static class SumGroupByReducer extends Reducer<Text, D2_AdMetricWritable, Text, D2_AdMetricWritable>{
        private int reduceCounter = 0;
        @Override
        protected void reduce(Text key, Iterable<D2_AdMetricWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("第" + reduceCounter + "次被调用 reduce！");
            long pv = 0;
            long click = 0;
            float clickRate = 0;
            for(D2_AdMetricWritable adMetric : values){
                pv += adMetric.getPv();
                click += adMetric.getClick();
            }
            //clickRate = click / pv
            if(pv !=0 && click != 0){
                clickRate = (float) click / (float) pv;
            }
            D2_AdMetricWritable ad = new D2_AdMetricWritable(pv,click,clickRate);
            context.write(key,ad);
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
        job.setJarByClass(D2_01_SumGroupByMRJobNew.class);

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
        job.setMapperClass(SumGroupByMapper.class);
        job.setMapOutputKeyClass(Text.class);//map输出key类型
        job.setMapOutputValueClass(D2_AdMetricWritable.class); //map输出value类型

        //3.3设置reduce执行阶段
        job.setReducerClass(SumGroupByReducer.class);
        job.setOutputKeyClass(Text.class);//reduce输出key类型
        job.setOutputValueClass(D2_AdMetricWritable.class);//reduce输出value类型

        //job.setNumReduceTasks(3);//硬编码，不灵活
        //-D mapreduce.job.reduces=3

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
                    "hdfs://ns/mr_project/ad_log",
                    "hdfs://ns/mr_project/log_analysis/output2-1"
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
                System.out.println("output the result");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            int status = ToolRunner.run(conf,new D2_01_SumGroupByMRJobNew(),args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
