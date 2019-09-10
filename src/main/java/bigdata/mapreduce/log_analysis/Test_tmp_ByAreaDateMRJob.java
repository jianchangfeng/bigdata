package bigdata.mapreduce.log_analysis;

import bigdata.io.Test_tmp_ByAreaDate_Writable;
import bigdata.lib.Test_tmp_FilePartitioner;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Test_tmp_ByAreaDateMRJob extends Configured implements Tool {

    //Map阶段
    public static class TestByAreaDateMRJobMapper extends Mapper<LongWritable,Text, Text, Test_tmp_ByAreaDate_Writable>{
        private int mapCounter = 0;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("第" + mapCounter + "次被调用 map！");
            //id,advertiser_id,duration,position,area_id,terminal_id,view_type,device_id,date
            String line = value.toString();
            String[] fields = line.split("\t");
            String areaId = fields[4];
            String date = fields[8];
            String viewType = fields[6];
            if(viewType !=null && !viewType.equals("")){
                Test_tmp_ByAreaDate_Writable adMetric = new Test_tmp_ByAreaDate_Writable();
                adMetric.setDate(date);
                adMetric.setAreaId(areaId);
                int viewTypeInt = Integer.parseInt(viewType);
                if(viewTypeInt == 1){//曝光
                    adMetric.setPv(1);
                }else if(viewTypeInt == 2){
                    adMetric.setClick(1);
                }
                context.write(new Text(date),adMetric);
                System.out.println(adMetric.toString());
                mapCounter += 1;
            }
        }
    }
    //Reduce阶段
    public static class TestByAreaDateMRJobReducer extends Reducer<Text, Test_tmp_ByAreaDate_Writable, NullWritable, Test_tmp_ByAreaDate_Writable>{
        private int reduceCounter = 0;
        @Override
        protected void reduce(Text key, Iterable<Test_tmp_ByAreaDate_Writable> values, Context context) throws IOException, InterruptedException {
            System.out.println("第" + reduceCounter + "次被调用 reduce！");
            String date = key.toString();
            String areaId = "";
            long pv = 0;
            long click = 0;
            for (Test_tmp_ByAreaDate_Writable value : values){
                areaId = value.getAreaId();
                pv  += value.getPv();
                click += value.getClick();
            }
            Test_tmp_ByAreaDate_Writable output = new Test_tmp_ByAreaDate_Writable(date,areaId,pv,click);
            context.write(NullWritable.get(),output);
            System.out.println(output.toString());
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
        job.setJarByClass(Test_tmp_ByAreaDateMRJob.class);

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
        job.setMapperClass(TestByAreaDateMRJobMapper.class);
        job.setMapOutputKeyClass(Text.class);//map输出key类型
        job.setMapOutputValueClass(Test_tmp_ByAreaDate_Writable.class); //map输出value类型

        //3.3设置reduce执行阶段
        job.setReducerClass(TestByAreaDateMRJobReducer.class);
        job.setOutputKeyClass(NullWritable.class);//reduce输出key类型
        job.setOutputValueClass(Test_tmp_ByAreaDate_Writable.class);//reduce输出value类型

        job.setNumReduceTasks(7);//硬编码，不灵活
//        job.setPartitionerClass(D4_TerminalTypePartitioner.class);

        job.setPartitionerClass(Test_tmp_FilePartitioner.class);//设置自定义的partitioner

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
                    "hdfs://ns/mr_project/ad_log/",
                    "hdfs://ns/mr_project/log_analysis/selftestnew4"
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
            int status = ToolRunner.run(conf,new Test_tmp_ByAreaDateMRJob(),args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
