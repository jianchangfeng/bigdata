package bigdata.mapreduce.log_analysis;

import bigdata.io.D2_AdMetricWritable;
import bigdata.io.D4_TerminalTypeReportWritable;
import bigdata.lib.D4_TerminalTypePartitioner;
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
import java.util.HashMap;

public class D4_CaseWhenSumGroupByMRJobNew extends Configured implements Tool {

    //Map阶段
    public static class CaseWhenSumGroupByMapper extends Mapper<LongWritable,Text, Text, D2_AdMetricWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //id,advertiser_id,duration,position,area_id,terminal_id,view_type,device_id,date
            String line = value.toString();
            String[] fields = line.split("\t");

//            String date = fields[8]; //只处理20170107日的数据，所以先不用date数据
//            String areaId = fields[4];
            String terminal_id = fields[5];
            String viewType = fields[6];

            if(viewType !=null && !viewType.equals("")){
                D2_AdMetricWritable adMetric = new D2_AdMetricWritable();
                int viewTypeInt = Integer.parseInt(viewType);
                if(viewTypeInt == 1){//曝光
                    adMetric.setPv(1);
                }else if(viewTypeInt == 2){
                    adMetric.setClick(1); //点击
                }
                System.out.println(adMetric);
                context.write(new Text(terminal_id),adMetric);
            }
        }
    }

    //Reduce阶段
    public static class CaseWhenSumGroupByReducer extends Reducer<Text, D2_AdMetricWritable, NullWritable, D4_TerminalTypeReportWritable>{
        private D4_TerminalTypeReportWritable terminalTypeReport = new D4_TerminalTypeReportWritable();
        private HashMap<String,String> terminalTypeMap = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //setup方法用于reducer的初识化配置，只执行一次
//            1.1 安卓手机
//            1.2 iphone
//            2.1 安卓平板
//            2.2 ipad
//            3.1 电脑
//            4.1 H5
//            5.1 小程序
//            6.1 微信公众号
            terminalTypeMap.put("1.1","Mobile");
            terminalTypeMap.put("1.2","Mobile");
            terminalTypeMap.put("2.1","Mobile");
            terminalTypeMap.put("2.2","Mobile");
            terminalTypeMap.put("3.1","PC");
            terminalTypeMap.put("4.1","Mobile");
            terminalTypeMap.put("5.1","WeChat");
            terminalTypeMap.put("6.1","WeChat");
        }

        @Override
        protected void reduce(Text key, Iterable<D2_AdMetricWritable> values, Context context) throws IOException, InterruptedException {
            if("".equals(terminalTypeReport.getTerminalType())){
                String terminalType = terminalTypeMap.get(key.toString());//通过终端id获取对应的终端类型
                terminalTypeReport.setTerminalType(terminalType);
            }

            for(D2_AdMetricWritable value : values){
                terminalTypeReport.plusPv(value.getPv());
                terminalTypeReport.plusClick(value.getClick());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //cleanup方法用于释放reducer中使用的资源，在reduce task结束的时候调用，只执行一次
            context.write(NullWritable.get(),terminalTypeReport);
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
        job.setJarByClass(D4_CaseWhenSumGroupByMRJobNew.class);

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
        job.setMapperClass(CaseWhenSumGroupByMapper.class);
        job.setMapOutputKeyClass(Text.class);//map输出key类型
        job.setMapOutputValueClass(D2_AdMetricWritable.class); //map输出value类型

        //3.3设置reduce执行阶段
        job.setReducerClass(CaseWhenSumGroupByReducer.class);
        job.setOutputKeyClass(NullWritable.class);//reduce输出key类型
        job.setOutputValueClass(D4_TerminalTypeReportWritable.class);//reduce输出value类型

        job.setNumReduceTasks(4);//硬编码，不灵活

        job.setPartitionerClass(D4_TerminalTypePartitioner.class);//设置自定义的partitioner

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
                    "hdfs://ns/mr_project/log_analysis/output4"
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
            int status = ToolRunner.run(conf,new D4_CaseWhenSumGroupByMRJobNew(),args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
