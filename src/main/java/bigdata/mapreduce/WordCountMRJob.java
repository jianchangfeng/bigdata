package bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountMRJob {

    //Map阶段
    /**
     * 输入数据键值对类型：
     * LongWritable:输入数据的偏移量
     * Text：输入数据类型
     *
     * 输出数据键值对类型：
     * Text：输出数据key的类型
     * IntWritable：输出数据value的类型
     */
    public static class WordCountMapper extends Mapper<LongWritable,Text, Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] words = line.split("\t");

            for(String word : words){
                //word 1
                context.write(new Text(word),new IntWritable(1));
            }
        }
    }
    //Reduce阶段

    /**
     * 输入数据键值对类型：
     * Text：输入数据的key类型
     * IntWritable：输入数据的key类型
     *
     * 输出数据键值对类型：
     * Text：输出数据的key类型
     * IntWritable：输出数据的key类型
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // word {1,1,1,...}

            int sum = 0;

            for(IntWritable value : values){
                sum += value.get();
            }

            context.write(key,new IntWritable(sum));
        }
    }

    public static void main(String[] args) {
        //1.配置job
        Configuration conf = new Configuration();
        Job job = null;
        //2.创建job
        try {
            job = Job.getInstance(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.setJarByClass(WordCountMRJob.class);
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
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);//map输出key类型
        job.setMapOutputValueClass(IntWritable.class); //map输出value类型
        //3.3设置reduce执行阶段
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);//reduce输出key类型
        job.setOutputValueClass(IntWritable.class);//reduce输出value类型
        //3.4设置job计算结果输出路径
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job,output);
        //4. 提交job，并等待job执行完成
        try {
            boolean result = job.waitForCompletion(true);
            System.exit(result ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
