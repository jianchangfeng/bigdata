package bigdata.mapreduce.log_analysis;

import bigdata.lib.D6_FileType;
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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class D6_MapSideJoinMRJobNew extends Configured implements Tool {
    //Map阶段
    public static class MapSideJoinMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        Map<String, String> areaMap = new HashMap<>();//存储地域id与地域名称的映射关系

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("area_info.txt")));
            String line = "";
            while ((line = br.readLine()) != null) {
                String[] areaInfos = line.split("\t");
                areaMap.put(areaInfos[0], areaInfos[1]);
            }
            br.close();
        }
        private int mapCounter = 0;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("第" + mapCounter + "次被调用 map！");
            String line = value.toString();
            String[] fields = line.split("\t");
            String areaId = fields[1];
            String areaName = areaMap.get(areaId);
            String outputValue = fields[0] + "\t" + areaName + "\t" + fields[2] + "\t" + fields[3] + "\t" + fields[4];
            context.write(NullWritable.get(), new Text(outputValue));
            mapCounter += 1;
//            Thread.sleep(5000);
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
        job.setJarByClass(D6_MapSideJoinMRJobNew.class);

        //3.给job添加执行流程

        //3.1 HDFS中需要处理的文件路径
        Path path = new Path(args[0]);
        try {
            //job添加输入路径
            FileInputFormat.addInputPath(job, path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //3.2设置map执行阶段
        job.setMapperClass(MapSideJoinMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);//map输出key类型
        job.setMapOutputValueClass(Text.class); //map输出value类型

        //3.3设置reduce执行阶段

        job.setNumReduceTasks(0);

        try {
              D6_FileType GF = new D6_FileType();
              String filetype = GF.getFileType(args[2]);
              if (filetype == "zip") {job.addCacheArchive(new URI(args[2]));}  //缓存HDFS中的压缩文件到task运行节点的工作目录}
              else {job.addCacheFile(new URI(args[2]));}    //缓存HDFS中的普通文件到task运行节点的工作目录

        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        //3.4设置job计算结果输出路径
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output);

        //4. 提交job，并等待job执行完成

        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    public static void main(String[] args) {
        //用于本地测试
        if (args.length == 0) {
            args = new String[]{
                    "hdfs://ns/mr_project/log_analysis/output5/ReduceSideJoin",
                    "hdfs://ns/mr_project/log_analysis/output6/MapSideJoin",
                    "hdfs://ns/mr_project/relation/area_info.zip"
            };
        }
            //1.配置job
            Configuration conf = new Configuration();
            Path hdfsOutputPath = new Path(args[1]);//mr在hdfs上的输出路径
            try {
                //如果mr的输出结果路径存在，则删除
                FileSystem fileSystem = FileSystem.get(conf);
                if (fileSystem.exists(hdfsOutputPath)) {
                    fileSystem.delete(hdfsOutputPath, true);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                int status = ToolRunner.run(conf, new D6_MapSideJoinMRJobNew(), args);
                System.exit(status);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }