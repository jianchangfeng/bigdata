package bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;


public class HDFSClient {
    /**
     * 获取HDFS文件系统对象
     * @return
     * @throws IOException
     */
    private FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);//创建hdfs文件系统对象
        return fs;
    }

    /**
     * 读取hdfs中的文件内容
     * @param hdfsFilePath
     */
    public void readHDFSFile(String hdfsFilePath){
        BufferedReader reader = null;
        FSDataInputStream fsDataInputStream = null;
        //通过HDFS Java API读取HDFS中的文件
        try {
            Path path = new Path(hdfsFilePath);
            fsDataInputStream = this.getFileSystem().open(path);//根据path创建FSDataInputStream输入流对象
            reader = new BufferedReader(new InputStreamReader(fsDataInputStream));
            String line = "";
            while((line = reader.readLine()) != null){
                System.out.println(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fsDataInputStream != null) {
                    fsDataInputStream.close();
                }

                if (reader != null) {
                    reader.close();
                }
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 将本地文件内容写入到HDFS指定文件中
     * @param localFilePath
     * @param hdfsFilePath
     */
    public void writeHDFSFile(String localFilePath,String hdfsFilePath){
        System.out.println("TestWrite");
        FSDataOutputStream fsDataOutputStream = null;
        FileInputStream fileInputStream = null;
        Path path = new Path(hdfsFilePath);
        try {
            //根据path创建输出流对象
            fsDataOutputStream = this.getFileSystem().create(path);

            //创建读取本地文件的输入流对象
            fileInputStream = new FileInputStream(new File(localFilePath));

            IOUtils.copyBytes(fileInputStream,fsDataOutputStream,4096,false);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fsDataOutputStream != null){
                    fsDataOutputStream.close();
                }
                if (fileInputStream != null){
                    fileInputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] ars) {
        String hdfsFilePath = "hdfs://ns/hdfs_client/from_local_2_hdfs.txt";
        HDFSClient client = new HDFSClient();
//        client.readHDFSFile(hdfsFilePath);
        String localFilePath = "hdfstest.txt";

        client.writeHDFSFile(localFilePath,hdfsFilePath);

        client.readHDFSFile(hdfsFilePath);

    }
}

