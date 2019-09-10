package bigdata.HBase.MapReduce;

import org.apache.hadoop.util.ProgramDriver;

public class Driver {
    public static void main(String[] args) throws Throwable {
        ProgramDriver pgd = new ProgramDriver();
        pgd.addClass("ImportFromHDFS", ImportFromHDFS.class, "Import from hdfs to HBase");
        pgd.addClass("HBaseToHDFS", HBaseToHDFS.class, "HBase to HDFS");
        pgd.addClass("HBaseToHDFSZuoYe", HBaseToHDFSZuoYe.class, "HBaseToHDFSZuoYe");
        pgd.driver(args);

    }
}
