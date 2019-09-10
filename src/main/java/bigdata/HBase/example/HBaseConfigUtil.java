package bigdata.HBase.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseConfigUtil {
    public static Configuration getHBaseConfiguration() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path("/usr/local/hbase/conf/hbase-site.xml"));
//        configuration.set("hbase.zookeeper.quorum", "roc15");
//        configuration.set("hbase.zookeeper.property.clientPort", "2181");
//        configuration.set("hbase.master", "roc15:16010");
        return configuration;
    }
}
