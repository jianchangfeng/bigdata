package bigdata.HBase.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class DeleteTable {
    public static void main(String[] args) {
        Configuration conf = HBaseConfigUtil.getHBaseConfiguration();
        Connection connection = null;
        Admin admin = null;

        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();

            TableName tableName = TableName.valueOf("people");

            if(admin.isTableAvailable(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try{
                if(admin != null) {
                    admin.close();
                }
                if(connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
}
