package bigdata.HBase.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class ListTables {
    public static void main(String[] args) {
        ListTables object = new ListTables();
        object.listTables();
    }
    public void listTables() {
        Configuration config = HBaseConfigUtil.getHBaseConfiguration();
        Connection connection = null;
        Admin admin = null;

        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();

            HTableDescriptor tableDescriptor[] = admin.listTables();

            for(int i = 0; i < tableDescriptor.length; i ++) {
                System.out.println(tableDescriptor[i].getNameAsString());
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
