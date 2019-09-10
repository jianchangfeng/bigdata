package bigdata.HBase.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertIntoTable {
    public static void main(String[] args) {
        InsertIntoTable object = new InsertIntoTable();
        object.insertRecords();
    }

    public void insertRecords() {
        Configuration config = HBaseConfigUtil.getHBaseConfiguration();
        Connection connection = null;
        Table table = null;

        try {
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf("peoples"));

            //			creating sample data that can be used to save into hbase table
            String[][] people = {
                    { "1", "Marcel", "Haddad", "marcel@xyz.com", "M", "26", "www.google.com" },
                    { "2", "Franklin", "Holtz", "franklin@xyz.com", "M", "24", "www.bing.com" },
                    { "3", "Dwayne", "McKee", "dwayne@xyz.com", "M", "27", "www.bing.com" },
                    { "4", "Rae", "Schroeder", "rae@xyz.com", "F", "31", "www.baidu.com" },
                    { "5", "Rosalie", "burton", "rosalie@xyz.com", "F", "25", "www.baidu.com" },
                    { "6", "Gabriela", "Ingram", "gabriela@xyz.com", "F", "24", "www.baidu.com" },
                    { "7", "Marcel", "Haddad", "marcel@xyz.com", "M", "26", "www.facebook.com" },
                    { "8", "Franklin", "Holtz", "franklin@xyz.com", "M", "24", "www.facebook.com" },
                    { "9", "Dwayne", "McKee", "dwayne@xyz.com", "M", "27", "www.google.com" },
                    { "10", "Rae", "Schroeder", "rae@xyz.com", "F", "31", "www.google.com" },
                    { "11", "Rosalie", "burton", "rosalie@xyz.com", "F", "25", "www.google.com" },
                    { "12", "Gabriela", "Ingram", "gabriela@xyz.com", "F", "24", "www.google.com" } };

            for (int i = 0; i < people.length; i ++) {
                Put person = new Put(Bytes.toBytes(i));
                person.addColumn(Bytes.toBytes("name"), Bytes.toBytes("first"), Bytes.toBytes(people[i][1]));
                person.addColumn(Bytes.toBytes("name"), Bytes.toBytes("last"), Bytes.toBytes(people[i][2]));
                person.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"), Bytes.toBytes(people[i][3]));
                person.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("gender"), Bytes.toBytes(people[i][4]));
                person.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"), Bytes.toBytes(people[i][5]));
                person.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("web"), Bytes.toBytes(people[i][6]));
                table.put(person);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try{
                if(connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
}
