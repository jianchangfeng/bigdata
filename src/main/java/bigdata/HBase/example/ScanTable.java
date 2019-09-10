package bigdata.HBase.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanTable {
    public static void main(String[] args) {
        ScanTable object = new ScanTable();
        object.scanTableData();
    }

    public void scanTableData() {
        Configuration config = HBaseConfigUtil.getHBaseConfiguration();
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;

        try {
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf("peoples"));

            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("name"), Bytes.toBytes("first"));
            scan.addColumn(Bytes.toBytes("name"), Bytes.toBytes("last"));
            scan.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
            scan.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("gender"));
            scan.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"));

            resultScanner = table.getScanner(scan);

            for(Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
                byte[] firstNameValue = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("first"));
                byte[] lastNameValue = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("last"));
                byte[] emailValue = result.getValue(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
                byte[] genderValue = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("gender"));
                byte[] ageValue = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"));

                String firstName = Bytes.toString(firstNameValue);
                String lastName = Bytes.toString(lastNameValue);
                String email = Bytes.toString(emailValue);
                String gender = Bytes.toString(genderValue);
                String age = Bytes.toString(ageValue);

                System.out.println("First Name:" + firstName);
                System.out.println("last Name:" + lastName);
                System.out.println("email:" + email);
                System.out.println("gender:" + gender);
                System.out.println("age:" + age);
                System.out.println("finished Get");
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
