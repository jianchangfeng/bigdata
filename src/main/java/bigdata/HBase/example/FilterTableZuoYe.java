package bigdata.HBase.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterTableZuoYe {
    public static void main(String[] args) {
        FilterTableZuoYe object = new FilterTableZuoYe();
        object.scanage0rweb();
    }

    public void scanage0rweb() {
        Configuration config = HBaseConfigUtil.getHBaseConfiguration();
        Connection connection = null;
        Table table = null;
        ResultScanner resultScanner = null;

        try {
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf("peoples"));
            SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("24"));
            SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("personalinfo"), Bytes.toBytes("web"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("www.bing.com"));

            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

            filterList.addFilter(filter1);
            filterList.addFilter(filter2);

            Scan scan = new Scan();
            scan.setFilter(filterList);
            scan.addColumn(Bytes.toBytes("name"), Bytes.toBytes("first"));
            scan.addColumn(Bytes.toBytes("name"), Bytes.toBytes("last"));
            scan.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
            scan.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("gender"));
            scan.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"));
            scan.addColumn(Bytes.toBytes("personalinfo"), Bytes.toBytes("web"));

            resultScanner = table.getScanner(scan);

            for(Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
                byte[] firstNameValue = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("first"));
                byte[] lastNameValue = result.getValue(Bytes.toBytes("name"), Bytes.toBytes("last"));
                byte[] emailValue = result.getValue(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
                byte[] genderValue = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("gender"));
                byte[] ageValue = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("age"));
                byte[] webValue = result.getValue(Bytes.toBytes("personalinfo"), Bytes.toBytes("web"));

                String firstName = Bytes.toString(firstNameValue);
                String lastName = Bytes.toString(lastNameValue);
                String email = Bytes.toString(emailValue);
                String gender = Bytes.toString(genderValue);
                String age = Bytes.toString(ageValue);
                String web = Bytes.toString(webValue);

                System.out.println("First Name:" + firstName);
                System.out.println("last Name:" + lastName);
                System.out.println("email:" + email);
                System.out.println("gender:" + gender);
                System.out.println("age:" + age);
                System.out.println("web:" + web);
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
