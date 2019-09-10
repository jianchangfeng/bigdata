package org.sparkcourse.SparkStreaming_userbehavior;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

public class DBConnectionPool {
    private static LinkedList<Connection> connectionQueue;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection() {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<Connection>();
                for (int i = 0;i < 5;i ++) {
                    Connection conn = DriverManager.getConnection(
                            // "jdbc:mysql://localhost:3306/test?characterEncoding=utf8&useSSL=true",
                            "jdbc:mysql://rm-m5e7v62j450jyecm24o.mysql.rds.aliyuncs.com/test?characterEncoding=utf8&useSSL=true",
                            "root",
                            "Hadoop@xiaoxiang"
                    );
                    connectionQueue.push(conn);
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }
}