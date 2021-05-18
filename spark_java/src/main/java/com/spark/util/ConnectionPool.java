package com.spark.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 *
 * 简易版的连接池
 *
 * @author yangqian
 * @date 2021/5/18
 */
public class ConnectionPool {

    /**
     * 静态的Connection队列
     */
    private static LinkedList<Connection> connectionQueue;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection() {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<Connection>();
                for (int i = 0; i < 10; i++) {
                    Connection conn = DriverManager.getConnection(
                            "jdbc:mysql://localhost:3306/test_spark_db",
                            "root",
                            "admin123"
                            );
                    connectionQueue.push(conn);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    /**
     * 还一个连接回去
     * @param conn
     */
    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }

}
