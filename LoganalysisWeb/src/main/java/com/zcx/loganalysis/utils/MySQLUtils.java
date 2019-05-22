package com.zcx.loganalysis.utils;

import java.sql.*;

/**
 * 操作MySQL的工具类
 */
public class MySQLUtils {

    private static final String USERNAME = "root";

    private static final String PASSWORD = "zoucaoxin";

    private static final String DRIVERCLASS = "com.mysql.jdbc.Driver";

    private static final String URL = "jdbc:mysql://192.168.48.143:3306/loganalysis";


    /**
     * 获取数据库连接
     */
    public static Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName(DRIVERCLASS);
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return connection;
    }


    /**
     * 释放资源
     */
    public static void release(Connection connection, PreparedStatement pstmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(getConnection());
    }

}
