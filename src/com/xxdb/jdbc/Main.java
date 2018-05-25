package com.xxdb.jdbc;

import java.sql.*;

public class Main {
    // JDBC 驱动名及数据库 URL
    private static final String JDBC_DRIVER = "com.xxdb.jdbc.Driver";
    //data目录有数据库文件
    /*
        timestamp = 2012.06.13 13:30:10.008

        timestamp=09:30:00+rand(18000,n)

        qty=100*(1+rand(100,n))

        price=5.0+rand(100.0,n)

        string=`IBM`C`MS`MSFT`JPM`ORCL`FB`GE

        string=rand(string,n)

        t1= table(timestamp,string,qty,price)
     */
    //使用时要修改路径

    private static final String DB_URL = "jdbc:dolphindb://databasePath=/home/swang/dolphin/data/db01/t1";

    public static void main(String[] args) throws Exception{

        TestStatement();
        TestPreparedStatement();
    }

    public static void TestStatement() throws Exception{
        System.out.println("TestStatement begin");
        Class.forName(JDBC_DRIVER);
        Connection conn = null;
        Statement stmt = null;
        try{
            // 注册 JDBC 驱动
            Class.forName("com.xxdb.jdbc.Driver");

            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL);

            // 执行查询
            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement();
            String sql;
            sql = "select * from t1 where string = `MS";
            ResultSet rs = stmt.executeQuery(sql);

            // 展开结果集数据库
            while(rs.next()){
                // 通过字段检索
                Time timestamp  = rs.getTime("timestamp");
                String sym = rs.getString("string");
                int qty = rs.getInt("qty");
                double price = rs.getDouble("price");

                // 输出数据
                System.out.print("timestamp: " + timestamp);
                System.out.print(", string: " + sym);
                System.out.print(", qty: " + qty);
                System.out.print(", price: " + price);
                System.out.print("\n");
            }
            // 完成后关闭
            rs.close();
            stmt.close();
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }// 什么都不做
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("TestStatement end");
    }

    public static void TestPreparedStatement() throws Exception{
        System.out.println("TestStatement begin");
        String preparedsql = "select * from t1 where string = ? and price > ? ";
        Class.forName(JDBC_DRIVER);
        Connection conn = null;
        PreparedStatement stmt = null;
        try{
            // 注册 JDBC 驱动
            Class.forName("com.xxdb.jdbc.Driver");

            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL);

            // 执行查询
            System.out.println(" 实例化Statement对象...");
            stmt = conn.prepareStatement(preparedsql);
            //stmt.setString(1,"MS");
            stmt.setObject(1,"MSFT");
            stmt.setObject(2,25.0);
            ResultSet rs = stmt.executeQuery();

            // 展开结果集数据库
            while(rs.next()){
                // 通过字段检索
                Time timestamp  = rs.getTime("timestamp");
                String sym = rs.getString("string");
                int qty = rs.getInt("qty");
                double price = rs.getDouble("price");

                // 输出数据
                System.out.print("timestamp: " + timestamp);
                System.out.print(", string: " + sym);
                System.out.print(", qty: " + qty);
                System.out.print(", price: " + price);
                System.out.print("\n");
            }
            // 完成后关闭
            rs.close();
            stmt.close();
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }// 什么都不做
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("TestPreparedStatement end");
    }
}
