package com.xxdb.jdbc;

import com.xxdb.DBConnection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.text.MessageFormat;

public class Main {
    // JDBC 驱动名及数据库 URL
    private static final String JDBC_DRIVER = "com.xxdb.jdbc.Driver";
    //data目录有数据库文件
    /*
        sym = `C`MS`MS`MS`IBM`IBM`C`C`C$symbol;

        price= 49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29;

        qty = 2200 1900 2100 3200 6800 5400 1300 2500 8800;

        timestamp = [09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12];

        t1 = table(timestamp, sym, qty, price);

        db=database("D:/dolphinDB/data/data01");

        saveTable(db,t1,`t1);

     */
    //使用时要修改路径

    private static final String DB_URL = "jdbc:dolphindb://databasePath=D:/dolphinDB/data/data01/t1";

    public static void main(String[] args) throws Exception{
        //CreateTable("D:\\dolphinDB\\data\\data1.java");
        TestStatement(new String[]{"select * from t1",
                "recordNum=1..9;select 1..9 as recordNum, sym from t1",
                "select 3 as portfolio, sym from t1;",
                "def f(a):a+100;select f(qty) as newQty, sym from t1;",
                "select last price from t1 group by sym;",
                "select top 3 * from t1;",
                "select * from t1 where sym=`IBM;",
                "select * from t1 where sym==`IBM;",
                "select * from t1 where sym=`IBM and qty>=2000 or timestamp>09:37:00;",
                "select * from t1 where qty>=2000, timestamp.minute()>=09:36m;",
                "select * from t1 where qty>=2000 and timestamp.minute()>=09:36m;",
                "select * from t1 where qty>=2000 && timestamp.minute()>=09:36m;",
                "select * from t1 where price>avg(price);",
                "select * from t1 where price>contextby(avg, price, sym) order by sym, price;",
                "select * from t1 order by sym, timestamp;",
                "select * from t1 where sym in `C`IBM order by sym, timestamp desc;",
                "select count(sym) as counts from t1 group by sym; ",
                "select avg(qty) from t1 group by sym;",
                "select wavg(price, qty) as vwap, sum(qty) from t1 group by sym;",
                "select wsum(price, qty) as dollarVolume, sum(qty) from t1 group by minute(timestamp) as ts;",
                "select sum(qty) from t1 group by sym, timestamp.minute() as minute;",
                "select sum(qty) from t1 group by sym, timestamp.minute() as minute order by minute;",
                "select wavg(price,qty) as wvap, sum(qty) as totalqty from t1 group by sym;",
                "select sym, price, qty, wavg(price,qty) as wvap, sum(qty) as totalqty from t1 context by sym;",
                "select sym, timestamp, price, eachPre(\\,price)-1.0 as ret from t1 context by sym;",
                "select *, cumsum(qty) from t1 context by sym, timestamp.minute();",
                "select top 2 * from t1 context by sym;",
                "select *, ols(price, qty)[0]+ols(price, qty)[1]*qty as fittedPrice from t1 context by sym;",
                "select *, ols(price, qty)[0]+ols(price, qty)[1]*qty as fittedPrice from t1 context by sym order by timestamp;",
                "select sum(qty) as totalqty from t1 group by sym having sum(qty)>10000;",
                "select * from t1 context by sym having count(sym)>2 and sum(qty)>10000;",
                "select price from t1 pivot by timestamp, sym;",
                "select last(price) from t1 pivot by timestamp.minute(), sym;",
                "update t1 set price=price+0.5, qty=qty-50 where sym=`C;t1;",
                "update t1 set price=price-avg(price) context by sym;t1",
                "item = table(1..10 as id, 10+rand(100,10) as qty, 1.0+rand(10.0,10) as price);promotion = table(1..10 as id, rand(0b 1b, 10) as flag, 0.5+rand(0.4,10) as discount);update item set price = price*discount from ej(item, promotion, `id) where flag=1;item",
                "exec price as p from t1;"
        });

        TestPreparedStatement("select * from t1 where string = ? and price > ? ",new Object[]{"MS",30.0});
    }

    public static void CreateTable(String fileName){
        DBConnection db=null;
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            StringBuilder sb = new StringBuilder();
            String s = null;
            while ((s = bufferedReader.readLine()) != null){
                sb.append(s);
            }
            db = new DBConnection();
            db.connect("127.0.0.1",8848);
            db.run(sb.toString());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(db != null) db.close();
        }
    }

    public static void TestStatement(String[] sqls) throws Exception{
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
            for(String sql : sqls) {
                ResultSet rs = stmt.executeQuery(sql);
                ResultSetMetaData resultSetMetaData = rs.getMetaData();
                int len = resultSetMetaData.getColumnCount();
                // 展开结果集数据库
                while (rs.next()) {
                    // 通过字段检索

                    for (int i = 0; i < len; ++i) {
                        // 输出数据
                        System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getObject(i)));
                    }
                    System.out.print("\n");
                }
                rs.close();
            }
            // 完成后关闭
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

    public static void TestPreparedStatement(String sql, Object[] objects) throws Exception{
        System.out.println("TestStatement begin");
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
            stmt = conn.prepareStatement(sql);
            int index = 1;
            for(Object o: objects){
                stmt.setObject(index,o);
                ++index;
            }

            ResultSet rs = stmt.executeQuery();

            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            int len = resultSetMetaData.getColumnCount();
            // 展开结果集数据库
            while (rs.next()) {
                // 通过字段检索

                for (int i = 0; i < len; ++i) {
                    // 输出数据
                    System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getObject(i)));
                }
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
