package com.xxdb.jdbc;

import com.xxdb.DBConnection;
import com.xxdb.data.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.HashMap;

public class Main {
    // JDBC 驱动名及数据库 URL
    private static final String JDBC_DRIVER = "com.xxdb.jdbc.Driver";
    //windows databasePath = "D:/dolphinDB/data/data01"
    //linux databasePath = "home/swang/dolphin/data/db02"
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

    private static final String path_All = "/data/dballdata/t1";

    private static final String path_data = "data/t1";

    private static final String DB_URL = MessageFormat.format("jdbc:dolphindb://localhost:8848;databasePath={0}{1}",System.getProperty("user.dir"),path_All);

    public static void main(String[] args) throws Exception{
//        CreateTable(System.getProperty("user.dir")+"/data/createTable.java");
//        TestStatement(new String[]{"select * from t1",
//                "recordNum=1..9;select 1..9 as recordNum, sym from t1",
//                "select 3 as portfolio, sym from t1;",
//                "def f(a):a+100;select f(qty) as newQty, sym from t1;",
//                "select last price from t1 group by sym;",
//                "select top 3 * from t1;",
//                "select * from t1 where sym=`IBM;",
//                "select * from t1 where sym==`IBM;",
//                "select * from t1 where sym=`IBM and qty>=2000 or timestamp>09:37:00;",
//                "select * from t1 where qty>=2000, timestamp.minute()>=09:36m;",
//                "select * from t1 where qty>=2000 and timestamp.minute()>=09:36m;",
//                "select * from t1 where qty>=2000 && timestamp.minute()>=09:36m;",
//                "select * from t1 where price>avg(price);",
//                "select * from t1 where price>contextby(avg, price, sym) order by sym, price;",
//                "select * from t1 order by sym, timestamp;",
//                "select * from t1 where sym in `C`IBM order by sym, timestamp desc;",
//                "select count(sym) as counts from t1 group by sym; ",
//                "select avg(qty) from t1 group by sym;",
//                "select wavg(price, qty) as vwap, sum(qty) from t1 group by sym;",
//                "select wsum(price, qty) as dollarVolume, sum(qty) from t1 group by minute(timestamp) as ts;",
//                "select sum(qty) from t1 group by sym, timestamp.minute() as minute;",
//                "select sum(qty) from t1 group by sym, timestamp.minute() as minute order by minute;",
//                "select wavg(price,qty) as wvap, sum(qty) as totalqty from t1 group by sym;",
//                "select sym, price, qty, wavg(price,qty) as wvap, sum(qty) as totalqty from t1 context by sym;",
//                "select sym, timestamp, price, eachPre(\\,price)-1.0 as ret from t1 context by sym;",
//                "select *, cumsum(qty) from t1 context by sym, timestamp.minute();",
//                "select top 2 * from t1 context by sym;",
//                "select *, ols(price, qty)[0]+ols(price, qty)[1]*qty as fittedPrice from t1 context by sym;",
//                "select *, ols(price, qty)[0]+ols(price, qty)[1]*qty as fittedPrice from t1 context by sym order by timestamp;",
//                "select sum(qty) as totalqty from t1 group by sym having sum(qty)>10000;",
//                "select * from t1 context by sym having count(sym)>2 and sum(qty)>10000;",
//                "select price from t1 pivot by timestamp, sym;",
//                "select last(price) from t1 pivot by timestamp.minute(), sym;",
//                "update t1 set price=price+0.5, qty=qty-50 where sym=`C;t1;",
//                "update t1 set price=price-avg(price) context by sym;t1",
//                "item = table(1..10 as id, 10+rand(100,10) as qty, 1.0+rand(10.0,10) as price);promotion = table(1..10 as id, rand(0b 1b, 10) as flag, 0.5+rand(0.4,10) as discount);update item set price = price*discount from ej(item, promotion, `id) where flag=1;item",
//                "exec price as p from t1;"
//        });

//          long l = 2222222l;
//          int i = (int)l;
//          short s = (short)l;
//        System.out.println(i);
//        System.out.println(s);

        Vector vector;

        vector = new BasicBooleanVector(2);
        vector.set(0,new BasicBoolean(true));
        vector.set(1,new BasicBoolean(false));

        vector = new BasicByteVector(2);
        vector.set(0,new BasicByte((byte)97));
        vector.set(1,new BasicByte((byte)98));

        vector = new BasicShortVector(2);
        vector.set(0,new BasicShort((short)1));
        vector.set(1,new BasicShort((short)2));

        vector = new BasicIntVector(2);
        vector.set(0,new BasicInt(1));
        vector.set(1,new BasicInt(2));

        vector = new BasicLongVector(2);
        vector.set(0,new BasicLong(1));
        vector.set(1,new BasicLong(2));

        vector = new BasicStringVector(2);
        vector.set(0,new BasicString("A"));
        vector.set(1,new BasicString("B"));

        vector = new BasicStringVector(2);
        vector.set(0,new BasicString("A"));
        vector.set(1,new BasicString("B"));

        vector = new BasicStringVector(2);
        vector.set(0,new BasicString("A"));
        vector.set(1,new BasicString("B"));

        vector = new BasicStringVector(2);
        vector.set(0,new BasicString("A"));
        vector.set(1,new BasicString("B"));

        vector = new BasicStringVector(2);
        vector.set(0,new BasicString("A"));
        vector.set(1,new BasicString("B"));

        vector = new BasicStringVector(2);
        vector.set(0,new BasicString("A"));
        vector.set(1,new BasicString("B"));

        vector = new BasicStringVector(2);
        vector.set(0,new BasicString("A"));
        vector.set(1,new BasicString("B"));

        vector = new BasicStringVector(2);
        vector.set(0,new BasicString("A"));
        vector.set(1,new BasicString("B"));

        vector = new BasicStringVector(2);
        vector.set(0,new BasicString("A"));
        vector.set(1,new BasicString("B"));

        vector = new BasicStringVector(2);
        vector.set(0,new BasicString("A"));
        vector.set(1,new BasicString("B"));

        vector = new BasicStringVector(2);
        vector.set(0,new BasicString("A"));
        vector.set(1,new BasicString("B"));




        Object[] o1 = new Object[]{true, 'a', 122, 21, 22, 2.1f, 2.1, "Hello",
                new BasicDate(LocalDate.parse("2013-06-13")),
                new BasicMonth(YearMonth.parse("2016-06")),
                new BasicTime(LocalTime.parse("13:30:10.008")),
                new BasicMinute(LocalTime.parse("13:30:10")),
                new BasicSecond(LocalTime.parse("13:30:10")),
                new BasicDateTime(LocalDateTime.parse("2012-06-13T13:30:10")),
                new BasicTimestamp(LocalDateTime.parse("2012-06-13T13:30:10.008")),
                new BasicNanoTime(LocalTime.parse("13:30:10.008007006")),
                new BasicNanoTimestamp(LocalDateTime.parse("2012-06-13T13:30:10.008007006"))};

        Object[] o2 = new Object[]{true, 'A', 123, 22, 23, 2.2f, 2.2, "world",
                new BasicDate(LocalDate.parse("2013-06-14")),
                new BasicMonth(YearMonth.parse("2016-07")),
                new BasicTime(LocalTime.parse("13:30:10.009")),
                new BasicMinute(LocalTime.parse("13:31:10")),
                new BasicSecond(LocalTime.parse("13:30:11")),
                new BasicDateTime(LocalDateTime.parse("2012-06-13T13:30:11")),
                new BasicTimestamp(LocalDateTime.parse("2012-06-13T13:30:10.009")),
                new BasicNanoTime(LocalTime.parse("13:30:10.008007007")),
                new BasicNanoTimestamp(LocalDateTime.parse("2012-06-13T13:30:10.008007007"))};

        Object[][] o3 = new Object[][]{o1,o2};
        System.out.println(o3.length);
        System.out.println(o3[0].length);

        int len = o3[0].length;
        int n = o3.length;

        HashMap<Integer,Object> map = new HashMap<>(len+1);
        Object[] o4 = new Object[len];


        for(int i=0; i< len; ++i){
            vector = new BasicAnyVector(n);
            switch (i){
                case 0:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicBoolean((boolean)o3[j][i]));
                    }
                    break;
                case 1:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicByte((byte) ((char)o3[j][i] & 0xFF)));
                    }
                    break;
                case 2:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicShort(Short.parseShort(o3[j][i].toString())));
                    }
                    break;
                case 3:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicInt((int)o3[j][i]));
                    }
                    break;
                case 4:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicLong((int)o3[j][i]));
                    }
                    break;
                case 5:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicFloat((float) o3[j][i]));
                    }
                    break;
                case 6:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicDouble((double)o3[j][i]));
                    }
                    break;
                case 7:
                    for(int j=0; j<n; ++j){
                        vector.set(j,new BasicString((String) o3[j][i]));
                    }
                    break;
                case 8:
                case 9:
                case 10:
                case 11:
                case 12:
                case 13:
                case 14:
                case 15:
                case 16:
                    for(int j=0; j<n; ++j){
                        vector.set(j,(Scalar) o3[j][i]);
                    }
                    break;
            }
            map.put(i+1,vector);
            o4[i] = vector;
        }

        TestStatementInsert("select * from t1",o4);




//          TestStatementUpdate("update t1 set bool = true, char = 'A', short = 123, int = 22, long = 23, float = 2.2, double = 2.2, string = `world, date = 2013.06.14, month = 2016.07M, time = 13:30:10.009, minute = 13:31m, second = 13:30:11, datetime = 2012.06.13T13:30:11, timestamp = 2012.06.13T13:30:10.009, nanotime = 13:30:10.008007007, nanotimestamp = 2012.06.13T13:30:10.008007007 where bool = true ,char = 'a' ,short = 122 ,int = 21 ,long = 22 ,float = 2.1f ,double = 2.1 ,string = `Hello ,date = 2013.06.13 ,month = 2016.06M ,time = 13:30:10.008 ,minute = 13:30m ,second = 13:30:10 ,datetime = 2012.06.13T13:30:10 ,timestamp = 2012.06.13T13:30:10.008 ,nanotime = 13:30:10.008007006 ,nanotimestamp = 2012.06.13T13:30:10.008007006");
//
//          TestStatement(new String[]{"select * from t1"});
//
//
//
//
//
//
//          TestPreparedStatement("select * from t1","select * from t1 where bool = ? , char = ?, short = ?, int = ?, long = ?, float = ?, double = ?, string = ?, date = ?, month = ?, time = ?, minute = ?, second = ?, datetime = ?, timestamp = ?, nanotime = ?, nanotimestamp =? ;",
//                  o1);
//
//        TestPreparedStatement("select * from t1","insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
//                o1);
//
//        TestPreparedStatementBantch("select * from t1","insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",new Object[]{o1,o2});
//
//        HashMap<Integer,Object> hashMap = new HashMap<>(1);
//        int index = 1;
//        for(Object o : o2){
//            hashMap.put(index,o);
//            index++;
//        }
//        TestStatementUpdate("select * from t1",1,hashMap);
//        TestStatementDelete("select * from t1",1);
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

                    for (int i = 1; i <= len; ++i) {
                        // 输出数据
                        System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getString(i)));
                    }
                    System.out.print("\n");
                }
                rs.absolute(1);
                System.out.println(rs.getBoolean(1));
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

    public static void TestStatementUpdate(String sql) throws Exception{
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
            stmt.executeUpdate(sql);
            ResultSet rs = stmt.getResultSet();
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            int len = resultSetMetaData.getColumnCount();
            // 展开结果集数据库
            while (rs.next()) {
                // 通过字段检索

                for (int i = 1; i <= len; ++i) {
                    // 输出数据
                    System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getString(i)));
                }
                System.out.print("\n");
            }

            rs.close();
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

    public static void TestPreparedStatement(String select, String presql, Object[] objects) throws Exception{
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
            stmt = conn.prepareStatement(presql);
            int index = 1;
            for(Object o: objects){
                stmt.setObject(index,o);
                ++index;
            }

            stmt.execute();
            ResultSet rs = stmt.executeQuery(select);

            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            int len = resultSetMetaData.getColumnCount();
            // 展开结果集数据库
            while (rs.next()) {
                // 通过字段检索

                for (int i = 1; i <= len; ++i) {
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

    public static void TestPreparedStatementBantch(String select, String bantchsql, Object[] objects) throws Exception{
        System.out.println("TestPreparedStatementBantch begin");
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
            stmt = conn.prepareStatement(bantchsql);
            for(int i=0; i<objects.length; ++i){
                int index = 1;
                Object[] o = (Object[]) objects[i];
                for(Object o1: o){
                    stmt.setObject(index,o1);
                    ++index;
                }
                stmt.addBatch();
            }


            stmt.executeBatch();

            ResultSet rs = stmt.executeQuery(select);

            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            int len = resultSetMetaData.getColumnCount();
            // 展开结果集数据库
            while (rs.next()) {
                // 通过字段检索

                for (int i = 1; i <= len; ++i) {
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
        System.out.println("TestPreparedStatementBantch end");
    }

    public static void TestStatementUpdate(String select, int row, HashMap<Integer,Object> hashMap) throws Exception{
        System.out.println("TestStatementUpdate begin");
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
            ResultSet rs = stmt.executeQuery(select);
            rs.absolute(row);

            for (int key : hashMap.keySet()){
                rs.updateObject(key,hashMap.get(key));
            }

            rs.updateRow();
            rs.beforeFirst();


            //rs = stmt.executeQuery(select);

            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            int len = resultSetMetaData.getColumnCount();
            // 展开结果集数据库
            while (rs.next()) {
                // 通过字段检索

                for (int i = 1; i <= len; ++i) {
                    // 输出数据
                    System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getObject(i)));
                }
                System.out.print("\n");
            }
            rs.close();


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
        System.out.println("TestStatementUpdate end");
    }

    public static void TestStatementInsert(String select,Object[] objects) throws Exception{
        System.out.println("TestStatementInsert begin");
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
            ResultSet rs = stmt.executeQuery(select);
            rs.next();
            rs.moveToInsertRow();
            int index = 1;
            for(Object o : objects){
                rs.updateObject(index,o);
                ++index;
            }
            rs.insertRow();
            rs.beforeFirst();

            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            int len = resultSetMetaData.getColumnCount();
            // 展开结果集数据库
            while (rs.next()) {
                // 通过字段检索

                for (int i = 1; i <= len; ++i) {
                    // 输出数据
                    System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getObject(i)));
                }
                System.out.print("\n");
            }
            rs.close();


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
        System.out.println("TestStatementInsert end");
    }

    public static void TestStatementDelete(String select, int row) throws Exception{
        System.out.println("TestStatementDelete begin");
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
            ResultSet rs = stmt.executeQuery(select);
            rs.absolute(row);
            rs.deleteRow();
            rs.beforeFirst();

            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            int len = resultSetMetaData.getColumnCount();
            // 展开结果集数据库
            while (rs.next()) {
                // 通过字段检索

                for (int i = 1; i <= len; ++i) {
                    // 输出数据
                    System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getObject(i)));
                }
                System.out.print("\n");
            }
            rs.close();


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
        System.out.println("TestStatementDelete end");
    }
}
