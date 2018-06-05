package com.dolphindb.jdbc;

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
    private static final String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";

    private static final String path_All = "/data/dballdata";

    private static final String path_data = "data/t1";

    private static final String DB_URL = MessageFormat.format("jdbc:dolphindb://localhost:8848?databasePath={0}{1}",System.getProperty("user.dir").replaceAll("\\\\","/"),path_All);

    private static final String DB_URL1 = "jdbc:dolphindb://";

    public static void main(String[] args) throws Exception{
//      CreateTable(System.getProperty("user.dir")+"/data/createTable.java");

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

        int len = o3[0].length;
        int n = o3.length;

        HashMap<Integer,Object> map = new HashMap<>(len+1);
        Object[] o4 = new Object[len];
        Vector vector;

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


        TestPreparedStatement("t1 = loadTable(system_db,`t1)","insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",o4);


        TestResultSetInsert("t1 = loadTable(system_db,`t1)","select * from t1",o1,false);
        TestResultSetInsert("t1 = loadTable(system_db,`t1)","select * from t1",o1,true);

        TestResultSetUpdate("t1 = loadTable(system_db,`t1)","select * from t1",o1,false);
        TestResultSetUpdate("t1 = loadTable(system_db,`t1)","select * from t1",o1,true);

        TestResultSetDelete("t1 = loadTable(system_db,`t1)","select * from t1",2,false);
        TestResultSetDelete("t1 = loadTable(system_db,`t1)","select * from t1",2,true);

        TestPreparedStatementBantch("t1 = loadTable(system_db,`t1)","select * from t1","insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",o3);


        String sql = "bool = [true, false];\n" +
                "int = [1, 2];\n" +
                "bool_single = true;\n" +
                "int_single = 1;\n" +
                "t1 = table(bool, int);\n" +
                "insert into t1 values (bool_single, int_single);\n" +
                "tableInsert(t1, (bool_single, int_single));\n" +
                "select * from t1;\n" +
                "delete from t1 where int=1;\n" +
                "update t1 set int = 2 where int = 1;\n" +
                "t1;";

        TestStatementExecute(DB_URL1,sql);

    }

    public static void CreateTable(String fileName){
        DBConnection db=null;
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            StringBuilder sb = new StringBuilder();
            String s = null;
            while ((s = bufferedReader.readLine()) != null){
                sb.append(s).append("\n");
            }
            sb.append(Driver.DB).append(" = ").append("database(\"").
                    append(System.getProperty("user.dir").replaceAll("\\\\","/")).append(path_All).append("\")\n");
            sb.append("saveTable(").append(Driver.DB).append(", t1, `t1);\n");
            db = new DBConnection();
            db.connect("127.0.0.1",8848);
            db.run(sb.toString());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(db != null) db.close();
        }
    }

    public static void TestStatementExecute(String url,String sql) throws Exception{
        System.out.println("TestStatementExecute begin");
        Connection conn = null;
        Statement stmt = null;
        try{
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);
            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(url);

            // 执行查询
            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement();

            ResultSet rs = null;

            int UpdateCount = -1;

            if(stmt.execute(sql)){
                rs = stmt.getResultSet();
                printData(rs);
            }else {
                UpdateCount =  stmt.getUpdateCount();
                if(UpdateCount != -1) {
                    System.out.println(UpdateCount + " row affected");
                }
            }

            while (true){
                if(stmt.getMoreResults()){
                    rs =  stmt.getResultSet();
                    printData(rs);
                }else{
                    UpdateCount =  stmt.getUpdateCount();
                    if(UpdateCount != -1) {
                        System.out.println(UpdateCount + "row affected");
                    }else{
                        break;
                    }
                }
            }
            if(rs != null) {
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
        System.out.println("TestStatementExecute end");
    }


    public static void TestPreparedStatement(String loadTable,String presql, Object[] objects) throws Exception{
        System.out.println("TestStatement begin");

        Connection conn = null;
        PreparedStatement stmt = null;
        try{
            // 注册 JDBC 驱动
            Class.forName("com.dolphindb.jdbc.Driver");

            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL);

            // 执行查询
            System.out.println(" 实例化Statement对象...");
            stmt = conn.prepareStatement(presql);
            stmt.execute(loadTable);
            int index = 1;
            for(Object o: objects){
                stmt.setObject(index,o);
                ++index;
            }

            ResultSet rs = null;

            int UpdateCount = -1;

            if(stmt.execute()){
                rs = stmt.getResultSet();
                printData(rs);
            }else {
                UpdateCount =  stmt.getUpdateCount();
                if(UpdateCount != -1) {
                    System.out.println(UpdateCount + " row affected");
                }
            }
            while (true){
                if(stmt.getMoreResults()){
                    rs =  stmt.getResultSet();
                    printData(rs);
                }else{
                    UpdateCount =  stmt.getUpdateCount();
                    if(UpdateCount != -1) {
                        System.out.println(UpdateCount + "row affected");
                    }else{
                        break;
                    }
                }
            }
            if(rs != null) {
                rs.close();
            }
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

    public static void TestPreparedStatementBantch(String loadTable,String select, String bantchsql, Object[] objects) throws Exception{
        System.out.println("TestPreparedStatementBantch begin");

        Connection conn = null;
        PreparedStatement stmt = null;
        try{
            // 注册 JDBC 驱动
            Class.forName("com.dolphindb.jdbc.Driver");

            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL);

            // 执行查询
            System.out.println(" 实例化Statement对象...");
            stmt = conn.prepareStatement(bantchsql);

            stmt.execute(loadTable);
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

            printData(rs);
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

    public static void TestResultSetUpdate(String loadTable,String select, int row, HashMap<Integer,Object> hashMap) throws Exception{
        System.out.println("TestResultSetUpdate begin");

        Connection conn = null;
        Statement stmt = null;
        try{
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL);

            // 执行查询
            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement();
            stmt.execute(loadTable);
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
        System.out.println("TestResultSetUpdate end");
    }

    public static void TestResultSetInsert(String loadTable, String select, Object[] objects, boolean isInsert) throws Exception{
        System.out.println("TestResultSetInsert begin");

        Connection conn = null;
        Statement stmt = null;
        try{
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL);

            // 执行查询
            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
            stmt.execute(loadTable);
            ResultSet rs = stmt.executeQuery(select);

            printData(rs);

            rs.absolute(2);
            rs.moveToInsertRow();
            int index = 1;
            for(Object o : objects){
                rs.updateObject(index,o);
                ++index;
            }

            if(isInsert) {
                rs.insertRow();
            }
            //rs.cancelRowUpdates();
            rs.beforeFirst();

            printData(rs);

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
        System.out.println("TestResultSetInsert end");
    }

    public static void TestResultSetUpdate(String loadTable,String select, Object[] objects, boolean isUpdate) throws Exception{
        System.out.println("TestResultSetInsert begin");

        Connection conn = null;
        Statement stmt = null;
        try{
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL);

            // 执行查询
            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_UPDATABLE);
            stmt.execute(loadTable);
            ResultSet rs = stmt.executeQuery(select);

            printData(rs);

            rs.absolute(2);
            int index = 1;
            for(Object o : objects){
                rs.updateObject(index,o);
                ++index;
            }
            if(isUpdate) {
                rs.updateRow();
            }
            //rs.cancelRowUpdates();
            rs.beforeFirst();

            printData(rs);

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
        System.out.println("TestResultSetInsert end");
    }

    public static void TestResultSetDelete(String loadTable,String select, int row, boolean isDelete) throws Exception{
        System.out.println("TestResultSetDelete begin");

        Connection conn = null;
        Statement stmt = null;
        try{
            // 注册 JDBC 驱动
            Class.forName("com.dolphindb.jdbc.Driver");

            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL);

            // 执行查询
            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement();
            stmt.execute(loadTable);
            ResultSet rs = stmt.executeQuery(select);
            printData(rs);
            rs.absolute(row);
            if(isDelete) {
                rs.deleteRow();
            }
            rs.beforeFirst();

            printData(rs);


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
        System.out.println("TestResultSetDelete end");
    }

    public static void printData(ResultSet rs) throws SQLException{
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
    }
}
