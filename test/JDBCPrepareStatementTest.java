import com.dolphindb.jdbc.JDBCResultSet;
import com.dolphindb.jdbc.TypeCast;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
//import java.util.Date;
import org.junit.*;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
//import com.xxdb.DBConnection;

public class JDBCPrepareStatementTest {
    static String HOST = JDBCTestUtil.HOST;
    static int PORT = JDBCTestUtil.PORT;
    Properties LOGININFO = new Properties();
    String JDBC_DRIVER;
    String DB_URL ;
    Statement stm ;
    Connection conn;
    @Before
    public void SetUp(){
        JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        LOGININFO = new Properties();
        LOGININFO.put("user", "admin");
        LOGININFO.put("password", "123456");
        DB_URL = "jdbc:dolphindb://"+HOST+":"+PORT;
        JDBCTestUtil.LOGININFO.put("user", "admin");
        JDBCTestUtil.LOGININFO.put("password", "123456");
        conn = JDBCTestUtil.getConnection(JDBCTestUtil.LOGININFO);
        try {
            stm = conn.createStatement();
        }catch (SQLException ex){

        }
    }
    public static boolean CreateDfsTable(String host, Integer port) {
        boolean success = false;
        DBConnection db = null;
        try {
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n" +
                    "t = table(1..10000 as id, take(1, 10000) as val) \n" +
                    "db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10001) \n" +
                    "db.createPartitionedTable(t, `pt, `id).append!(t) \n";
            db = new DBConnection();
            db.connect(host, port);
            db.run(script);
            success = true;
        } catch (Exception e) {
            e.printStackTrace();
            success = false;
        } finally {
            if (db != null) {
                db.close();
            }
            return success;
        }
    }
    public static boolean createPartitionTable(String dataType){
        boolean success = false;
        DBConnection db = null;

        try{
            String script = "login(`admin, `123456); \n"+
                    "if(existsDatabase('dfs://test_append_type'))" +
                    "{ dropDatabase('dfs://test_append_type')} \n"+
                    "t = table(10:0,`id`dataType,[INT,"+dataType+"]) \n"+
                    "db=database('dfs://test_append_type', RANGE, 1 2001 4001 6001 8001 10000001) \n"+
                    "db.createPartitionedTable(t, `pt, `id) \n";
            db = new DBConnection();
            db.connect(HOST, PORT);
            db.run(script);
            success = true;
        }catch(Exception e){
            e.printStackTrace();
            success = false;
        }finally{
            if(db != null){
                db.close();
            }
            return success;
        }
    }
    public static boolean createPartitionTable1() {
        boolean success = false;
        DBConnection db = null;
        try{
            String script = "login(`admin, `123456); \n"+
                    "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                    "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                    "colNames=\"col\"+string(1..29);\n" +
                    "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                    "t=table(1:0,colNames,colTypes);\n" +
                    "db=database('dfs://test_append_type_tsdb1', RANGE, 1 2001 4001 6001 8001 10001 10000000,,'TSDB') \n"+
                    "db.createPartitionedTable(t, `pt, `col1,,`col1)\n";
            db = new DBConnection();
            db.connect(HOST, PORT);
            db.run(script);
            success = true;
        }catch(Exception e){
            e.printStackTrace();
            success = false;
        }finally{
            if(db != null){
                db.close();
            }
            return success;
        }

    }
    public static boolean createPartitionTable_Array(String dataType) {
        boolean success = false;
        DBConnection db = null;
        try{
            String script = "login(`admin, `123456); \n"+
                    "if(existsDatabase('dfs://test_append_array_tsdb1'))" +
                    "{ dropDatabase('dfs://test_append_array_tsdb1')} \n"+
//                    "colNames=\"col\"+string(1..26);\n" +
                    "colNames=\"col\"+string(1..2);\n" +
                    "colTypes=[INT,"+dataType+"[]];\n" +
//                    "colTypes=[INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],UUID[],DATEHOUR[],IPADDR[],INT128[],COMPLEX[],POINT[],DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(19)[]];\n" +
                    "t=table(1:0,colNames,colTypes);\n" +
                    "db=database('dfs://test_append_array_tsdb1', RANGE, 1 2001 4001 6001 8001 10001 10000000,,'TSDB') \n"+
                    "db.createPartitionedTable(t, `pt, `col1,,`col1)\n";
            db = new DBConnection();
            db.connect(HOST, PORT);
            db.run(script);
            success = true;
        }catch(Exception e){
            e.printStackTrace();
            success = false;
        }finally{
            if(db != null){
                db.close();
            }
            return success;
        }
    }
    public static boolean createPartitionTable_insert() {
        boolean success = false;
        DBConnection db = null;
        try{
            String script = "login(`admin, `123456); \n"+
                    "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                    "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                    "colNames=\"col\"+string(1..29);\n" +
                    "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                    "t=table(1:0,colNames,colTypes);\n" +
                    "insert into t values(2,true,'a',2h,2,22l,9999.12.06,9999.06M,23:59:59.999,23:59m,23:59:59,9999.12.31 23:59:59,9999.12.31 23:59:59.999,00:00:00.999999999,9999.06.13 13:30:10.008007006,2f,2.12345,\"1212\",\"1212\",uuid(\"00000000-0000-0001-0000-000000000002\"),datehour(2012.06.13 13:30:10),ipaddr(\"0::1:0:0:0:2\"),int128(\"00000000000000010000000000000002\"),blob(\"123\"),complex(111,1),point(1,2),decimal32(1.1,2),decimal64(1.1,7),decimal128(1.1,19));\n" +
                    "insert into t values(1,,,,,,,,,,,,,,,,,,,,,,,,,,,,);\n" +
                    "db=database('dfs://test_append_type_tsdb1', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n"+
                    "pt=db.createPartitionedTable(t, `pt,`col1,,`col1)\n" +
                    "pt.append!(t)\n";
            db = new DBConnection();
            db.connect(HOST, PORT);
            db.run(script);
            Connection conn;
            success = true;
        }catch(Exception e){
            e.printStackTrace();
            success = false;
        }finally{
            if(db != null){
                db.close();
            }
            return success;
        }

    }
    public static boolean createTable1() {
        boolean success = false;
        DBConnection db = null;
        try{
            String script = "login(`admin, `123456); \n"+
                    "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                    "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                    "colNames=\"col\"+string(1..29);\n" +
                    "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                    "t=table(1:0,colNames,colTypes);\n" +
                    "db=database('dfs://test_append_type_tsdb1', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n"+
                    "db.createTable(t, `pt,,`col1)\n";
            db = new DBConnection();
            db.connect(HOST, PORT);
            db.run(script);
            success = true;
        }catch(Exception e){
            e.printStackTrace();
            success = false;
        }finally{
            if(db != null){
                db.close();
            }
            return success;
        }

    }
    public static boolean createTSDBPartitionTable(String dataType){
        boolean success = false;
        DBConnection db = null;
        try{
            String script = "login(`admin, `123456); \n"+
                    "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                    "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                    "t = table(10:0,`id`dataType,[INT,"+dataType+"]) \n"+
                    "db=database('dfs://test_append_type_tsdb1', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n"+
                    "db.createPartitionedTable(t, `pt, `id,,`id) \n";
            db = new DBConnection();
            db.connect(HOST, PORT);
            db.run(script);
            success = true;
        }catch(Exception e){
            e.printStackTrace();
            success = false;
        }finally{
            if(db != null){
                db.close();
            }
            return success;
        }
    }
    public static boolean createWideTable(String databaseName, int intColNum, int doubleColNum) throws IOException {
        boolean success = false;
        DBConnection db = null;
        try{
        db = new DBConnection();
        db.connect(HOST, PORT);
        StringBuilder colNames = new StringBuilder("`time`id");
        StringBuilder colTypes = new StringBuilder("`TIMESTAMP`SYMBOL");
        for(int i = 0; i < intColNum; ++i) {
            colNames.append("`int_" + i);
            colTypes.append("`INT");
        }
        for(int i = 0; i < doubleColNum; ++i) {
            colNames.append("`double_" + i);
            colTypes.append("`DOUBLE");
        }
        System.out.println(colNames.toString());
        System.out.println(colTypes.toString());
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('" + databaseName + "'))" +
                "{ dropDatabase('" + databaseName + "')} \n" +
                "t=table(1:0," + colNames + "," + colTypes + ");\n"+
                "db=database('" + databaseName + "', HASH, [SYMBOL,10],,'TSDB') \n"+
                "db.createPartitionedTable(t, `pt, `id,,`id) \n";
        db.run(script);
            success = true;
        }catch(Exception e){
            e.printStackTrace();
            success = false;
        }finally{
            if(db != null){
                db.close();
            }
            return success;
        }
    }
    @Test
    public  void TestPrepareStatement() throws ClassNotFoundException,SQLException{
        CreateDfsTable(HOST,PORT);
        Properties info = new Properties();
        info.put("user", "admin");
        info.put("password", "123456");
        Connection conn = null;
        String preSql="select count(*) from loadTable('dfs://db_testStatement','pt') group by id ";
        PreparedStatement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL, info);
        stmt = conn.prepareStatement(preSql);
        stmt.executeQuery();
    }

    @Test
    public void Test_prepareStatement_inmemory_query_SetInt() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..100 as id, 201..300 as val)");
            pstmt = conn.prepareStatement("select * from t where id <= ?");
            pstmt.setInt(1, 10);
            rs = pstmt.executeQuery();
            for (int i = 1; i <= 10; i++) {
                rs.absolute(i);
                org.junit.Assert.assertEquals(rs.getInt(1), i);
            }
            ResultSetMetaData rsmd = rs.getMetaData();
            int len = rsmd.getColumnCount();
            org.junit.Assert.assertEquals(len, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_SetString() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(take('aa' 'bb' 'cc' 'dd' 'ee',100) as id, 201..300 as val)");
            pstmt = conn.prepareStatement("select * from t where id = ?");
            pstmt.setString(1, "dd");
            rs = pstmt.executeQuery();
            for (int i = 1; i <= 20; i++) {
                rs.absolute(i);
                org.junit.Assert.assertEquals(rs.getString(1), "dd");
            }
            ResultSetMetaData rsmd = rs.getMetaData();
            int len = rsmd.getColumnCount();
            org.junit.Assert.assertEquals(len, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_SetFloat() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(take(1.1f 1.2f 1.3f 1.4f 1.5f,100) as id, 201..300 as val)");
            pstmt = conn.prepareStatement("select * from t where id = ?");
            pstmt.setFloat(1, 1.1f);
            rs = pstmt.executeQuery();
//			while (rs.next()) {
//				System.out.println(rs.getFloat(1) + " " + rs.getInt(2));
//			}
//			for(int i=1; i<=20; i++){
//				rs.absolute(i);
////				org.junit.Assert.assertEquals(rs.getFloat(1),1.1f,0);
//				System.out.println(rs.getFloat("id"));
//			}
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_SetDouble() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(take(1.1 1.2 1.3 1.4 1.5,100) as id, 201..300 as val)");
            pstmt = conn.prepareStatement("select * from t where id = ?");
            pstmt.setDouble(1, 1.1);
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getDouble(1)+" "+rs.getInt(2));
//			}
            for (int i = 1; i <= 20; i++) {
                rs.absolute(i);
                org.junit.Assert.assertEquals(rs.getDouble("id"), 1.1, 0);
            }
            ResultSetMetaData rsmd = rs.getMetaData();
            int len = rsmd.getColumnCount();
            org.junit.Assert.assertEquals(len, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_setNull_Boolean_byte_bytes() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        java.sql.Time insertTime;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..100 as id, 201..300 as val)");
            //setNull
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 101);
            pstmt.setNull(2, Types.INTEGER);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            rs.absolute(101);
            org.junit.Assert.assertEquals(rs.getInt(2), 0);
            //setBoolean
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 102);
            pstmt.setBoolean(2,false);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            rs.absolute(102);
            org.junit.Assert.assertEquals(rs.getBoolean(2), false);
            //setByte
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 103);
            pstmt.setByte(2,(byte) 100 );
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            rs.absolute(103);
            org.junit.Assert.assertEquals(rs.getByte(2), 100);
            //setBytes
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 104);
            pstmt.setBytes(2,new byte[]{2});
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            rs.absolute(104);
            org.junit.Assert.assertEquals(rs.getInt(2), 2);
            //setBigDecimal
//			pstmt = conn.prepareStatement("insert into t values(?,?)");
//			pstmt.setInt(1, 105);
//			BigDecimal a =new BigDecimal("12");
//			System.out.println(a);
//			pstmt.setBigDecimal(2,a);
//			pstmt.executeUpdate();
//			pstmt = conn.prepareStatement("select * from t");
//			rs = pstmt.executeQuery();
//			rs.absolute(105);
//			org.junit.Assert.assertEquals(rs.getBigDecimal(2), 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_setTime() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        java.sql.Time insertTime;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..3 as id,[13:30:10,13:30:11,13:30:12] as time)");
            //setTime
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 4);
            Time t = new Time(13,30,13);
            pstmt.setTime(2,t);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getTime(2));
//			}
            rs.absolute(4);
            org.junit.Assert.assertEquals(rs.getTime(2),t);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_setDate() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..3 as id,[2020.08.01,2020.08.02,2020.08.03] as date)");
            //setDate
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 4);
            Date date = Date.valueOf("2020-08-04");
            pstmt.setDate(2,date);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            rs.absolute(4);
            org.junit.Assert.assertEquals(rs.getDate(2), date);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    @Test
    public void Test_prepareStatement_inmemory_query_setShort() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..3 as id,[121h 122h 123h] as val)");
            //setDate
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 4);
            pstmt.setShort(2,(short)124);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            rs.absolute(4);
            org.junit.Assert.assertEquals(rs.getShort(2), 124);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_setLong() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..3 as id,[1l 2l 3l] as val)");
            //setDate
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 4);
            pstmt.setLong(2,4L);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            rs.absolute(4);
            org.junit.Assert.assertEquals(rs.getLong(2), 4l);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_setTimestamp() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..3 as id,[2012.06.13T13:30:10.006 2012.06.13T13:30:10.007 2012.06.13T13:30:10.008] as timestamp)");
            //setDate
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 4);
            Timestamp t = Timestamp.valueOf("2012-06-13 13:30:10.009");
            pstmt.setTimestamp(2, t);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getTimestamp(2));
//			}
            rs.absolute(4);
            org.junit.Assert.assertEquals(rs.getTimestamp(2), t);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_clearParameters() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String s = "No value specified for parameter 1";
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..100 as id, 201..300 as val)");
            pstmt = conn.prepareStatement("select * from t where id <= ?");
            pstmt.setInt(1, 10);
            pstmt.clearParameters();
            rs = pstmt.executeQuery();
        } catch (Exception e) {
            org.junit.Assert.assertThat(e.getMessage(), containsString(s));
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_setObject() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String s = "No value specified for parameter 1";
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..3 as id,[1.1f 1.2f 1.3f] as val)");
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setObject(1, 4);
            pstmt.setObject(2, 1.4f);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t ");
            rs = pstmt.executeQuery();
            rs.absolute(4);
            org.junit.Assert.assertEquals(rs.getInt(1), 4);
            org.junit.Assert.assertEquals(rs.getDouble(2), 1.4f,0);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_execute() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            //insert
            stmt.execute("t=table(1..100 as id, 201..300 as val)");
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 101);
            pstmt.setInt(2, 301);
            pstmt.execute();
            pstmt = conn.prepareStatement("select * from t");
            pstmt.execute();
            rs = pstmt.getResultSet();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
            rs.absolute(101);
            org.junit.Assert.assertEquals(rs.getInt(1), 101);
            org.junit.Assert.assertEquals(rs.getInt(2), 301);
            //update
            pstmt = conn.prepareStatement("update t set val=? where id=?");
            pstmt.setInt(1, 333);
            pstmt.setInt(2, 101);
            pstmt.execute();
            pstmt = conn.prepareStatement("select * from t");
            pstmt.execute();
            rs = pstmt.getResultSet();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
            rs.absolute(101);
            org.junit.Assert.assertEquals(rs.getInt(2), 333);
            //delete
            pstmt = conn.prepareStatement("delete from t where id=?");
            pstmt.setInt(1, 101);
            pstmt.execute();
            pstmt = conn.prepareStatement("select * from t");
            pstmt.execute();
            rs = pstmt.getResultSet();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
            rs.last();
            int rowCount = rs.getRow();
            org.junit.Assert.assertEquals(rowCount, 100);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_executeUpdate() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..100 as id, 201..300 as val)");
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 101);
            pstmt.setInt(2, 301);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
            rs.absolute(101);
            org.junit.Assert.assertEquals(rs.getInt(1), 101);
            org.junit.Assert.assertEquals(rs.getInt(2), 301);
            pstmt = conn.prepareStatement("update t set val=? where id=?");
            pstmt.setInt(1, 333);
            pstmt.setInt(2, 101);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
            rs.absolute(101);
            org.junit.Assert.assertEquals(rs.getInt(2), 333);
            pstmt = conn.prepareStatement("delete from t where id=?");
            pstmt.setInt(1, 101);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
            rs.last();
            int rowCount = rs.getRow();
            org.junit.Assert.assertEquals(rowCount, 100);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_Batch() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..100 as id, 201..300 as val)");
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 101);
            pstmt.setInt(2, 301);
            pstmt.addBatch();
            pstmt.setInt(1,102);
            pstmt.setInt(2, 302);
            pstmt.addBatch();
//			pstmt.clearBatch();
            pstmt.executeBatch();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
//			pstmt.clearBatch();
//			pstmt = conn.prepareStatement("select * from t");
//			rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
//			rs.absolute(102);
//			org.junit.Assert.assertEquals(rs.getInt(1), 101);
//			org.junit.Assert.assertEquals(rs.getInt(2), 301);
//			rs.last();
//			int rowCount = rs.getRow();
//			org.junit.Assert.assertEquals(rowCount, 102);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    @Test
    public void Test_prepareStatement_dfs_query_Batch1() throws Exception {
        String tableName = "trade";
        String dataBase = "dfs://test_jdbc_sql";
        Connection conn = JDBCSQLSelectTest.getConnection();
        String sql = "insert into trade values(?,?,?,?,?,?,?,?)";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd");
        PreparedStatement ps = null;
        StringBuilder sb = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        sb2.append("if(existsDatabase(\""+ dataBase +"\"))dropDatabase(\""+ dataBase +"\")\n");
        sb2.append("db=database(\""+ dataBase +"\", RANGE, `A`F`K`O`S`ZZZ)\n");
        sb2.append("t1=table(100:0, `PERMNO`date`TICKER`PRC`VOL`BID`ASK`SHROUT, [INT, DATE, SYMBOL, DOUBLE, INT, DOUBLE, DOUBLE,INT])\n");
        sb2.append("db.createPartitionedTable(t1,`trade, `TICKER)\n");
        ps = conn.prepareStatement(sb2.toString());
        ps.execute();
        ps.execute("trade=loadTable(\""+ dataBase +"\", `"+ tableName +")");
        ps = conn.prepareStatement(sql);
        ps.clearBatch();
        ps.setInt(1, Integer.parseInt("1"));
        LocalDate localDate = LocalDate.parse("2010.03.13", formatter);
        ps.setObject(2, new BasicDate(localDate));
        ps.setString(3, "NULL");
        ps.setNull(4, Types.DOUBLE);
        ps.setNull(5, Types.INTEGER);
        ps.setDouble(6, Double.parseDouble("6.1"));
        ps.setDouble(7, Double.parseDouble("6.1"));
        ps.setNull(8, Types.INTEGER);
        ps.addBatch();

        ps.setInt(1, Integer.parseInt("2"));
        ps.setObject(2, new BasicDate(localDate));
        ps.setString(3, "NULL");
        ps.setNull(4, Types.DOUBLE);
        ps.setNull(5, Types.INTEGER);
        ps.setDouble(6, Double.parseDouble("6.1"));
        ps.setDouble(7, Double.parseDouble("6.1"));
        ps.setNull(8, Types.INTEGER);
        ps.addBatch();
        ps.executeBatch();
        ps.clearBatch();
    }

    @Test
    public void Test_prepareStatement_inmemory_query_getMetaData() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..100 as id, 201..300 as val)");
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            String tableName = rs.getMetaData().getTableName(1);
            org.junit.Assert.assertEquals(tableName, "id");
            String columnName = rs.getMetaData().getColumnName(1);
            org.junit.Assert.assertEquals(columnName, "id");
            int columnCount = rs.getMetaData().getColumnCount();
            org.junit.Assert.assertEquals(columnCount, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_executeQuery1() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1 2 3 4 5 6 7 8 10 9 as id, 21 22 23 24 25 22 27 28 29 30 as val)");
            //where
            pstmt = conn.prepareStatement("select * from t where id = ?");
            pstmt.setInt(1,1);
            rs = pstmt.executeQuery();
            rs.last();
            int rowCount = rs.getRow();
            org.junit.Assert.assertEquals(rowCount,1);
            rs.absolute(1);
            org.junit.Assert.assertEquals(rs.getInt(2), 21);
            //order by
            pstmt = conn.prepareStatement("select * from t order by id");
            rs = pstmt.executeQuery();
            rs.absolute(10);
            org.junit.Assert.assertEquals(rs.getInt(1), 10);
            org.junit.Assert.assertEquals(rs.getInt(2), 29);
            //group by having
            pstmt = conn.prepareStatement("select count(val) from t group by val having count(val)>1");
            rs = pstmt.executeQuery();
            rs.last();
            int rowCount1 = rs.getRow();
            org.junit.Assert.assertEquals(rowCount1,1);
            rs.absolute(1);
            org.junit.Assert.assertEquals(rs.getInt(2), 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_executeQuery2() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t = table(`IBM`IBM`IBM`IBM`C`C`C`C as sym, 09:30m 09:30m 09:31m 09:31m 09:30m 09:30m 09:31m 09:31m as time, 1..8 as qty, 10 .. 17 as price)");
            //cgroup by
            pstmt = conn.prepareStatement("select wavg(price, qty) as wvap from t where sym = 'IBM' cgroup by time order by time");
            rs = pstmt.executeQuery();
            rs.absolute(1);
            org.junit.Assert.assertEquals(rs.getString(1), "09:30");
            org.junit.Assert.assertEquals(rs.getDouble(2), 10.666667,0.0001);
            rs.absolute(2);
            org.junit.Assert.assertEquals(rs.getString(1), "09:31");
            org.junit.Assert.assertEquals(rs.getInt(2), 12);
            rs.last();
            int rowCount = rs.getRow();
            org.junit.Assert.assertEquals(rowCount,2);
            //context by
            pstmt = conn.prepareStatement("select wavg(price, qty) as wvap from t context by sym");
            rs = pstmt.executeQuery();
            rs.last();
            int rowCount1 = rs.getRow();
            org.junit.Assert.assertEquals(rowCount1,8);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_executeQuery3() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("sym = `C`MS`MS`MS`IBM`IBM`C`C`C$SYMBOL\n" +
                    "price= 49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29\n" +
                    "qty = 2200 1900 2100 3200 6800 5400 1300 2500 8800\n" +
                    "timestamp = [09:34:07,09:35:42,09:36:51,09:36:59,09:35:47,09:36:26,09:34:16,09:35:26,09:36:12]\n" +
                    "t = table(timestamp, sym, qty, price)");
            //pivot by
            pstmt = conn.prepareStatement("select last(price) from t pivot by timestamp.minute(), sym");
            rs = pstmt.executeQuery();
            String columnName = rs.getMetaData().getColumnName(1);
            org.junit.Assert.assertEquals(columnName, "minute_timestamp");
            String columnName1 = rs.getMetaData().getColumnName(2);
            org.junit.Assert.assertEquals(columnName1, "C");
            String columnName2 = rs.getMetaData().getColumnName(3);
            org.junit.Assert.assertEquals(columnName2, "IBM");
            rs.last();
            int rowCount = rs.getRow();
            org.junit.Assert.assertEquals(rowCount,3);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_dfs_query_setInt() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
            pstmt = conn.prepareStatement("select * from pt where id <= ?");
            pstmt.setInt(1, 10);
            rs = pstmt.executeQuery();
            for (int i = 1; i <= 10; i++) {
                rs.absolute(i);
                org.junit.Assert.assertEquals(rs.getInt(1), i);
            }
            ResultSetMetaData rsmd = rs.getMetaData();
            int len = rsmd.getColumnCount();
            org.junit.Assert.assertEquals(len, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_dfs_query_setString() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n" +
                    "t = table(1..10000 as id, take('aa' 'bb' 'cc' 'dd' 'ee',10000) as val) \n" +
                    "db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10001) \n" +
                    "db.createPartitionedTable(t, `pt, `id).append!(t) \n";
            stmt.execute(script);
            stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
            pstmt = conn.prepareStatement("select * from pt where val = ?");
            pstmt.setString(1, "dd");
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getString(2));
//			}
            for (int i = 1; i <= 200; i++) {
                rs.absolute(i);
                org.junit.Assert.assertEquals(rs.getString(2), "dd");
            }
            ResultSetMetaData rsmd = rs.getMetaData();
            int len = rsmd.getColumnCount();
            org.junit.Assert.assertEquals(len, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_dfs_query_setFloat() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n" +
                    "t = table(1..10000 as id, take(1.1f 1.2f 1.3f 1.4f 1.5f,10000) as val) \n" +
                    "db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10001) \n" +
                    "db.createPartitionedTable(t, `pt, `id).append!(t) \n";
            stmt.execute(script);
            stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
            pstmt = conn.prepareStatement("select * from pt where val = ?");
            pstmt.setFloat(1, 1.1f);
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getFloat(2));
//			}
//			for (int i = 1; i <= 200; i++) {
//				rs.absolute(i);
//				org.junit.Assert.assertEquals(rs.getFloat(2), 1.1f, 1);
//			}
            ResultSetMetaData rsmd = rs.getMetaData();
            int len = rsmd.getColumnCount();
            org.junit.Assert.assertEquals(len, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_dfs_query_setDouble() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n" +
                    "t = table(1..10000 as id, take(1.1 1.2 1.3 1.4 1.5,10000) as val) \n" +
                    "db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10001) \n" +
                    "db.createPartitionedTable(t, `pt, `id).append!(t) \n";
            stmt.execute(script);
            stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");

            pstmt = conn.prepareStatement("select * from pt where val = ?");
            pstmt.setDouble(1, 1.1);
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getDouble(2));
//			}
            for (int i = 1; i <= 200; i++) {
                rs.absolute(i);
                org.junit.Assert.assertEquals(rs.getDouble(2), 2, 6);
                System.out.println(rs.getDouble(2));
            }
            ResultSetMetaData rsmd = rs.getMetaData();
            int len = rsmd.getColumnCount();
            org.junit.Assert.assertEquals(len, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_dfs_query_executeUpdate_() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String s = "only local in-memory table can update";
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n" +
                    "t = table(1..10000 as id, 10001..20000 as val) \n" +
                    "db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10002) \n" +
                    "db.createPartitionedTable(t, `pt, `id).append!(t) \n";
            stmt.execute(script);
            stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
            stmt.execute("t1 = table(10001 as id,20001 as val)");
            pstmt = conn.prepareStatement("pt.append!(t1)");
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from pt");
            rs = pstmt.executeQuery();
            rs.absolute(10001);
            org.junit.Assert.assertEquals(rs.getInt(1), 10001);
            org.junit.Assert.assertEquals(rs.getInt(2), 20001);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            pstmt = conn.prepareStatement("update pt set val=20002 where id=10001");
            pstmt.executeUpdate();
        } catch (Exception e) {
            org.junit.Assert.assertThat(e.getMessage(), containsString(s));
        }
        try {
            pstmt = conn.prepareStatement("delete from pt where id=10001");
            pstmt.executeUpdate();
        } catch (Exception e) {
            org.junit.Assert.assertThat(e.getMessage(), containsString(s));
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void test_PreparedStatement_executeTable(){
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String s = "only local in-memory table can update";
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("use testOuRui;query()");
            rs = stmt.getResultSet();
            Assert.assertTrue(rs.next());
            System.out.println(rs.getString(1));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void test_PreparedStatement_executeScalar(){
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        JDBCResultSet rs = null;
        String s = "only local in-memory table can update";
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("use testOuRui;printStr('123')");
            rs = (JDBCResultSet) stmt.getResultSet();
            Assert.assertEquals("b123",rs.getResult().getString());
            Assert.assertTrue(rs.getResult().isScalar());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void test_PreparedStatement_executeVector(){
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        JDBCResultSet rs = null;
        String s = "only local in-memory table can update";
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("use testOuRui;printVector()");
            rs = (JDBCResultSet) stmt.getResultSet();
            Assert.assertEquals("[1,2,3,4]",rs.getResult().getString());
            Assert.assertTrue(rs.getResult().isVector());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    @Test
    public void test_PreparedStatement_execute() throws ClassNotFoundException, SQLException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        JDBCResultSet rs = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        Statement stm = conn.createStatement();
        String sql = "login('admin','123456');\n " +
                "t = table(1..100 as id, norm(1.0,0.1,100) as prc,take(`C`E,100) as ticker, take(2018.01.01..2018.10.18,100) as date, norm(15.0,0.1,100) as bid)" +
                "share t as tt" ;
        stm.execute(sql);
        PreparedStatement  ps = conn.prepareStatement("select * from tt where  id = ?, date = ?, ticker = ?");
        ps.setObject(1,1);
        ps.setObject(2,new BasicDate(LocalDate.parse("2018-01-01")));
        ps.setObject(3,"C");
        ps.execute();
        Assert.assertTrue(ps.getResultSet().next());
        conn.close();
    }

    @Test
    public void test_PreparedStatement_setObject() throws ClassNotFoundException, SQLException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        JDBCResultSet rs = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        Statement stm = conn.createStatement();
        String sql = "login('admin','123456');\n " +
                "t = table(1..100 as id, norm(1.0,0.1,100) as prc,take(`C`E,100) as ticker, take(2018.01.01..2018.10.18,100) as date, norm(15.0,0.1,100) as bid)" +
                "share t as tt" ;
        stm.execute(sql);
        PreparedStatement  ps = conn.prepareStatement("select * from tt where  id = ?, date = ?, ticker = ?");
        ps.setObject(1,1);
        ps.setObject(2,new BasicDate(LocalDate.parse("2018-01-01")));
        ps.setObject(3,new Thread());
        try{
            ps.execute();
        }catch(Exception e){
            System.out.println(e.getMessage());
            Assert.assertEquals("Unsupported type for parameter 3 class java.lang.Thread",e.getMessage());
        }
        conn.close();
    }

    @Test
    public void test_PreparedStatement_setObject_Date() throws ClassNotFoundException, SQLException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        JDBCResultSet rs = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        Statement stm = conn.createStatement();
        String sql = "login('admin','123456');\n " +
                "t = table(1..100 as id, norm(1.0,0.1,100) as prc,take(`C`E,100) as ticker, take(2012.06.13T13:30:10.008..2012.06.13T13:30:10.108,100) as date, norm(15.0,0.1,100) as bid)" +
                "share t as tt" ;
        stm.execute(sql);
        PreparedStatement  ps = conn.prepareStatement("select * from tt where  id = ?, date = ?");
        ps.setObject(1,1);
        ps.setObject(2,new java.util.Date(1));
        ps.execute();
        Assert.assertFalse(ps.getResultSet().next());
        PreparedStatement  ps1 = conn.prepareStatement(" update tt set date = ? where  id = ?");
        ps1.setObject(2,1);
        java.util.Date t = new java.util.Date();
        System.out.println(t);
        ps1.setObject(1,t);
        ps1.execute();
        PreparedStatement  ps2 = conn.prepareStatement("select   * from tt where  id = ?, date = ?");
        ps2.setObject(1,1);
        ps2.setObject(2,t);
        ps2.execute();
        Assert.assertTrue(ps2.getResultSet().next());
        conn.close();
    }
    @Test
    public void test_PreparedStatement_setObject_Date2() throws ClassNotFoundException, SQLException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        JDBCResultSet rs = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        Statement stm = conn.createStatement();
        String sql = "login('admin','123456');\n " +
                "t = table(1..100 as id, norm(1.0,0.1,100) as prc,take(`C`E,100) as ticker, take(2013.06.13..2013.06.23,100) as date, norm(15.0,0.1,100) as bid)" +
                "share t as tt" ;
        stm.execute(sql);
        PreparedStatement  ps = conn.prepareStatement("select * from tt where  id = ?, date = ?");
        ps.setObject(1,1);
        ps.setObject(2,new java.util.Date(1));
        ps.execute();
        Assert.assertFalse(ps.getResultSet().next());
        PreparedStatement  ps1 = conn.prepareStatement("update tt set date = ? where  id = ?");
        ps1.setObject(2,1);
        ps1.setObject(1,new java.util.Date(1));
        ps1.execute();
        PreparedStatement  ps2 = conn.prepareStatement("select * from tt where  id = ?, date = 1970.01.01");
        ps2.setObject(1,1);
        ps2.execute();
        Assert.assertTrue(ps2.getResultSet().next());
        conn.close();
    }

    @Test
    public void test_PreparedStatement_insert_into_Boolean() throws SQLException {
        createPartitionTable("BOOL");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setBoolean(2,true);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.BOOLEAN);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("dataType"), true);
        rs.next();
        rs.getBoolean("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Int() throws SQLException {
        createPartitionTable("INT");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setInt(2,100);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.INTEGER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
        rs.next();
        rs.getInt("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_Int_1() throws SQLException {
        createPartitionTable("INT");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        ps.setInt(2,100);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.INTEGER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
        rs.next();
        rs.getInt("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_Int_2() throws SQLException {
        createPartitionTable("INT");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("INSERt inTO pt Values(?,?)");
        ps.setInt(1,1);
        ps.setInt(2,100);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.INTEGER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
        rs.next();
        rs.getInt("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_Int_3() throws SQLException {
        createPartitionTable("INT");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt(id,dataType) values(?,?)");
        ps.setInt(1,1);
        ps.setInt(2,100);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.INTEGER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
        rs.next();
        rs.getInt("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_Int_4() throws SQLException {
        createPartitionTable("INT");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt (id,dataType) values(?,?)");
        ps.setInt(1,1);
        ps.setInt(2,100);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.INTEGER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
        rs.next();
        rs.getInt("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_Int_5() throws SQLException {
        createPartitionTable("INT");
        PreparedStatement ps = conn.prepareStatement("inSERT iNto loadTable('dfs://test_append_type','pt') ValUes(?,?)");
        ps.setInt(1,1);
        ps.setInt(2,100);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.INTEGER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
        rs.next();
        rs.getInt("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_Int_line_break() throws SQLException {
        createPartitionTable("INT");
        PreparedStatement ps = conn.prepareStatement("insert into \nloadTable('dfs://test_append_type','pt')\n"+" values(?,?)\n");
        ps.setInt(1,1);
        ps.setInt(2,100);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.INTEGER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
        rs.next();
        rs.getInt("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_Char() throws SQLException {
        createPartitionTable("CHAR");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setByte(2, (byte) 12);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.CHAR);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getByte("dataType"), 12);
        rs.next();
        rs.getByte("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Short() throws SQLException {
        createPartitionTable("SHORT");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setShort(2, (short) 12);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getShort("dataType"), 12);
        rs.next();
        rs.getShort("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Long() throws SQLException {
        createPartitionTable("LONG");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setLong(2, (long) 12);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getLong("dataType"), 12);
        rs.next();
        rs.getLong("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Float() throws SQLException {
        createPartitionTable("FLOAT");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setFloat(2, (float) 12.23);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.FLOAT);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("dataType"),4);
        rs.next();
        rs.getFloat("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Double() throws SQLException {
        createPartitionTable("DOUBLE");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setDouble(2, (double) 12.23);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.DOUBLE);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("dataType"),4);
        rs.next();
        rs.getDouble("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_String() throws SQLException {
        createPartitionTable("STRING");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setString(2, "test1");
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals("test1",rs.getString("dataType"));
        rs.next();
        rs.getString("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Symbol() throws SQLException {
        createPartitionTable("SYMBOL");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setString(2, "test1");
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals("test1",rs.getString("dataType"));
        rs.next();
        rs.getString("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Date() throws SQLException {
        createPartitionTable("DATE");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setDate(2, Date.valueOf(LocalDate.of(2021,1,1)));
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.DATE);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("dataType"));
        rs.next();
        rs.getDate("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Month() throws SQLException {
        createPartitionTable("MONTH");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        BasicMonth tmp_month = new BasicMonth(YearMonth.of(2021,1));
        ps.setObject(2, tmp_month);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Time() throws SQLException {
        createPartitionTable("TIME");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setTime(2, Time.valueOf(LocalTime.of(1,1,1)));
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.TIME);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("dataType"));
        rs.next();
        rs.getTime("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Minute() throws SQLException {
        createPartitionTable("MINUTE");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        BasicMinute tmp_minute = new BasicMinute(LocalTime.of(1,1));
        ps.setObject(2, tmp_minute);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Second() throws SQLException {
        createPartitionTable("SECOND");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        BasicSecond tmp_second = new BasicSecond(LocalTime.of(1,1,1));
        ps.setObject(2, tmp_second);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Datetime() throws SQLException {
        createPartitionTable("DATETIME");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        BasicDateTime tmp_datetime = new BasicDateTime(LocalDateTime.of(2021,1,1,1,1,1));
        ps.setObject(2, tmp_datetime);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Timestamp() throws SQLException {
        createPartitionTable("TIMESTAMP");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        BasicTimestamp tmp_timestamp = new BasicTimestamp(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(2, tmp_timestamp);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Nanotime() throws SQLException {
        createPartitionTable("NANOTIME");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        BasicNanoTime tmp_nanotime = new BasicNanoTime(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(2, tmp_nanotime);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1),rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Nanotimestamp() throws SQLException {
        createPartitionTable("NANOTIMESTAMP");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        BasicNanoTimestamp tmp_nanotimestamp = new BasicNanoTimestamp(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(2, tmp_nanotimestamp);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Datehour() throws SQLException {
        createPartitionTable("DATEHOUR");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        BasicDateHour tmp_datehour = new BasicDateHour(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(2, tmp_datehour);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Uuid() throws SQLException {
        createPartitionTable("UUID");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        BasicUuid uuids = new BasicUuid(1,2);
        ps.setObject(2, uuids);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Ipaddr() throws SQLException {
        createPartitionTable("IPADDR");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        BasicIPAddr ipaddrs = new BasicIPAddr(1,2);
        ps.setObject(2, ipaddrs);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Int128() throws SQLException {
        createPartitionTable("INT128");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        BasicInt128 int128 = new BasicInt128(1,2);
        ps.setObject(2, int128);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Blob() throws SQLException {
        createTSDBPartitionTable("BLOB");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2, "TEST BLOB");
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals("TEST BLOB",rs.getString("dataType"));
        //getBlob还没有实现,等实现后替换getString
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Complex() throws SQLException {
        createPartitionTable("COMPLEX");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        BasicComplex complexs = new BasicComplex(1,2);
        ps.setObject(2, complexs);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_Point() throws SQLException {
        createPartitionTable("POINT");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        BasicPoint points = new BasicPoint(0,0);
        ps.setObject(2, points);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_Mul() throws SQLException {
        createPartitionTable("INT");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        for(int i =1;i<=10;i++) {
            ps.setInt(1, i);
            ps.setInt(2, i*100);
            ps.addBatch();
        }
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select count(*) from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getInt("count"), 10);
    }

    @Test
    public void test_PreparedStatement_insert_into_AddBatch() throws SQLException {
        createPartitionTable("INT");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        for(int i =1;i<=10000000;i++) {
            ps.setInt(1, i);
            ps.setInt(2, i*100);
            ps.addBatch();
        }
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select count(*) from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals(10000000, rs.getLong("count"));
    }
    @Test
    public void test_PreparedStatement_insert_into_Decimal32() throws SQLException {
        createPartitionTable("DECIMAL32(4)");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,37,4);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals("123421.0001",rs.getObject("dataType").toString());
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_Decimal64() throws SQLException {
        createPartitionTable("DECIMAL64(4)");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,38,4);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals("123421.0001",rs.getObject("dataType").toString());
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_Decimal128() throws SQLException {
        createPartitionTable("DECIMAL128(4)");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"123421.00012",39,4);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        rs.next();
        org.junit.Assert.assertEquals("123421.0001",rs.getObject("dataType").toString());
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_col_Boolean() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col2) values(?,?)");
        ps.setInt(1,1);
        ps.setBoolean(2,true);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.BOOLEAN);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
        rs.next();
        rs.getBoolean("col2");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_col_Char() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col3) values(?,?)");
        ps.setInt(1,1);
        ps.setByte(2, (byte) 12);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.CHAR);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getByte("col3"), 12);
        rs.next();
        rs.getByte("col3");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Short() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col4) values(?,?)");
        ps.setInt(1,1);
        ps.setShort(2, (short) 12);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
        rs.next();
        rs.getShort("col4");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Int() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col5) values(?,?)");
        ps.setInt(1,1);
        ps.setInt(2,100);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.INTEGER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
        rs.next();
        rs.getInt("col5");
        org.junit.Assert.assertTrue(rs.wasNull());
    }


    @Test
    public void test_PreparedStatement_insert_into_col_Long() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col6) values(?,?)");
        ps.setInt(1,1);
        ps.setLong(2, (long) 12);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
        rs.next();
        rs.getLong("col6");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_col_Date() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col7) values(?,?)");
        ps.setInt(1,1);
        ps.setDate(2, Date.valueOf(LocalDate.of(2021,1,1)));
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.DATE);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
        rs.next();
        rs.getDate("col7");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Month() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col8) values(?,?)");
        ps.setInt(1,1);
        BasicMonth tmp_month = new BasicMonth(YearMonth.of(2021,1));
        ps.setObject(2, tmp_month);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
        rs.next();
        rs.getObject("col8");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Time() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col9) values(?,?)");
        ps.setInt(1,1);
        ps.setTime(2, Time.valueOf(LocalTime.of(1,1,1)));
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.TIME);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
        rs.next();
        rs.getTime("col9");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Minute() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col10) values(?,?)");
        ps.setInt(1,1);
        BasicMinute tmp_minute = new BasicMinute(LocalTime.of(1,1));
        ps.setObject(2, tmp_minute);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
        rs.next();
        rs.getObject("col10");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Second() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col11) values(?,?)");
        ps.setInt(1,1);
        BasicSecond tmp_second = new BasicSecond(LocalTime.of(1,1,1));
        ps.setObject(2, tmp_second);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("col11"));
        rs.next();
        rs.getObject("col11");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Datetime() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col12) values(?,?)");
        ps.setInt(1,1);
        BasicDateTime tmp_datetime = new BasicDateTime(LocalDateTime.of(2021,1,1,1,1,1));
        ps.setObject(2, tmp_datetime);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col12"));
        rs.next();
        rs.getObject("col12");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Timestamp() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col13) values(?,?)");
        ps.setInt(1,1);
        BasicTimestamp tmp_timestamp = new BasicTimestamp(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(2, tmp_timestamp);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col13"));
        rs.next();
        rs.getObject("col13");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Nanotime() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col14) values(?,?)");
        ps.setInt(1,1);
        BasicNanoTime tmp_nanotime = new BasicNanoTime(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(2, tmp_nanotime);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1),rs.getObject("col14"));
        rs.next();
        rs.getObject("col14");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Nanotimestamp() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col15) values(?,?)");
        ps.setInt(1,1);
        BasicNanoTimestamp tmp_nanotimestamp = new BasicNanoTimestamp(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(2, tmp_nanotimestamp);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("col15"));
        rs.next();
        rs.getObject("col15");
        org.junit.Assert.assertTrue(rs.wasNull());
    }


    @Test
    public void test_PreparedStatement_insert_into_col_Float() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col16) values(?,?)");
        ps.setInt(1,1);
        ps.setFloat(2, (float) 12.23);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.FLOAT);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
        rs.next();
        rs.getFloat("col16");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Double() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col17) values(?,?)");
        ps.setInt(1,1);
        ps.setDouble(2, (double) 12.23);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.DOUBLE);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
        rs.next();
        rs.getDouble("col17");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_col_Symbol() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col18) values(?,?)");
        ps.setInt(1,1);
        ps.setString(2, "test1");
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals("test1",rs.getString("col18"));
        rs.next();
        rs.getString("col18");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_String() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col19) values(?,?)");
        ps.setInt(1,1);
        ps.setString(2, "test1");
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals("test1",rs.getString("col19"));
        rs.next();
        rs.getString("col19");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_col_Uuid() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col20) values(?,?)");
        ps.setInt(1,1);
        BasicUuid uuids = new BasicUuid(1,2);
        ps.setObject(2, uuids);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
        rs.next();
        rs.getObject("col20");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Datehour() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col21) values(?,?)");
        ps.setInt(1,1);
        BasicDateHour tmp_datehour = new BasicDateHour(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(2, tmp_datehour);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
        rs.next();
        rs.getObject("col21");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Ipaddr() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col22) values(?,?)");
        ps.setInt(1,1);
        BasicIPAddr ipaddrs = new BasicIPAddr(1,2);
        ps.setObject(2, ipaddrs);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
        rs.next();
        rs.getObject("col22");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Int128() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col23) values(?,?)");
        ps.setInt(1,1);
        BasicInt128 int128 = new BasicInt128(1,2);
        ps.setObject(2, int128);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
        rs.next();
        rs.getObject("col23");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Blob() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col24) values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2, "TEST BLOB");
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
        //getBlob还没有实现,等实现后替换getString
        rs.next();
        rs.getObject("col24");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Complex() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col25) values(?,?)");
        ps.setInt(1,1);
        BasicComplex complexs = new BasicComplex(1,2);
        ps.setObject(2, complexs);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
        rs.next();
        rs.getObject("col25");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Point() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col26) values(?,?)");
        ps.setInt(1,1);
        BasicPoint points = new BasicPoint(0,0);
        ps.setObject(2, points);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
        rs.next();
        rs.getObject("col26");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_PreparedStatement_insert_into_col_Decimal32() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col27) values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,37,4);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        rs.getObject("col27");
        org.junit.Assert.assertTrue(rs.wasNull());
        rs.next();
        org.junit.Assert.assertEquals("123421.00",rs.getObject("col27").toString());

    }
    @Test
    public void test_PreparedStatement_insert_into_col_Decimal64() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col28) values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,38,4);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        rs.getObject("col28");
        org.junit.Assert.assertTrue(rs.wasNull());
        rs.next();
        org.junit.Assert.assertEquals("123421.0001200",rs.getObject("col28").toString());

    }
    @Test
    public void test_PreparedStatement_insert_into_col_Decimal128() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col29) values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"123421.00012",39,4);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        rs.getObject("col29");
        org.junit.Assert.assertTrue(rs.wasNull());
        rs.next();
        org.junit.Assert.assertEquals("123421.0001200000000000000",rs.getObject("col29").toString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_all_dateType1() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt') values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        BasicInt tmp_int = new BasicInt(1);
        ps.setObject(1,tmp_int);
        BasicBoolean tmp_Boolean = new BasicBoolean(true);
        ps.setObject(2,tmp_Boolean);
        BasicByte tmp_Byte = new BasicByte((byte) 12);
        ps.setObject(3, tmp_Byte);
        BasicShort tmp_Short = new BasicShort((short) 12);
        ps.setObject(4, tmp_Short);
        BasicInt tmp_int1 = new BasicInt(100);
        ps.setObject(5,tmp_int1);
        BasicLong tmp_Long = new BasicLong((long) 12);
        ps.setObject(6, tmp_Long);
        BasicDate tmp_Date = new BasicDate(LocalDate.of(2021,1,1));
        ps.setObject(7, tmp_Date);
        BasicMonth tmp_month = new BasicMonth(YearMonth.of(2021,1));
        ps.setObject(8, tmp_month);
        BasicTime tmp_Time = new BasicTime(LocalTime.of(1,1,1));
        ps.setObject(9, tmp_Time);
        BasicMinute tmp_minute = new BasicMinute(LocalTime.of(1,1));
        ps.setObject(10, tmp_minute);
        BasicSecond tmp_second = new BasicSecond(LocalTime.of(1,1,1));
        ps.setObject(11, tmp_second);
        BasicDateTime tmp_datetime = new BasicDateTime(LocalDateTime.of(2021,1,1,1,1,1));
        ps.setObject(12, tmp_datetime);
        BasicTimestamp tmp_timestamp = new BasicTimestamp(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(13, tmp_timestamp);
        BasicNanoTime tmp_nanotime = new BasicNanoTime(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(14, tmp_nanotime);
        BasicNanoTimestamp tmp_nanotimestamp = new BasicNanoTimestamp(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(15, tmp_nanotimestamp);
        BasicFloat tmp_Float = new BasicFloat((float) 12.23);
        ps.setObject(16, tmp_Float);
        BasicDouble tmp_Double = new BasicDouble((double) 12.23);
        ps.setObject(17, tmp_Double);
        BasicString tmp_String = new BasicString("test1");
        ps.setObject(18, tmp_String);
        BasicString tmp_Symbol = new BasicString("test1");
        ps.setObject(19, tmp_Symbol);
        BasicUuid uuids = new BasicUuid(1,2);
        ps.setObject(20, uuids);
        BasicDateHour tmp_datehour = new BasicDateHour(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(21, tmp_datehour);
        BasicIPAddr ipaddrs = new BasicIPAddr(1,2);
        ps.setObject(22, ipaddrs);
        BasicInt128 int128 = new BasicInt128(1,2);
        ps.setObject(23, int128);
        ps.setObject(24, "TEST BLOB");
        BasicComplex complexs = new BasicComplex(1,2);
        ps.setObject(25, complexs);
        BasicPoint points = new BasicPoint(0,0);
        ps.setObject(26, points);
        BasicDecimal32 tmp_decimal32 = new BasicDecimal32("1421.00012",5);
        ps.setObject(27,tmp_decimal32);
        BasicDecimal64 tmp_decimal64 = new BasicDecimal64("123421.00012",5);
        ps.setObject(28,tmp_decimal64);
        BasicDecimal128 tmp_decimal128 = new BasicDecimal128("123421.00012",5);
        ps.setObject(29,tmp_decimal128);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
        org.junit.Assert.assertEquals(rs.getByte("col3"), 12);
        org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
        org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
        org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
        org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
        org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
        org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("col11"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col12"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col13"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1),rs.getObject("col14"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("col15"));
        org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
        org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
        org.junit.Assert.assertEquals("test1",rs.getString("col18"));
        org.junit.Assert.assertEquals("test1",rs.getString("col19"));
        org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
        org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
        org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
        org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
        org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
        org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
        org.junit.Assert.assertEquals("1421.00",rs.getObject("col27").toString());
        org.junit.Assert.assertEquals("123421.0001200",rs.getObject("col28").toString());
        org.junit.Assert.assertEquals("123421.0001200000000000000",rs.getObject("col29").toString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_all_dateType_2() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28,col29) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        ps.setInt(1,1000);
        ps.setBoolean(2,true);
        ps.setByte(3, (byte) 12);
        ps.setShort(4, (short) 12);
        ps.setInt(5,100);
        ps.setLong(6, (long) 12);
        ps.setDate(7, Date.valueOf(LocalDate.of(2021,1,1)));
        BasicMonth tmp_month = new BasicMonth(YearMonth.of(2021,1));
        ps.setObject(8, tmp_month);
        ps.setTime(9, Time.valueOf(LocalTime.of(1,1,1)));
        BasicMinute tmp_minute = new BasicMinute(LocalTime.of(1,1));
        ps.setObject(10, tmp_minute);
        BasicSecond tmp_second = new BasicSecond(LocalTime.of(1,1,1));
        ps.setObject(11, tmp_second);
        BasicDateTime tmp_datetime = new BasicDateTime(LocalDateTime.of(2021,1,1,1,1,1));
        ps.setObject(12, tmp_datetime);
        BasicTimestamp tmp_timestamp = new BasicTimestamp(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(13, tmp_timestamp);
        BasicNanoTime tmp_nanotime = new BasicNanoTime(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(14, tmp_nanotime);
        BasicNanoTimestamp tmp_nanotimestamp = new BasicNanoTimestamp(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(15, tmp_nanotimestamp);
        ps.setFloat(16, (float) 12.23);
        ps.setDouble(17, (double) 12.23);
        ps.setString(18, "test1");
        ps.setString(19, "test1");
        BasicUuid uuids = new BasicUuid(1,2);
        ps.setObject(20, uuids);
        BasicDateHour tmp_datehour = new BasicDateHour(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(21, tmp_datehour);
        BasicIPAddr ipaddrs = new BasicIPAddr(1,2);
        ps.setObject(22, ipaddrs);
        BasicInt128 int128 = new BasicInt128(1,2);
        ps.setObject(23, int128);
        ps.setObject(24, "TEST BLOB");
        BasicComplex complexs = new BasicComplex(1,2);
        ps.setObject(25, complexs);
        BasicPoint points = new BasicPoint(0,0);
        ps.setObject(26, points);
        ps.setObject(27,123421.00012,37,4);
        ps.setObject(28,123421.00012,38,4);
        ps.setObject(29,"123421.00012",39,4);
        ps.addBatch();
        ps.executeBatch();
        //JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        //BasicTable bt = (BasicTable) rs.getResult();
        //System.out.println(bt.getString());
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
        org.junit.Assert.assertEquals(rs.getByte("col3"), 12);
        org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
        org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
        org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
        org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
        org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
        org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("col11"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col12"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col13"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1),rs.getObject("col14"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("col15"));
        org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
        org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
        org.junit.Assert.assertEquals("test1",rs.getString("col18"));
        org.junit.Assert.assertEquals("test1",rs.getString("col19"));
        org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
        org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
        org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
        org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
        org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
        org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
        org.junit.Assert.assertEquals("123421.0001",rs.getObject("col27").toString());
        org.junit.Assert.assertEquals("123421.0001",rs.getObject("col28").toString());
        org.junit.Assert.assertEquals("123421.0001",rs.getObject("col29").toString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_all_dateType_3() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(col21,col22,col23,col24,col25,col26,col27,col28,col29,col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        BasicDateHour tmp_datehour = new BasicDateHour(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(1, tmp_datehour);
        BasicIPAddr ipaddrs = new BasicIPAddr(1,2);
        ps.setObject(2, ipaddrs);
        BasicInt128 int128 = new BasicInt128(1,2);
        ps.setObject(3, int128);
        ps.setObject(4, "TEST BLOB");
        BasicComplex complexs = new BasicComplex(1,2);
        ps.setObject(5, complexs);
        BasicPoint points = new BasicPoint(0,0);
        ps.setObject(6, points);
        ps.setObject(7,123421.00012,37,4);
        ps.setObject(8,123421.00012,38,4);
        ps.setObject(9,"123421.00012",39,4);
        ps.setInt(10,1000);
        ps.setBoolean(11,true);
        ps.setByte(12, (byte) 12);
        ps.setShort(13, (short) 12);
        ps.setInt(14,100);
        ps.setLong(15, (long) 12);
        ps.setDate(16, Date.valueOf(LocalDate.of(2021,1,1)));
        BasicMonth tmp_month = new BasicMonth(YearMonth.of(2021,1));
        ps.setObject(17, tmp_month);
        ps.setTime(18, Time.valueOf(LocalTime.of(1,1,1)));
        BasicMinute tmp_minute = new BasicMinute(LocalTime.of(1,1));
        ps.setObject(19, tmp_minute);
        BasicSecond tmp_second = new BasicSecond(LocalTime.of(1,1,1));
        ps.setObject(20, tmp_second);
        BasicDateTime tmp_datetime = new BasicDateTime(LocalDateTime.of(2021,1,1,1,1,1));
        ps.setObject(21, tmp_datetime);
        BasicTimestamp tmp_timestamp = new BasicTimestamp(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(22, tmp_timestamp);
        BasicNanoTime tmp_nanotime = new BasicNanoTime(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(23, tmp_nanotime);
        BasicNanoTimestamp tmp_nanotimestamp = new BasicNanoTimestamp(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(24, tmp_nanotimestamp);
        ps.setFloat(25, (float) 12.23);
        ps.setDouble(26, (double) 12.23);
        ps.setString(27, "test1");
        ps.setString(28, "test1");
        BasicUuid uuids = new BasicUuid(1,2);
        ps.setObject(29, uuids);
        ps.addBatch();
        ps.executeBatch();
        //JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        //BasicTable bt = (BasicTable) rs.getResult();
        //System.out.println(bt.getString());
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
        org.junit.Assert.assertEquals(rs.getByte("col3"), 12);
        org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
        org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
        org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
        org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
        org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
        org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("col11"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col12"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col13"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1),rs.getObject("col14"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("col15"));
        org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
        org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
        org.junit.Assert.assertEquals("test1",rs.getString("col18"));
        org.junit.Assert.assertEquals("test1",rs.getString("col19"));
        org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
        org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
        org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
        org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
        org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
        org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
        org.junit.Assert.assertEquals("123421.00",rs.getObject("col27").toString());
        org.junit.Assert.assertEquals("123421.0001200",rs.getObject("col28").toString());
        org.junit.Assert.assertEquals("123421.0001200000000000000",rs.getObject("col29").toString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_all_dateType4() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt') values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        ps.setInt(1,1000);
        ps.setBoolean(2,true);
        ps.setByte(3, (byte) 12);
        ps.setShort(4, (short) 12);
        ps.setInt(5,100);
        ps.setLong(6, (long) 12);
        ps.setDate(7, Date.valueOf(LocalDate.of(2021,1,1)));
        ps.setObject(8, LocalDate.of(2021,1,1));
        ps.setTime(9, Time.valueOf(LocalTime.of(1,1,1)));
        ps.setObject(10, LocalTime.of(1,1));
        ps.setObject(11, LocalTime.of(1,1,1));
        ps.setObject(12, LocalDateTime.of(2021,1,1,1,1,1));
        ps.setObject(13, LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(14, LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(15, LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setFloat(16, (float) 12.23);
        ps.setDouble(17, (double) 12.23);
        ps.setString(18, "test1");
        ps.setString(19, "test1");
        ps.setObject(20, "00000000-0000-0001-0000-000000000002");
        ps.setObject(21, LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(22, "0::1:0:0:0:2");
        ps.setObject(23, "00000000000000010000000000000002");
        ps.setObject(24, "TEST BLOB");
        Entity complexs = new BasicComplex(1,2);
        ps.setObject(25, complexs);
        Entity points = new BasicPoint(0,0);
        ps.setObject(26, points);
        ps.setObject(27,123421.00012,37,4);
        ps.setObject(28,123421.00012,38,4);
        ps.setObject(29,"123421.00012",39,4);
        ps.addBatch();
        ps.executeBatch();
        //JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        //BasicTable bt = (BasicTable) rs.getResult();
        //System.out.println(bt.getString());
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
        org.junit.Assert.assertEquals(rs.getByte("col3"), 12);
        org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
        org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
        org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
        org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
        org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
        org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("col11"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col12"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col13"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1),rs.getObject("col14"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("col15"));
        org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
        org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
        org.junit.Assert.assertEquals("test1",rs.getString("col18"));
        org.junit.Assert.assertEquals("test1",rs.getString("col19"));
        org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
        org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
        org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
        org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
        org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
        org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
        org.junit.Assert.assertEquals("123421.00",rs.getObject("col27").toString());
        org.junit.Assert.assertEquals("123421.0001200",rs.getObject("col28").toString());
        org.junit.Assert.assertEquals("123421.0001200000000000000",rs.getObject("col29").toString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_all_dateType5() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("INSERT INTO loadTable('dfs://test_append_type_tsdb1','pt') VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        ps.setInt(1,1000);
        ps.setBoolean(2,true);
        ps.setByte(3, (byte) 12);
        ps.setShort(4, (short) 12);
        ps.setInt(5,100);
        ps.setLong(6, (long) 12);
        ps.setDate(7, Date.valueOf(LocalDate.of(2021,1,1)));
        ps.setObject(8, LocalDate.of(2021,1,1));
        ps.setTime(9, Time.valueOf(LocalTime.of(1,1,1)));
        ps.setObject(10, LocalTime.of(1,1));
        ps.setObject(11, LocalTime.of(1,1,1));
        ps.setObject(12, LocalDateTime.of(2021,1,1,1,1,1));
        ps.setObject(13, LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(14, LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(15, LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setFloat(16, (float) 12.23);
        ps.setDouble(17, (double) 12.23);
        ps.setString(18, "test1");
        ps.setString(19, "test1");
        ps.setObject(20, "00000000-0000-0001-0000-000000000002");
        ps.setObject(21, LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(22, "0::1:0:0:0:2");
        ps.setObject(23, "00000000000000010000000000000002");
        ps.setObject(24, "TEST BLOB");
        Entity complexs = new BasicComplex(1,2);
        ps.setObject(25, complexs);
        Entity points = new BasicPoint(0,0);
        ps.setObject(26, points);
        ps.setObject(27,123421.00012,37,4);
        ps.setObject(28,123421.00012,38,4);
        ps.setObject(29,"123421.00012",39,4);
        ps.addBatch();
        ps.executeBatch();
        //JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        //BasicTable bt = (BasicTable) rs.getResult();
        //System.out.println(bt.getString());
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
        org.junit.Assert.assertEquals(rs.getByte("col3"), 12);
        org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
        org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
        org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
        org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
        org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
        org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("col11"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col12"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col13"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1),rs.getObject("col14"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("col15"));
        org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
        org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
        org.junit.Assert.assertEquals("test1",rs.getString("col18"));
        org.junit.Assert.assertEquals("test1",rs.getString("col19"));
        org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
        org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
        org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
        org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
        org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
        org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
        org.junit.Assert.assertEquals("123421.00",rs.getObject("col27").toString());
        org.junit.Assert.assertEquals("123421.0001200",rs.getObject("col28").toString());
        org.junit.Assert.assertEquals("123421.0001200000000000000",rs.getObject("col29").toString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_not_addBatch() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt') values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        BasicInt tmp_int = new BasicInt(1);
        ps.setObject(1,tmp_int);
        BasicBoolean tmp_Boolean = new BasicBoolean(true);
        ps.setObject(2,tmp_Boolean);
        BasicByte tmp_Byte = new BasicByte((byte) 12);
        ps.setObject(3, tmp_Byte);
        BasicShort tmp_Short = new BasicShort((short) 12);
        ps.setObject(4, tmp_Short);
        BasicInt tmp_int1 = new BasicInt(100);
        ps.setObject(5,tmp_int1);
        BasicLong tmp_Long = new BasicLong((long) 12);
        ps.setObject(6, tmp_Long);
        BasicDate tmp_Date = new BasicDate(LocalDate.of(2021,1,1));
        ps.setObject(7, tmp_Date);
        BasicMonth tmp_month = new BasicMonth(YearMonth.of(2021,1));
        ps.setObject(8, tmp_month);
        BasicTime tmp_Time = new BasicTime(LocalTime.of(1,1,1));
        ps.setObject(9, tmp_Time);
        BasicMinute tmp_minute = new BasicMinute(LocalTime.of(1,1));
        ps.setObject(10, tmp_minute);
        BasicSecond tmp_second = new BasicSecond(LocalTime.of(1,1,1));
        ps.setObject(11, tmp_second);
        BasicDateTime tmp_datetime = new BasicDateTime(LocalDateTime.of(2021,1,1,1,1,1));
        ps.setObject(12, tmp_datetime);
        BasicTimestamp tmp_timestamp = new BasicTimestamp(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(13, tmp_timestamp);
        BasicNanoTime tmp_nanotime = new BasicNanoTime(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(14, tmp_nanotime);
        BasicNanoTimestamp tmp_nanotimestamp = new BasicNanoTimestamp(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(15, tmp_nanotimestamp);
        BasicFloat tmp_Float = new BasicFloat((float) 12.23);
        ps.setObject(16, tmp_Float);
        BasicDouble tmp_Double = new BasicDouble((double) 12.23);
        ps.setObject(17, tmp_Double);
        BasicString tmp_String = new BasicString("test1");
        ps.setObject(18, tmp_String);
        BasicString tmp_Symbol = new BasicString("test1");
        ps.setObject(19, tmp_Symbol);
        BasicUuid uuids = new BasicUuid(1,2);
        ps.setObject(20, uuids);
        BasicDateHour tmp_datehour = new BasicDateHour(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(21, tmp_datehour);
        BasicIPAddr ipaddrs = new BasicIPAddr(1,2);
        ps.setObject(22, ipaddrs);
        BasicInt128 int128 = new BasicInt128(1,2);
        ps.setObject(23, int128);
        ps.setObject(24, "TEST BLOB");
        BasicComplex complexs = new BasicComplex(1,2);
        ps.setObject(25, complexs);
        BasicPoint points = new BasicPoint(0,0);
        ps.setObject(26, points);
        BasicDecimal32 tmp_decimal32 = new BasicDecimal32("1421.00012",5);
        ps.setObject(27,tmp_decimal32);
        BasicDecimal64 tmp_decimal64 = new BasicDecimal64("123421.00012",5);
        ps.setObject(28,tmp_decimal64);
        BasicDecimal128 tmp_decimal128 = new BasicDecimal128("123421.00012",5);
        ps.setObject(29,tmp_decimal128);
//        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
        org.junit.Assert.assertEquals(rs.getByte("col3"), 12);
        org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
        org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
        org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
        org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
        org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
        org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("col11"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col12"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col13"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1),rs.getObject("col14"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("col15"));
        org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
        org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
        org.junit.Assert.assertEquals("test1",rs.getString("col18"));
        org.junit.Assert.assertEquals("test1",rs.getString("col19"));
        org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
        org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
        org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
        org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
        org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
        org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
        org.junit.Assert.assertEquals("1421.00",rs.getObject("col27").toString());
        org.junit.Assert.assertEquals("123421.0001200",rs.getObject("col28").toString());
        org.junit.Assert.assertEquals("123421.0001200000000000000",rs.getObject("col29").toString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_not_addBatch_1() throws SQLException {
        createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt') values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
//        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(false, rs.next());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_not_support() throws SQLException {
        createPartitionTable1();
        String re = null;
        try{
            PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt') values(1,,,,,,,,,,,,,,,,,,,,,,,,,,,,)");
            ps.executeUpdate();
        }catch(Exception e){
            re = e.getMessage();
        }
        org.junit.Assert.assertEquals(true, re.contains("check the SQl insert into loadTable"));
    }
    @Test  //1212eeee
    public void test_PreparedStatement_insert_into_DFS_all_dateType_mul() throws SQLException {
        createPartitionTable1();
        long start = System.nanoTime();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt') values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        for(int i =1;i<=100000;i++) {
            ps.setInt(1, i);
            ps.setBoolean(2, true);
            ps.setByte(3, (byte) 12);
            ps.setShort(4, (short) 12);
            ps.setInt(5, 100);
            ps.setLong(6, (long) 12);
            ps.setDate(7, Date.valueOf(LocalDate.of(2021, 1, 1)));
            BasicMonth tmp_month = new BasicMonth(YearMonth.of(2021, 1));
            ps.setObject(8, tmp_month);
            ps.setTime(9, Time.valueOf(LocalTime.of(1, 1, 1)));
            BasicMinute tmp_minute = new BasicMinute(LocalTime.of(1, 1));
            ps.setObject(10, tmp_minute);
            BasicSecond tmp_second = new BasicSecond(LocalTime.of(1, 1, 1));
            ps.setObject(11, tmp_second);
            BasicDateTime tmp_datetime = new BasicDateTime(LocalDateTime.of(2021, 1, 1, 1, 1, 1));
            ps.setObject(12, tmp_datetime);
            BasicTimestamp tmp_timestamp = new BasicTimestamp(LocalDateTime.of(2021, 1, 1, 1, 1, 1, 001));
            ps.setObject(13, tmp_timestamp);
            BasicNanoTime tmp_nanotime = new BasicNanoTime(LocalDateTime.of(2021, 1, 1, 1, 1, 1, 001));
            ps.setObject(14, tmp_nanotime);
            BasicNanoTimestamp tmp_nanotimestamp = new BasicNanoTimestamp(LocalDateTime.of(2021, 1, 1, 1, 1, 1, 123456));
            ps.setObject(15, tmp_nanotimestamp);
            ps.setFloat(16, (float) 12.23);
            ps.setDouble(17, (double) 12.23);
            ps.setString(18, "test1");
            ps.setString(19, "test1");
            BasicUuid uuids = new BasicUuid(1, 2);
            ps.setObject(20, uuids);
            BasicDateHour tmp_datehour = new BasicDateHour(LocalDateTime.of(2021, 1, 1, 1, 1, 1, 123456));
            ps.setObject(21, tmp_datehour);
            BasicIPAddr ipaddrs = new BasicIPAddr(1, 2);
            ps.setObject(22, ipaddrs);
            BasicInt128 int128 = new BasicInt128(1, 2);
            ps.setObject(23, int128);
            ps.setObject(24, "TEST BLOB");
            BasicComplex complexs = new BasicComplex(1, 2);
            ps.setObject(25, complexs);
            BasicPoint points = new BasicPoint(0, 0);
            ps.setObject(26, points);
            ps.setObject(27, 123421.00012, 37, 4);
            ps.setObject(28, 123421.00012, 38, 4);
            ps.setObject(29, "123421.00012", 39, 4);
            ps.addBatch();
        }
        ps.executeBatch();
        long end = System.nanoTime();
        System.out.println("耗费时间："+ ((end - start) / 1000000000.0) + "秒");
        JDBCResultSet rs1 = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt = (BasicTable) rs1.getResult();
        org.junit.Assert.assertEquals(100000, bt.rows());
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        while (rs.next()) {
            org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
            org.junit.Assert.assertEquals(rs.getByte("col3"), 12);
            org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
            org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
            org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
            org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021, 1, 1)), rs.getDate("col7"));
            org.junit.Assert.assertEquals(YearMonth.of(2021, 1), rs.getObject("col8"));
            org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1, 1, 1)), rs.getTime("col9"));
            org.junit.Assert.assertEquals(LocalTime.of(1, 1), rs.getObject("col10"));
            org.junit.Assert.assertEquals(LocalTime.of(1, 1, 1), rs.getObject("col11"));
            org.junit.Assert.assertEquals(LocalDateTime.of(2021, 1, 1, 1, 1, 1), rs.getObject("col12"));
            org.junit.Assert.assertEquals(LocalDateTime.of(2021, 1, 1, 1, 1, 1), rs.getObject("col13"));
            org.junit.Assert.assertEquals(LocalTime.of(1, 1, 1, 1), rs.getObject("col14"));
            org.junit.Assert.assertEquals(LocalDateTime.of(2021, 1, 1, 1, 1, 1, 123456), rs.getObject("col15"));
            org.junit.Assert.assertEquals((float) 12.23, rs.getFloat("col16"), 4);
            org.junit.Assert.assertEquals((Double) 12.23, rs.getDouble("col17"), 4);
            org.junit.Assert.assertEquals("test1", rs.getString("col18"));
            org.junit.Assert.assertEquals("test1", rs.getString("col19"));
            org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"), rs.getObject("col20"));
            org.junit.Assert.assertEquals(LocalDateTime.of(2021, 1, 1, 1, 0), rs.getObject("col21"));
            org.junit.Assert.assertEquals("0::1:0:0:0:2", rs.getObject("col22"));
            org.junit.Assert.assertEquals("00000000000000010000000000000002", rs.getObject("col23"));
            org.junit.Assert.assertEquals("TEST BLOB", rs.getString("col24"));
            org.junit.Assert.assertEquals("1.0+2.0i", rs.getObject("col25"));
            org.junit.Assert.assertEquals("(0.0, 0.0)", rs.getObject("col26"));
            org.junit.Assert.assertEquals("123421.00",rs.getObject("col27").toString());
            org.junit.Assert.assertEquals("123421.0001200",rs.getObject("col28").toString());
            org.junit.Assert.assertEquals("123421.0001200000000000000",rs.getObject("col29").toString());
        }
    }
    @Test
    public void test_PreparedStatement_insert_into_memoryTable() throws SQLException {
        stm.execute("pt=table(1:0,`col1`dataType,[INT,INT])");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        ps.setInt(2,100);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.INTEGER);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
        rs.next();
        rs.getInt("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void test_PreparedStatement_insert_into_dimension_all_dateType() throws SQLException {
        createTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt') values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        ps.setInt(1,1000);
        ps.setBoolean(2,true);
        ps.setByte(3, (byte) 12);
        ps.setShort(4, (short) 12);
        ps.setInt(5,100);
        ps.setLong(6, (long) 12);
        ps.setDate(7, Date.valueOf(LocalDate.of(2021,1,1)));
        BasicMonth tmp_month = new BasicMonth(YearMonth.of(2021,1));
        ps.setObject(8, tmp_month);
        ps.setTime(9, Time.valueOf(LocalTime.of(1,1,1)));
        BasicMinute tmp_minute = new BasicMinute(LocalTime.of(1,1));
        ps.setObject(10, tmp_minute);
        BasicSecond tmp_second = new BasicSecond(LocalTime.of(1,1,1));
        ps.setObject(11, tmp_second);
        BasicDateTime tmp_datetime = new BasicDateTime(LocalDateTime.of(2021,1,1,1,1,1));
        ps.setObject(12, tmp_datetime);
        BasicTimestamp tmp_timestamp = new BasicTimestamp(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(13, tmp_timestamp);
        BasicNanoTime tmp_nanotime = new BasicNanoTime(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(14, tmp_nanotime);
        BasicNanoTimestamp tmp_nanotimestamp = new BasicNanoTimestamp(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(15, tmp_nanotimestamp);
        ps.setFloat(16, (float) 12.23);
        ps.setDouble(17, (double) 12.23);
        ps.setString(18, "test1");
        ps.setString(19, "test1");
        BasicUuid uuids = new BasicUuid(1,2);
        ps.setObject(20, uuids);
        BasicDateHour tmp_datehour = new BasicDateHour(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(21, tmp_datehour);
        BasicIPAddr ipaddrs = new BasicIPAddr(1,2);
        ps.setObject(22, ipaddrs);
        BasicInt128 int128 = new BasicInt128(1,2);
        ps.setObject(23, int128);
        ps.setObject(24, "TEST BLOB");
        BasicComplex complexs = new BasicComplex(1,2);
        ps.setObject(25, complexs);
        BasicPoint points = new BasicPoint(0,0);
        ps.setObject(26, points);
        ps.setObject(27,123421.00012,37,4);
        ps.setObject(28,123421.00012,38,4);
        ps.setObject(29,"123421.00012",39,4);
        ps.addBatch();
        ps.executeBatch();
        //JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        //BasicTable bt = (BasicTable) rs.getResult();
        //System.out.println(bt.getString());
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
        org.junit.Assert.assertEquals(rs.getByte("col3"), 12);
        org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
        org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
        org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
        org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
        org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
        org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("col11"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col12"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col13"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1),rs.getObject("col14"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("col15"));
        org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
        org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
        org.junit.Assert.assertEquals("test1",rs.getString("col18"));
        org.junit.Assert.assertEquals("test1",rs.getString("col19"));
        org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
        org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
        org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
        org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
        org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
        org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
        org.junit.Assert.assertEquals("123421.00",rs.getObject("col27").toString());
        org.junit.Assert.assertEquals("123421.0001200",rs.getObject("col28").toString());
        org.junit.Assert.assertEquals("123421.0001200000000000000",rs.getObject("col29").toString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_wideTable() throws SQLException, IOException {
        createWideTable("dfs://test_append_wideTable",1000,1000);
        long start = System.nanoTime();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_wideTable','pt') values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        BasicTimestamp tmp_timestamp = new BasicTimestamp(LocalDateTime.of(2021, 1, 1, 1, 1, 1, 001));
        ps.setObject(1, tmp_timestamp);
        ps.setString(2, "test1");
        for(int i =3;i<=1002;i++) {
            ps.setInt(i, i);
        }
        for(int i =1003;i<=2002;i++) {
            ps.setDouble(i, (double) i);
        }
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_wideTable','pt')");
        while (rs.next()) {
            org.junit.Assert.assertEquals(LocalDateTime.of(2021, 1, 1, 1, 1, 1), rs.getObject(1));
            org.junit.Assert.assertEquals("test1", rs.getString(2));
            for(int i =3;i<=1002;i++) {
                org.junit.Assert.assertEquals(i, rs.getInt(i));
            }
            for(int i =1003;i<=2002;i++) {
                org.junit.Assert.assertEquals((double) i, rs.getDouble(i), 4);
            }
        }
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_fail() throws SQLException, IOException {
        DBConnection db = null;
        String script = "login(`admin, `123456); \n"+
                    "if(existsDatabase('dfs://test_append_type'))" +
                    "{ dropDatabase('dfs://test_append_type')} \n"+
                    "t = table(10:0,`id`dataType,[INT,DATETIME]) \n"+
                    "db=database('dfs://test_append_type', VALUE, 1 2 3,,'TSDB') \n"+
                    "db.createPartitionedTable(t, `pt, `id,,`id`dataType) \n";
            db = new DBConnection();
            db.connect(HOST, PORT);
            db.run(script);
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2, LocalDateTime.of(2021,1,1,1,1,1));
        ps.addBatch();
        ps.setNull(1,Types.INTEGER);
        ps.setObject(2, LocalDateTime.of(2021,1,1,1,1,1));
        ps.addBatch();
        ps.setInt(1,2);
        ps.setObject(2, LocalDateTime.of(2021,1,1,1,1,1));
        ps.addBatch();
        try{
            ps.executeBatch();
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        BasicTable re = (BasicTable)rs.getResult();
        System.out.println(re.rows());
        org.junit.Assert.assertEquals(0,re.rows());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_fail_1() throws SQLException, IOException {
        DBConnection db = null;
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_append_type'))" +
                "{ dropDatabase('dfs://test_append_type')} \n"+
                "t = table(10:0,`id`dataType,[INT,DATETIME]) \n"+
                "db=database('dfs://test_append_type', RANGE, 1 20 30,,'TSDB') \n"+
                "db.createPartitionedTable(t, `pt, `id,,`id`dataType) \n";
        db = new DBConnection();
        db.connect(HOST, PORT);
        db.run(script);
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2, LocalDateTime.of(2021,1,1,1,1,1));
        ps.addBatch();
        ps.setNull(1,Types.INTEGER);
        ps.setObject(2, LocalDateTime.of(2021,1,1,1,1,1));
        ps.addBatch();
        ps.setInt(1,2);
        ps.setObject(2, LocalDateTime.of(2021,1,1,1,1,1));
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_type','pt')");
        BasicTable re = (BasicTable)rs.getResult();
        System.out.println(re.rows());
        org.junit.Assert.assertEquals(2,re.rows());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_BOOL() throws SQLException, IOException {
        createPartitionTable_Array("BOOL");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new boolean[]{true,true,false});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.BOOLEAN);
        ps.addBatch();
        ps.setInt(1,3);
        ps.setObject(2,new Boolean[]{true,false});
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[true,true,false]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
        Assert.assertEquals("[true,false]",re1.getColumn(1).get(2).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_CHAR() throws SQLException, IOException {
        createPartitionTable_Array("CHAR");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new byte[]{'A','F'});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.CHAR);
        ps.addBatch();
        ps.setInt(1,3);
        ps.setObject(2,new Byte[]{'A','C'});
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("['A','F']",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
        Assert.assertEquals("['A','C']",re1.getColumn(1).get(2).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_SHORT() throws SQLException, IOException {
        createPartitionTable_Array("SHORT");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new short[]{(short)555,(short)-1,0});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.CHAR);
        ps.addBatch();
        ps.setInt(1,3);
        ps.setObject(2,new Short[]{(short)1555,(short)-111,0});
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[555,-1,0]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
        Assert.assertEquals("[1555,-111,0]",re1.getColumn(1).get(2).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_INT() throws SQLException, IOException {
        createPartitionTable_Array("INT");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new int[]{12121,-11111,0});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.CHAR);
        ps.addBatch();
        ps.setInt(1,3);
        ps.setObject(2,new Integer[]{-12121,0,11111});
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[12121,-11111,0]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
        Assert.assertEquals("[-12121,0,11111]",re1.getColumn(1).get(2).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_LONG() throws SQLException, IOException {
        createPartitionTable_Array("LONG");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new long[]{(long)1233,(long)-1233,(long)0});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.CHAR);
        ps.addBatch();
        ps.setInt(1,3);
        ps.setObject(2,new Long[]{(long)-133,(long)13003,(long)0});
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[1233,-1233,0]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
        Assert.assertEquals("[-133,13003,0]",re1.getColumn(1).get(2).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_DATE() throws SQLException, IOException {
        createPartitionTable_Array("DATE");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new java.util.Date[]{Date.valueOf(LocalDate.of(2008,1,12)),Date.valueOf(LocalDate.of(1969,1,1))});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.DATE);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[2008.01.12,1969.01.01]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_MONTH() throws SQLException, IOException {
        createPartitionTable_Array("MONTH");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new LocalDate[]{LocalDate.of(1969,1,10),LocalDate.of(2021,1,10)});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[1969.01M,2021.01M]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());

    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_TIME() throws SQLException, IOException {
        createPartitionTable_Array("TIME");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new Time[]{Time.valueOf(LocalTime.of(1,1,1)),Time.valueOf(LocalTime.of(23,59,59))});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.TIME);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[01:01:01.000,23:59:59.000]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_MINUTE() throws SQLException, IOException {
        createPartitionTable_Array("MINUTE");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new LocalTime[]{LocalTime.of(0,1),LocalTime.of(23,59)});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[00:01m,23:59m]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_SECOND() throws SQLException, IOException {
        createPartitionTable_Array("SECOND");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new LocalTime[]{LocalTime.of(1,1,1),LocalTime.of(23,59,59)});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[01:01:01,23:59:59]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_DATETIME() throws SQLException, IOException {
        createPartitionTable_Array("DATETIME");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new LocalDateTime[]{LocalDateTime.of(2038,1,1,1,1,1),LocalDateTime.of(1969,12,31,23,59,59)});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[2038.01.01T01:01:01,1969.12.31T23:59:59]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_TIMESTAMP() throws SQLException, IOException {
        createPartitionTable_Array("TIMESTAMP");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new LocalDateTime[]{LocalDateTime.of(2030,12,31,23,59,59,999999999),LocalDateTime.of(1969,1,1,1,1,1,001)});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[2030.12.31T23:59:59.999,1969.01.01T01:01:01.000]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_NANOTIME() throws SQLException, IOException {
        createPartitionTable_Array("NANOTIME");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new LocalDateTime[]{LocalDateTime.of(2030,12,31,23,59,59,999999999),LocalDateTime.of(1969,1,1,1,1,1,001)});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[23:59:59.999999999,01:01:01.000000001]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_NANOTIMESTAMP() throws SQLException, IOException {
        createPartitionTable_Array("NANOTIMESTAMP");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new LocalDateTime[]{LocalDateTime.of(2030,12,31,23,59,59,999999999),LocalDateTime.of(1969,1,1,1,1,1,001)});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[2030.12.31T23:59:59.999999999,1969.01.01T01:01:01.000000001]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_FLOAT() throws SQLException, IOException {
        createPartitionTable_Array("FLOAT");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new float[]{(float)11.11,(float)-343411.11,0});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.FLOAT);
        ps.addBatch();
        ps.setInt(1,3);
        ps.setObject(2,new Float[]{(float)11.11,(float)-34411.11, Float.valueOf(0)});
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[11.10999966,-343411.125,0]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
        Assert.assertEquals("[11.10999966,-34411.109375,0]",re1.getColumn(1).get(2).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_DOUBLE() throws SQLException, IOException {
        createPartitionTable_Array("DOUBLE");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new double[]{11.11,-343411.11,0});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.DOUBLE);
        ps.addBatch();
        ps.setInt(1,3);
        ps.setObject(2,new Double[]{11.11,-34341.11, Double.valueOf(0)});
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[11.11,-343411.11,0]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
        Assert.assertEquals("[11.11,-34341.11,0]",re1.getColumn(1).get(2).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_UUID() throws SQLException, IOException {
        createPartitionTable_Array("UUID");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
//        ps.setObject(2,new UUID[]{UUID.fromString("00000000-0000-0001-0000-000000000002")});
        ps.setObject(2,new String[]{"00000000-0000-0001-0000-000000000002"});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[00000000-0000-0001-0000-000000000002]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_DATEHOUR() throws SQLException, IOException {
        createPartitionTable_Array("DATEHOUR");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new LocalDateTime[]{LocalDateTime.of(1967,1,1,1,1,1,123456),LocalDateTime.of(2099,1,1,1,1,1,123456)});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[1967.01.01T01,2099.01.01T01]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_IPADDR() throws SQLException, IOException {
        createPartitionTable_Array("IPADDR");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new String[]{"0::1:0:0:0:2","11::222:0:0:0:109"});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[0::1:0:0:0:2,11::222:0:0:0:109]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[0.0.0.0]",re1.getColumn(1).get(1).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_INT128() throws SQLException, IOException {
        createPartitionTable_Array("INT128");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new String[]{"00000000000000010000000000000002","e1671797c52e15f763380b45e841ec32"});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[00000000000000010000000000000002,e1671797c52e15f763380b45e841ec32]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }
    @Ignore//NOT SUPPORT JAVAOS-147
    public void test_PreparedStatement_insert_into_DFS_arrayVector_COMPLEX() throws SQLException, IOException {
        createPartitionTable_Array("COMPLEX");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new String[]{"1.0+2.0i","11111.0+22222.0i"});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[1.0+2.0i,11111.0+22222.0i]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }
    @Ignore//NOT SUPPORT JAVAOS-147
    public void test_PreparedStatement_insert_into_DFS_arrayVector_POINT() throws SQLException, IOException {
        createPartitionTable_Array("POINT");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new String[]{"(0.0, 0.0)","(0.0, 0.0)"});
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("['(0.0, 0.0)','(0.0, 0.0)']",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_DECIMAL32() throws SQLException, IOException {
        createPartitionTable_Array("DECIMAL32(5)");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
//        ps.setObject(2,new BigDecimal[]{new BigDecimal(1.11),new BigDecimal(-1.11)},37,4);
        ps.setObject(2,new Double[] {0.0,-123.00432,132.204234,100.0},37,2);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[0.00000,-123.00432,132.20423,100.00000]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());

    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_DECIMAL64() throws SQLException, IOException {
        createPartitionTable_Array("DECIMAL64(5)");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new Double[] {0.0,-123.00432,132.204234,100.0},38,4);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[0.00000,-123.00432,132.20423,100.00000]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_DECIMAL128() throws SQLException, IOException {
        createPartitionTable_Array("DECIMAL128(5)");
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new String[] {"0.0","-123.00432","132.204234","100.0"},39,4);
        ps.addBatch();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.addBatch();
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re1= (BasicTable) rs.getResult();
        Assert.assertEquals("[0.00000,-123.00432,132.20423,100.00000]",re1.getColumn(1).get(0).getString());
        Assert.assertEquals("[]",re1.getColumn(1).get(1).getString());
    }

    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_not_support_1() throws SQLException {
        createPartitionTable_Array("INT");
        String re = null;
        try{
            PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(1,)");
        }catch(Exception e){
            re = e.getMessage();
        }
        org.junit.Assert.assertEquals("check the SQl insert into loadTable('dfs://test_append_array_tsdb1','pt') values(1,)", re);
    }
    @Test
    public void test_PreparedStatement_insert_into_DFS_arrayVector_not_support_2() throws SQLException {
        createPartitionTable_Array("INT");
        String re = null;
        try{
            PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,)");
        }catch(Exception e){
            re = e.getMessage();
        }
        org.junit.Assert.assertEquals("The number of table columns and the number of values do not match! Please check the SQL!", re);
    }
    @Test
    public void test_PreparedStatement_delete_dimension_execute() throws SQLException, IOException {
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                "colNames=\"col\"+string(1..29);\n" +
                "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                "t=table(1:0,colNames,colTypes);\n" +
                "insert into t values(2,true,'a',2h,2,22l,9999.12.06,9999.06M,23:59:59.999,23:59m,23:59:59,9999.12.31 23:59:59,9999.12.31 23:59:59.999,00:00:00.999999999,9999.06.13 13:30:10.008007006,2f,2.12345,\"\",\"\",uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f7\"),datehour(9999.06.13 13:30:10),ipaddr(\"192.168.1.253\"),int128(\"e1671797c52e15f763380b45e841ec32\"),blob(\"123\"),complex(111,1),point(1,2),decimal32(1.1,2),decimal64(1.1,7),decimal128(1.1,19));\n" +
                "insert into t values(1,,,,,,,,,,,,,,,,,,,,,,,,,,,,);\n" +
                "db=database('dfs://test_append_type_tsdb1', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n"+
                "pt=db.createTable(t, `pt,,`col1)\n" +
                "pt.append!(t)\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT);
        db.run(script);
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt') values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        for(int i =3;i<=10000;i++) {
            ps.setInt(1, i);
            ps.addBatch();
        }
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt = (BasicTable) rs.getResult();
        System.out.println(bt.rows());
        org.junit.Assert.assertEquals(10000,bt.rows());
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col1 = 1000");
        ps1.execute();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(9999,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col1 = ?");
        ps2.setInt(1,1);
        ps2.execute();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(9998,bt2.rows());
        PreparedStatement ps3 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col1 < ? and col1 > ?");
        ps3.setInt(1,1000);
        ps3.setInt(2,100);
        ps3.execute();
        JDBCResultSet rs3 = (JDBCResultSet)ps3.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt3 = (BasicTable) rs3.getResult();
        System.out.println(bt3.rows());
        org.junit.Assert.assertEquals(9099,bt3.rows());
        PreparedStatement ps4 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col2 = ? ");
        ps4.setNull(1,Types.BOOLEAN);
        ps4.execute();
        JDBCResultSet rs4 = (JDBCResultSet)ps4.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt4 = (BasicTable) rs4.getResult();
        System.out.println(bt4.rows());
        org.junit.Assert.assertEquals(1,bt4.rows());
    }
    @Test
    public void test_PreparedStatement_delete_dfs_execute() throws SQLException, IOException {
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                "colNames=\"col\"+string(1..29);\n" +
                "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                "t=table(1:0,colNames,colTypes);\n" +
                "insert into t values(2,true,'a',2h,2,22l,9999.12.06,9999.06M,23:59:59.999,23:59m,23:59:59,9999.12.31 23:59:59,9999.12.31 23:59:59.999,00:00:00.999999999,9999.06.13 13:30:10.008007006,2f,2.12345,\"\",\"\",uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f7\"),datehour(9999.06.13 13:30:10),ipaddr(\"192.168.1.253\"),int128(\"e1671797c52e15f763380b45e841ec32\"),blob(\"123\"),complex(111,1),point(1,2),decimal32(1.1,2),decimal64(1.1,7),decimal128(1.1,19));\n" +
                "insert into t values(1,,,,,,,,,,,,,,,,,,,,,,,,,,,,);\n" +
                "db=database('dfs://test_append_type_tsdb1', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n"+
                "pt=db.createPartitionedTable(t, `pt,`col1,,`col1)\n" +
                "pt.append!(t)\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT);
        db.run(script);
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt') values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        for(int i =3;i<=10000;i++) {
            ps.setInt(1, i);
            ps.addBatch();
        }
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt = (BasicTable) rs.getResult();
        System.out.println(bt.rows());
        org.junit.Assert.assertEquals(10000,bt.rows());
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col1 = 1000");
        ps1.execute();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(9999,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col1 = ?");
        ps2.setInt(1,1);
        ps2.execute();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(9998,bt2.rows());
        PreparedStatement ps3 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col1 < ? and col1 > ?");
        ps3.setInt(1,1000);
        ps3.setInt(2,100);
        ps3.execute();
        JDBCResultSet rs3 = (JDBCResultSet)ps3.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt3 = (BasicTable) rs3.getResult();
        System.out.println(bt3.rows());
        org.junit.Assert.assertEquals(9099,bt3.rows());
        PreparedStatement ps4 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col2 = ? ");
        ps4.setNull(1,Types.BOOLEAN);
        ps4.execute();
        JDBCResultSet rs4 = (JDBCResultSet)ps4.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt4 = (BasicTable) rs4.getResult();
        System.out.println(bt4.rows());
        org.junit.Assert.assertEquals(1,bt4.rows());
    }
    @Test
    public void test_PreparedStatement_delete_excuteBanch() throws SQLException, IOException {
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                "colNames=\"col\"+string(1..29);\n" +
                "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                "t=table(1:0,colNames,colTypes);\n" +
                "insert into t values(2,true,'a',2h,2,22l,9999.12.06,9999.06M,23:59:59.999,23:59m,23:59:59,9999.12.31 23:59:59,9999.12.31 23:59:59.999,00:00:00.999999999,9999.06.13 13:30:10.008007006,2f,2.12345,\"\",\"\",uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f7\"),datehour(9999.06.13 13:30:10),ipaddr(\"192.168.1.253\"),int128(\"e1671797c52e15f763380b45e841ec32\"),blob(\"123\"),complex(111,1),point(1,2),decimal32(1.1,2),decimal64(1.1,7),decimal128(1.1,19));\n" +
                "insert into t values(1,,,,,,,,,,,,,,,,,,,,,,,,,,,,);\n" +
                "db=database('dfs://test_append_type_tsdb1', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n"+
                "pt=db.createPartitionedTable(t, `pt,`col1,,`col1)\n" +
                "pt.append!(t)\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT);
        db.run(script);
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt') values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        for(int i =3;i<=10000;i++) {
            ps.setInt(1, i);
            ps.addBatch();
        }
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt = (BasicTable) rs.getResult();
        System.out.println(bt.rows());
        org.junit.Assert.assertEquals(10000,bt.rows());
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col1 = ?");
        ps1.setInt(1,1);
        ps1.addBatch();
        ps1.setInt(1,2);
        ps1.addBatch();
        ps1.setNull(1,Types.BOOLEAN);
        ps1.addBatch();
        ps1.executeBatch();

        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(9998,bt1.rows());
    }
    @Test
    public void test_PreparedStatement_delete_excuteBanch_not_addBatch() throws SQLException, IOException {
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                "colNames=\"col\"+string(1..29);\n" +
                "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                "t=table(1:0,colNames,colTypes);\n" +
                "insert into t values(2,true,'a',2h,2,22l,9999.12.06,9999.06M,23:59:59.999,23:59m,23:59:59,9999.12.31 23:59:59,9999.12.31 23:59:59.999,00:00:00.999999999,9999.06.13 13:30:10.008007006,2f,2.12345,\"\",\"\",uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f7\"),datehour(9999.06.13 13:30:10),ipaddr(\"192.168.1.253\"),int128(\"e1671797c52e15f763380b45e841ec32\"),blob(\"123\"),complex(111,1),point(1,2),decimal32(1.1,2),decimal64(1.1,7),decimal128(1.1,19));\n" +
                "insert into t values(1,,,,,,,,,,,,,,,,,,,,,,,,,,,,);\n" +
                "db=database('dfs://test_append_type_tsdb1', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n"+
                "pt=db.createPartitionedTable(t, `pt,`col1,,`col1)\n" +
                "pt.append!(t)\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT);
        db.run(script);
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt') values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        for(int i =3;i<=10000;i++) {
            ps.setInt(1, i);
            ps.addBatch();
        }
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt = (BasicTable) rs.getResult();
        System.out.println(bt.rows());
        org.junit.Assert.assertEquals(10000,bt.rows());
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col1 = ?");
//        ps1.setInt(1,1);
//        ps1.addBatch();
//        ps1.setInt(1,2);
//        ps1.addBatch();
//        ps1.setNull(1,Types.BOOLEAN);
//        ps1.addBatch();
        ps1.executeBatch();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(10000,bt1.rows());
    }

    @Test
    public void test_PreparedStatement_delete_executeUpdate() throws SQLException, IOException {
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                "colNames=\"col\"+string(1..29);\n" +
                "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                "t=table(1:0,colNames,colTypes);\n" +
                "insert into t values(2,true,'a',2h,2,22l,9999.12.06,9999.06M,23:59:59.999,23:59m,23:59:59,9999.12.31 23:59:59,9999.12.31 23:59:59.999,00:00:00.999999999,9999.06.13 13:30:10.008007006,2f,2.12345,\"\",\"\",uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f7\"),datehour(9999.06.13 13:30:10),ipaddr(\"192.168.1.253\"),int128(\"e1671797c52e15f763380b45e841ec32\"),blob(\"123\"),complex(111,1),point(1,2),decimal32(1.1,2),decimal64(1.1,7),decimal128(1.1,19));\n" +
                "insert into t values(1,,,,,,,,,,,,,,,,,,,,,,,,,,,,);\n" +
                "db=database('dfs://test_append_type_tsdb1', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n"+
                "pt=db.createPartitionedTable(t, `pt,`col1,,`col1)\n" +
                "pt.append!(t)\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT);
        db.run(script);
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt') values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        for(int i =3;i<=10000;i++) {
            ps.setInt(1, i);
            ps.addBatch();
        }
        ps.executeBatch();
        JDBCResultSet rs = (JDBCResultSet)ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt = (BasicTable) rs.getResult();
        System.out.println(bt.rows());
        org.junit.Assert.assertEquals(10000,bt.rows());
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col1 = 1000");
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(9999,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col1 = ?");
        ps2.setInt(1,1);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(9998,bt2.rows());
        PreparedStatement ps3 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col1 < ? and col1 > ?");
        ps3.setInt(1,1000);
        ps3.setInt(2,100);
        ps3.executeUpdate();
        JDBCResultSet rs3 = (JDBCResultSet)ps3.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt3 = (BasicTable) rs3.getResult();
        System.out.println(bt3.rows());
        org.junit.Assert.assertEquals(9099,bt3.rows());
        PreparedStatement ps4 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col2 = ? ");
        ps4.setNull(1,Types.BOOLEAN);
        ps4.executeUpdate();
        JDBCResultSet rs4 = (JDBCResultSet)ps4.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt4 = (BasicTable) rs4.getResult();
        System.out.println(bt4.rows());
        org.junit.Assert.assertEquals(1,bt4.rows());
    }
    @Test
    public void test_PreparedStatement_delete_keyword_case_mixing() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("dElete frOm loadTable('dfs://test_append_type_tsdb1','pt') WHEre col3 = ?");
        ps1.setByte(1, (byte) 'a');
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("\nDELETE\n FROM \n\n\n\nloadTable('dfs://test_append_type_tsdb1','pt')\nWHERE\n\ncol3 =\n ?");
        ps2.setNull(1,Types.CHAR);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_char() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col3 = ?");
        ps1.setByte(1, (byte) 'a');
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col3 = ?");
        ps2.setNull(1,Types.CHAR);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_short() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col4 = ?");
        ps1.setShort(1, (short)2);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col4 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_int() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col5 = ?");
        ps1.setInt(1,2);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col5 = ?");
        ps2.setNull(1,Types.INTEGER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_long() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col6 = ?");
        ps1.setLong(1,(long)22);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col5 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_date() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col7 = ?");
        ps1.setDate(1, Date.valueOf(LocalDate.of(9999,12,6)));
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col7 = ?");
        ps2.setNull(1,Types.DATE);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_month() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col8 = ?");
        BasicMonth tmp_month = new BasicMonth(YearMonth.of(9999,6));
        ps1.setObject(1, tmp_month);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col8 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_time() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col9 = ?");
        ps1.setObject(1, LocalTime.of(23,59,59,999000000));
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col9 = ?");
        ps2.setNull(1,Types.TIME);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_minute() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col10 = ?");
        BasicMinute tmp_minute = new BasicMinute(LocalTime.of(23,59));
        ps1.setObject(1, tmp_minute);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col10 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_second() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col11 = ?");
        BasicSecond tmp_second = new BasicSecond(LocalTime.of(23,59,59));
        ps1.setObject(1, tmp_second);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col11 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_datetime() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col12 = ?");
        BasicDateTime tmp_datetime = new BasicDateTime(LocalDateTime.of(9999,12,31,23,59,59));
        ps1.setObject(1, tmp_datetime);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col12 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_timestamp() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col13 = ?");
        BasicTimestamp tmp_timestamp = new BasicTimestamp(LocalDateTime.of(9999,12,31,23,59,59,999000000));
        ps1.setObject(1, tmp_timestamp);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col13 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_nanotime() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col14 = ?");
        BasicNanoTime tmp_nanotime = new BasicNanoTime(LocalTime.of(0,0,0,999999999));
        ps1.setObject(1, tmp_nanotime);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col14 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_nanotimestamp() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col15 = ?");
        BasicNanoTimestamp tmp_nanotimestamp = new BasicNanoTimestamp(LocalDateTime.of(9999,6,13,13,30,10,8007006));
        ps1.setObject(1, tmp_nanotimestamp);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col15 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_float() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col16 = ?");
        ps1.setFloat(1,(float) 2);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col16 = ?");
        ps2.setNull(1,Types.FLOAT);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_double() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col17 = ?");
        ps1.setDouble(1,(double)2.12345);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col17 = ?");
        ps2.setNull(1,Types.DOUBLE);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_symbol() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col18 = ?");
        ps1.setString(1,"1212");
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col18 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_string() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col19 = ?");
        ps1.setString(1,"1212");
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col19 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_uuid() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col20 = ?");
        BasicUuid tmp_uuid = new BasicUuid(1,2);
        ps1.setObject(1,tmp_uuid);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col20 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_datehour() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col21 = ?");
        BasicDateHour tmp_datehour = new BasicDateHour(LocalDateTime.of(2012,06,13,13,30,10));
        ps1.setObject(1, tmp_datehour);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col21 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_ipaddr() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col22 = ?");
        BasicIPAddr ipaddrs = new BasicIPAddr(1,2);
        ps1.setObject(1, ipaddrs);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col22 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_int128() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col23 = ?");
        BasicInt128 int128 = new BasicInt128(1,2);
        ps1.setObject(1, int128);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col23 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_blob() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col24 = ?");
        ps1.setObject(1, "123");
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col24 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_complex() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col25 = ?");
        BasicComplex complexs = new BasicComplex(111,1);
        ps1.setObject(1, complexs);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col25 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_point() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col26 = ?");
        BasicPoint points = new BasicPoint(1,2);
        ps1.setObject(1, points);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col26 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_decimal32() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col27 = ?");
        ps1.setObject(1,1.1,37,2);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col27 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_decimal64() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col28 = ?");
        ps1.setObject(1,1.1,38,7);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col28 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_delete_decimal128() throws SQLException, IOException {
        createPartitionTable_insert();
        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col29 = ?");
        ps1.setObject(1,1.1,39,19);
        ps1.executeUpdate();
        JDBCResultSet rs1 = (JDBCResultSet)ps1.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt1 = (BasicTable) rs1.getResult();
        System.out.println(bt1.rows());
        org.junit.Assert.assertEquals(1,bt1.rows());
        PreparedStatement ps2 = conn.prepareStatement("delete from loadTable('dfs://test_append_type_tsdb1','pt') where col29 = ?");
        ps2.setNull(1,Types.OTHER);
        ps2.executeUpdate();
        JDBCResultSet rs2 = (JDBCResultSet)ps2.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        BasicTable bt2 = (BasicTable) rs2.getResult();
        System.out.println(bt2.rows());
        org.junit.Assert.assertEquals(0,bt2.rows());
    }
    @Test
    public void test_PreparedStatement_update_executeBatch() throws SQLException, IOException {
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                "colNames=\"col\"+string(1..29);\n" +
                "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                "t=table(1:0,colNames,colTypes);\n" +
                "insert into t values(2,false,'a',2h,2,22l,9999.12.06,9999.06M,23:59:59.999,23:59m,23:59:59,9999.12.31 23:59:59,9999.12.31 23:59:59.999,00:00:00.999999999,9999.06.13 13:30:10.008007006,2f,2.12345,\"\",\"\",uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f7\"),datehour(9999.06.13 13:30:10),ipaddr(\"192.168.1.253\"),int128(\"e1671797c52e15f763380b45e841ec32\"),blob(\"123\"),complex(111,1),point(1,2),decimal32(1.1,2),decimal64(1.1,7),decimal128(1.1,19));\n" +
                "insert into t values(1,,,,,,,,,,,,,,,,,,,,,,,,,,,,);\n" +
                "db=database('dfs://test_append_type_tsdb1', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n"+
                "pt=db.createTable(t, `pt,,`col1)\n" +
                "pt.append!(t)\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT);
        db.run(script);
        stm.execute("pt=loadTable('dfs://test_append_type_tsdb1','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        for(int i =3;i<=10000;i++) {
            ps.setInt(1, i);
            ps.addBatch();
        }
        ps.executeBatch();
        stm.execute("pt=loadTable('dfs://test_append_type_tsdb1','pt')");
        PreparedStatement ps1 = conn.prepareStatement("update pt set col2=? ");
        ps1.setBoolean(1,true);
        ps1.addBatch();
        ps1.executeBatch();
        ResultSet rs1 = ps1.executeQuery("select * from pt");
        while(rs1.next()){
            org.junit.Assert.assertEquals(rs1.getObject("col2"), true);
        }
    }
    @Test
    public void test_PreparedStatement_update_not_addbatch() throws SQLException, IOException {
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                "colNames=\"col\"+string(1..29);\n" +
                "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                "t=table(1:0,colNames,colTypes);\n" +
                "insert into t values(2,false,'a',2h,2,22l,9999.12.06,9999.06M,23:59:59.999,23:59m,23:59:59,9999.12.31 23:59:59,9999.12.31 23:59:59.999,00:00:00.999999999,9999.06.13 13:30:10.008007006,2f,2.12345,\"\",\"\",uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f7\"),datehour(9999.06.13 13:30:10),ipaddr(\"192.168.1.253\"),int128(\"e1671797c52e15f763380b45e841ec32\"),blob(\"123\"),complex(111,1),point(1,2),decimal32(1.1,2),decimal64(1.1,7),decimal128(1.1,19));\n" +
                "insert into t values(1,,,,,,,,,,,,,,,,,,,,,,,,,,,,);\n" +
                "db=database('dfs://test_append_type_tsdb1', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n"+
                "pt=db.createTable(t, `pt,,`col1)\n" +
                "pt.append!(t)\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT);
        db.run(script);
        stm.execute("pt=loadTable('dfs://test_append_type_tsdb1','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        for(int i =3;i<=10000;i++) {
            ps.setInt(1, i);
            ps.addBatch();
        }
        ps.executeBatch();
        stm.execute("pt=loadTable('dfs://test_append_type_tsdb1','pt')");
        PreparedStatement ps1 = conn.prepareStatement("update pt set col2=? ");
//        ps1.setBoolean(1,true);
//        ps1.addBatch();
        ps1.executeBatch();
        ResultSet rs1 = ps1.executeQuery("select * from pt order by col1 ");
        rs1.next();
        org.junit.Assert.assertEquals(rs1.getObject("col2"), null);
    }
    @Test
    public void test_PreparedStatement_update_executeUpdate() throws SQLException, IOException {
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_append_type_tsdb1'))" +
                "{ dropDatabase('dfs://test_append_type_tsdb1')} \n"+
                "colNames=\"col\"+string(1..29);\n" +
                "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                "t=table(1:0,colNames,colTypes);\n" +
                "insert into t values(2,false,'a',2h,2,22l,9999.12.06,9999.06M,23:59:59.999,23:59m,23:59:59,9999.12.31 23:59:59,9999.12.31 23:59:59.999,00:00:00.999999999,9999.06.13 13:30:10.008007006,2f,2.12345,\"\",\"\",uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f7\"),datehour(9999.06.13 13:30:10),ipaddr(\"192.168.1.253\"),int128(\"e1671797c52e15f763380b45e841ec32\"),blob(\"123\"),complex(111,1),point(1,2),decimal32(1.1,2),decimal64(1.1,7),decimal128(1.1,19));\n" +
                "insert into t values(1,,,,,,,,,,,,,,,,,,,,,,,,,,,,);\n" +
                "db=database('dfs://test_append_type_tsdb1', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n"+
                "pt=db.createTable(t, `pt,,`col1)\n" +
                "pt.append!(t)\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT);
        db.run(script);
        stm.execute("pt=loadTable('dfs://test_append_type_tsdb1','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        for(int i =3;i<=10000;i++) {
            ps.setInt(1, i);
            ps.addBatch();
        }
        ps.executeBatch();
        stm.execute("pt=loadTable('dfs://test_append_type_tsdb1','pt')");
        PreparedStatement ps1 = conn.prepareStatement("update pt set col2=? ");
        ps1.setBoolean(1,true);
        ps1.addBatch();
        ps1.executeUpdate();
        ResultSet rs1 = ps1.executeQuery("select * from pt");
        while(rs1.next()){
            org.junit.Assert.assertEquals(rs1.getObject("col2"), true);
        }
    }
    @Test
    public void TestNull() throws Exception {
        System.out.println("Decimal64:");
        Scalar scalar64 = (Scalar) TypeCast.nullScalar(Entity.DATA_TYPE.DT_DECIMAL64);
        org.junit.Assert.assertEquals(true,scalar64.isNull());
        System.out.println(scalar64.isNull());
        BasicDecimal64Vector basicDecimal64Vector = new BasicDecimal64Vector(0,0);
        basicDecimal64Vector.Append(scalar64);
        System.out.println(((Scalar)(basicDecimal64Vector.get(0))).isNull());
        org.junit.Assert.assertEquals(true,((Scalar)(basicDecimal64Vector.get(0))).isNull());

        System.out.println("Decimal32:");
        Scalar scalar32 = (Scalar) TypeCast.nullScalar(Entity.DATA_TYPE.DT_DECIMAL32);
        System.out.println(scalar32.isNull());
        org.junit.Assert.assertEquals(true,scalar32.isNull());

        BasicDecimal32Vector basicDecimal32Vector = new BasicDecimal32Vector(0,0);
        basicDecimal32Vector.Append(scalar32);
        System.out.println(((Scalar)(basicDecimal32Vector.get(0))).isNull());
        org.junit.Assert.assertEquals(true,((Scalar)(basicDecimal32Vector.get(0))).isNull());

        System.out.println("Decimal128:");
        Scalar scalar128 = (Scalar) TypeCast.nullScalar(Entity.DATA_TYPE.DT_DECIMAL128);
        System.out.println(scalar128.isNull());
        org.junit.Assert.assertEquals(true,scalar128.isNull());

        BasicDecimal128Vector basicDecimal128Vector = new BasicDecimal128Vector(0,0);
        basicDecimal128Vector.Append(scalar128);
        System.out.println(((Scalar)(basicDecimal128Vector.get(0))).isNull());
        org.junit.Assert.assertEquals(true,((Scalar)(basicDecimal128Vector.get(0))).isNull());

    }
    @After
    public void Destroy(){
        LOGININFO = null;
    }
}
