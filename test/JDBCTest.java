import com.dolphindb.jdbc.JDBCConnection;
import com.dolphindb.jdbc.JDBCResultSet;
import com.dolphindb.jdbc.JDBCStatement;
import com.xxdb.DBConnection;
import com.xxdb.data.BasicBoolean;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;
import java.util.ResourceBundle;

import static org.junit.Assert.assertEquals;

public class JDBCTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";

    static Connection conn = null;
    public static boolean createMemoryTable(String dataType){
        boolean success = false;
        DBConnection db = null;
        try{
            String script = "login(`admin, `123456); \n"+
                    "try{undef(`tt,SHARED)}catch(ex){};\n" +
                    "share table(10:0,`id`dataType,[INT,"+dataType+"])  as tt;\n";
            db = new DBConnection();
            db.connect(HOST, PORT,"admin","123456");
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
    public static boolean createTable() {
        DBConnection db = null;
        try {
            StringBuilder sb = new StringBuilder();
            sb = new StringBuilder();
            sb.append("bool = [1b, 0b];\n");
            sb.append("char = [97c, 'A'];\n");
            sb.append("short = [122h, 123h];\n");
            sb.append("int = [21, 22];\n");
            sb.append("long = [22l, 23l];\n");
            sb.append("float  = [2.1f, 2.2f];\n");
            sb.append("double = [2.1, 2.2];\n");
            sb.append("string= [`Hello, `world];\n");
            sb.append("symbol= [`symbol1, `symbol2];\n");
            sb.append("date = [2013.06.13, 2013.06.14];\n");
            sb.append("month = [2016.06M, 2016.07M];\n");
            sb.append("time = [13:30:10.008, 13:30:10.009];\n");
            sb.append("minute = [13:30m, 13:31m];\n");
            sb.append("second = [13:30:10, 13:30:11];\n");
            sb.append("datetime = [2012.06.13 13:30:10, 2012.06.13 13:30:10];\n");
            sb.append("timestamp = [2012.06.13 13:30:10.008, 2012.06.13 13:30:10.009];\n");
            sb.append("nanotime = [13:30:10.008007006, 13:30:10.008007007];\n");
            sb.append("nanotimestamp = [2012.06.13 13:30:10.008007006, 2012.06.13 13:30:10.008007007];\n");
            sb.append("datehour = [datehour(10),datehour(20)];\n");
            sb.append("uuid =  rand(uuid(),2);\n");
            sb.append("ipaddr =  rand(ipaddr(),2);\n");
            sb.append("int128 =  rand(int128(),2);\n");
            sb.append("blob= [`Hello, `world];\n");
            sb.append("complex= [complex(1,2),complex(2,3)];\n");
            sb.append("point= [point(1,2),point(2,3)];\n");
            sb.append("decimal32= decimal32([213.432,1.12],4);\n");
            sb.append("decimal64= decimal64([13.43241,231.54323],8);\n");
            sb.append("decimal128= decimal128([13.43241,231.54323],16);\n");
            sb.append("TT1= table(bool,char,short,int,long,float,double,string,symbol,date,month,time,minute,second,datetime,timestamp,nanotime,nanotimestamp,datehour,uuid,ipaddr,int128,blob,complex,point,decimal32,decimal64,decimal128);\n");
            sb.append("share TT1 as trade;");
            db = new DBConnection();
            db.connect(HOST,PORT);
            db.run(sb.toString());
            return false;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            if (db != null)
                db.close();
        }
    }
    @Test
    public void Test_getConnection_url() throws SQLException, ClassNotFoundException {
        String url = "jdbc:dolphindb://"+ HOST+":"+PORT+"?user=admin&password=123456";
        Connection conn = null;
        conn = DriverManager.getConnection(url);
        JDBCStatement stmt = (JDBCStatement) conn.createStatement();
        JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("bool(true)");
        BasicBoolean re =(BasicBoolean)rs1.getResult();
        System.out.println(re.getString());
        assertEquals("true",re.getString());
    }

    @Test
    public void Test_getConnection_url_prop() throws SQLException, ClassNotFoundException {
        Properties info = new Properties();
        info.put("hostName", HOST);
        info.put("port", PORT);
        info.put("user", "ad11min");
        info.put("password", "12341156");
        info.put("highAvailability", "false");
        String url = "jdbc:dolphindb://"+ HOST+":"+PORT+"?user=admin&password=123456";
        Connection conn = null;
        conn = DriverManager.getConnection(url,info);
        JDBCStatement stmt = (JDBCStatement) conn.createStatement();
        JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("bool(true)");
        BasicBoolean re =(BasicBoolean)rs1.getResult();
        System.out.println(re.getString());
        assertEquals("true",re.getString());
    }
    @Test
    public void Test_JDBConnection_url_prop() throws SQLException, ClassNotFoundException {
        Properties info = new Properties();
        info.put("hostName", HOST);
        info.put("port", PORT);
        info.put("user", "ad11min");
        info.put("password", "12341156");
        info.put("highAvailability", "false");
        String url = "jdbc:dolphindb://"+ HOST+":"+PORT+"?user=admin&password=123456";
        Connection conn = null;
        conn = new JDBCConnection(url, info);
        JDBCStatement stmt = (JDBCStatement) conn.createStatement();
        JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("bool(true)");
        BasicBoolean re =(BasicBoolean)rs1.getResult();
        System.out.println(re.getString());
        assertEquals("true",re.getString());
    }

    @Test
    public void test_JDBCStatement_execute_insert_into_executeQuery() throws SQLException, ClassNotFoundException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        createMemoryTable("BOOL");
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        JDBCStatement stmt = (JDBCStatement)conn.createStatement();
        stmt.execute("insert into tt values(1,true)");
        stmt.execute("insert into tt values(2,NULL)");
        ResultSet rs = stmt.executeQuery("select * from tt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("dataType"), true);
        rs.next();
        rs.getBoolean("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_JDBCStatement_execute_update_executeQuery() throws SQLException, ClassNotFoundException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        createTable();
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        JDBCStatement stmt = (JDBCStatement)conn.createStatement();
        stmt.execute("update trade set float = 1.111");
        ResultSet rs = stmt.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals(1.111,rs.getFloat(6),3);
        rs.next();
        org.junit.Assert.assertEquals(1.111,rs.getFloat(6),3);
    }

    @Test
    public void test_JDBCStatement_execute_delete_executeQuery() throws SQLException, ClassNotFoundException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        createMemoryTable("BOOL");
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        JDBCStatement stmt = (JDBCStatement)conn.createStatement();
        stmt.execute("insert into tt values(1,true)");
        stmt.execute("insert into tt values(2,NULL)");
        ResultSet rs = stmt.executeQuery("select * from tt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("dataType"), true);
        rs.next();
        rs.getBoolean("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
        stmt.execute("delete from  tt");
        ResultSet rs1 = stmt.executeQuery("select * from tt");
        org.junit.Assert.assertFalse(rs1.next());
    }

    @Test
    public void test_JDBCStatement_executeUpdate_insert_into_executeQuery() throws SQLException, ClassNotFoundException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        createMemoryTable("BOOL");
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        JDBCStatement stmt = (JDBCStatement)conn.createStatement();
        stmt.executeUpdate("insert into tt values(1,true)");
        stmt.executeUpdate("insert into tt values(2,NULL)");
        ResultSet rs = stmt.executeQuery("select * from tt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("dataType"), true);
        rs.next();
        rs.getBoolean("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void test_JDBCStatement_executeUpdate_update_executeQuery() throws SQLException, ClassNotFoundException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        createTable();
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        JDBCStatement stmt = (JDBCStatement)conn.createStatement();
        stmt.executeUpdate("update trade set float = 1.111");
        ResultSet rs = stmt.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals(1.111,rs.getFloat(6),3);
        rs.next();
        org.junit.Assert.assertEquals(1.111,rs.getFloat(6),3);
    }

    @Test
    public void test_JDBCStatement_executeUpdate_delete_executeQuery() throws SQLException, ClassNotFoundException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        createMemoryTable("BOOL");
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        JDBCStatement stmt = (JDBCStatement)conn.createStatement();
        stmt.executeUpdate("insert into tt values(1,true)");
        stmt.executeUpdate("insert into tt values(2,NULL)");
        ResultSet rs = stmt.executeQuery("select * from tt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("dataType"), true);
        rs.next();
        rs.getBoolean("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
        stmt.execute("delete from  tt");
        ResultSet rs1 = stmt.executeQuery("select * from tt");
        org.junit.Assert.assertFalse(rs1.next());
    }

    @Test
    public void test_JDBCPrepareStatement_execute_insert_into_executeQuery() throws SQLException, ClassNotFoundException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        createMemoryTable("BOOL");
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        PreparedStatement s = conn.prepareStatement("insert into tt values(?,?)");
        s.setObject(1,1);
        s.setObject(2,true);
        s.execute();
        ResultSet rs = s.executeQuery("select * from tt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("dataType"), true);
        org.junit.Assert.assertFalse(rs.next());
    }
    @Test
    public void test_JDBCPrepareStatement_execute_update_executeQuery() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set float = ?");
        s.setObject(1,1.111);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals(1.111,rs.getFloat(6),3);
        rs.next();
        org.junit.Assert.assertEquals(1.111,rs.getFloat(6),3);
    }

    @Test
    public void test_JDBCPrepareStatement_execute_delete_executeQuery() throws SQLException, ClassNotFoundException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        createMemoryTable("BOOL");
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        PreparedStatement s = conn.prepareStatement("insert into tt values(?,?)");
        s.setObject(1,1);
        s.setObject(2,true);
        s.execute();
        ResultSet rs = s.executeQuery("select * from tt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("dataType"), true);
        org.junit.Assert.assertFalse(rs.next());
        PreparedStatement s1 = conn.prepareStatement("delete from tt where id = ?");
        s1.setObject(1,1);
        s1.execute();
        ResultSet rs2 = s1.executeQuery("select * from tt");
        org.junit.Assert.assertFalse(rs2.next());
    }
    @Test
    public void test_JDBCPrepareStatement_executeUpdate_insert_into_executeQuery() throws SQLException, ClassNotFoundException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        createMemoryTable("BOOL");
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        PreparedStatement s = conn.prepareStatement("insert into tt values(?,?)");
        s.setObject(1,1);
        s.setObject(2,true);
        s.executeUpdate();
        ResultSet rs = s.executeQuery("select * from tt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("dataType"), true);
        org.junit.Assert.assertFalse(rs.next());
    }
    @Test
    public void test_JDBCPrepareStatement_executeUpdate_update_executeQuery() throws SQLException, ClassNotFoundException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        createTable();
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        PreparedStatement s = conn.prepareStatement("update trade set float = ?");
        s.setObject(1,1.111);
        s.executeUpdate();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals(1.111,rs.getFloat(6),3);
        rs.next();
        org.junit.Assert.assertEquals(1.111,rs.getFloat(6),3);
    }

    @Test
    public void test_JDBCPrepareStatement_executeUpdate_delete_executeQuery() throws SQLException, ClassNotFoundException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        createMemoryTable("BOOL");
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        PreparedStatement s = conn.prepareStatement("insert into tt values(?,?)");
        s.setObject(1,1);
        s.setObject(2,true);
        s.executeUpdate();
        ResultSet rs = s.executeQuery("select * from tt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("dataType"), true);
        org.junit.Assert.assertFalse(rs.next());
        PreparedStatement s1 = conn.prepareStatement("delete from tt where id = ?");
        s1.setObject(1,1);
        s1.executeUpdate();
        ResultSet rs2 = s1.executeQuery("select * from tt");
        org.junit.Assert.assertFalse(rs2.next());
    }

    @Test
    public void test_JDBCPrepareStatement_executeBatch_insert_into_executeQuery() throws SQLException, ClassNotFoundException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        createMemoryTable("BOOL");
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        PreparedStatement s = conn.prepareStatement("insert into tt values(?,?)");
        s.setObject(1,1);
        s.setObject(2,true);
        s.addBatch();
        s.executeBatch();
        ResultSet rs = s.executeQuery("select * from tt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("dataType"), true);
        org.junit.Assert.assertFalse(rs.next());
    }
    @Test
    public void test_JDBCPrepareStatement_executeBatch_update_executeQuery() throws SQLException, ClassNotFoundException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        createTable();
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        PreparedStatement s = conn.prepareStatement("update trade set float = ?");
        s.setObject(1,1.111);
        s.addBatch();
        s.executeBatch();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals(1.111,rs.getFloat(6),3);
        rs.next();
        org.junit.Assert.assertEquals(1.111,rs.getFloat(6),3);
    }

    @Test
    public void test_JDBCPrepareStatement_executeBatch_delete_executeQuery() throws SQLException, ClassNotFoundException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        createMemoryTable("BOOL");
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        PreparedStatement s = conn.prepareStatement("insert into tt values(?,?)");
        s.setObject(1,1);
        s.setObject(2,true);
        s.addBatch();
        s.executeBatch();
        ResultSet rs = s.executeQuery("select * from tt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("dataType"), true);
        org.junit.Assert.assertFalse(rs.next());
        PreparedStatement s1 = conn.prepareStatement("delete from tt where id = ?");
        s1.setObject(1,1);
        s1.addBatch();
        s1.executeBatch();
        ResultSet rs2 = s1.executeQuery("select * from tt");
        org.junit.Assert.assertFalse(rs2.next());
    }
}
