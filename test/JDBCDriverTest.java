import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class JDBCDriverTest {
    static String HOST = JDBCTestUtil.HOST;
    static int PORT = JDBCTestUtil.PORT;
    static String DATABASE = JDBCTestUtil.WORK_DIR+"/JDBC_driver_test_db2";
    Properties LOGININFO = new Properties();

    @Before
    public void SetUp(){
        LOGININFO.put("user", "admin");
        LOGININFO.put("password", "123456");
    }
//
//    @Test
//    public void TestConnect1() throws SQLException {
//        TestConnect("jdbc:dolphindb://"+HOST+":"+PORT+"?databasePath="+DATABASE);
//    }
//    @Test
//    public  void TestConnect2() throws SQLException {
//        TestConnect("jdbc:dolphindb://"+ HOST + ":" + PORT);
//    }
//    @Test
//    public  void TestConnect3() throws SQLException {
//        TestConnect("jdbc:dolphindb://databasePath=" + DATABASE);
//    }
//    @Test
//    public  void TestConnect4() throws SQLException {
//        TestConnect("jdbc:dolphindb://");
//    }

    private void TestConnect(String connstr) throws SQLException {

        Connection conn = null;
        try {
            Class.forName("com.dolphindb.jdbc.Driver");
            conn = DriverManager.getConnection(connstr, LOGININFO);
            DatabaseMetaData meta = conn.getMetaData();
            System.out.println(connstr);
            System.out.println("meta.getDatabaseProductName()");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    @After
    public void Destroy(){
        LOGININFO = null;
    }
}
