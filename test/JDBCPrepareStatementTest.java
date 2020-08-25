import com.sun.xml.internal.bind.v2.model.core.ID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.text.MessageFormat;
import java.util.Properties;
//import com.xxdb.DBConnection;

public class JDBCPrepareStatementTest {
    String HOST;
    int PORT;
    Properties LOGININFO = new Properties();
    String JDBC_DRIVER;
    String DB_URL ;
    @Before
    public void SetUp(){
        HOST = "localhost" ;
        PORT = 8848 ;
        JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        LOGININFO = new Properties();
        LOGININFO.put("user", "admin");
        LOGININFO.put("password", "123456");
        DB_URL = "jdbc:dolphindb://192.168.1.14:8941";
    }

    @Test
    public  void TestPrepareStatement() throws ClassNotFoundException,SQLException{
        Properties info = new Properties();
        info.put("user", "admin");
        info.put("password", "123456");
        Connection conn = null;
        String preSql="select count(*) from loadTable('dfs://rangedb','pt') group by ID ";
        PreparedStatement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL, info);
        stmt = conn.prepareStatement(preSql);
        stmt.executeQuery();
    }

    @After
    public void Destroy(){
        LOGININFO = null;
    }
}
