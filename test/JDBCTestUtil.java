import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JDBCTestUtil {
    static String HOST = "192.168.1.12" ;
    static int PORT = 8848 ;
    static String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
    static Properties LOGININFO = new Properties();

    public static Connection getConnection(Properties info){
        Connection conn = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection("jdbc:dolphindb://" + HOST +":" + PORT, info);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;

    }
}
