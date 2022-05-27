import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.ResourceBundle;
public class JDBCTestUtil {
    static ResourceBundle bundle = ResourceBundle.getBundle("setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    static String WORK_DIR = bundle.getString("WORK_DIR");
    static String DATA_DIR = bundle.getString("DATA_DIR");
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
