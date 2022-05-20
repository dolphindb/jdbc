import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.ResourceBundle;
public class JDBCTestUtil {
    static ResourceBundle bundle = ResourceBundle.getBundle("setup/settings");//传入文件名,不需要后缀
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
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
