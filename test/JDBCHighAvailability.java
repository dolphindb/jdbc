import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Currency;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class JDBCHighAvailability {
    public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
        Class.forName("com.dolphindb.jdbc.Driver");
        String url = "jdbc:dolphindb://192.168.56.10:9002";
        Properties prop = new Properties();
        prop.setProperty("user", "admin");
        prop.setProperty("password", "123456");
        prop.setProperty("initialScript","");
        prop.setProperty("highAvailability", "true");
        //prop.setProperty("highAvailabilitySites","192.168.56.10:9002 192.168.56.11:9002");
        ConcurrentLinkedQueue<Connection> connections = new ConcurrentLinkedQueue<>();
        CountDownLatch countDownLatch = new CountDownLatch(200);
        for (int i = 0; i < 500; i++) {
            Thread t = new Thread(()->{
                try {
                    Connection connection = DriverManager.getConnection(url, prop);
                    int holdability = connection.getHoldability();
                    Thread.sleep(10000);
                    connections.add(connection);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            t.join();
            t.start();

        }

    }
}
