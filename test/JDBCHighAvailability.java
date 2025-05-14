import org.junit.Assert;

import java.sql.*;
import java.util.Currency;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class JDBCHighAvailability {
//    public static void main(String[] args) throws ClassNotFoundException, InterruptedException {
//        Class.forName("com.dolphindb.jdbc.Driver");
//        String url = "jdbc:dolphindb://192.168.0.69:18921";
//        Properties prop = new Properties();
//        prop.setProperty("user", "admin");
//        prop.setProperty("password", "123456");
//        prop.setProperty("initialScript","test1 = loadTable(\"dfs://test_chinese_table\",\"pt\")");
//        prop.setProperty("highAvailability", "true");
//        prop.setProperty("highAvailabilitySites","192.168.0.69:18921 192.168.0.69:18922");
//        ConcurrentLinkedQueue<Connection> connections = new ConcurrentLinkedQueue<>();
//        CountDownLatch countDownLatch = new CountDownLatch(200);
//        for (int i = 0; i < 500; i++) {
//            Thread t = new Thread(()->{
//                try {
//                    Connection connection = DriverManager.getConnection(url, prop);
//                    int holdability = connection.getHoldability();
//                    Thread.sleep(10000);
//                    connections.add(connection);
//                } catch (SQLException e) {
//                    throw new RuntimeException(e);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            });
//            t.join();
//            t.start();
//
//        }
//
//    }


    public static void main(String[] args) throws ClassNotFoundException, InterruptedException, SQLException {
        Class.forName("com.dolphindb.jdbc.Driver");
        String url = "jdbc:dolphindb://192.168.0.69:18921";
        Properties prop = new Properties();
        prop.setProperty("user", "admin");
        prop.setProperty("password", "123456");
        prop.setProperty("initialScript","test123 = loadTable(\"dfs://test_chinese_table\",\"pt\")");
        prop.setProperty("highAvailability", "true");
        prop.setProperty("highAvailabilitySites","192.168.0.69:18922");
        Connection conn = DriverManager.getConnection(url, prop);
        String url1 = "jdbc:dolphindb://192.168.0.69:28920?user=admin&password=123456";
        Connection conn1 = null;
        conn1 = DriverManager.getConnection(url1);
        Statement stmt = null;
        stmt = conn1.createStatement();
        try{
            stmt.execute("stopDataNode([\"192.168.0.69:18921\",\"192.168.0.69:18922\"])");
        }catch(Exception ex)
        {
            System.out.println(ex.getMessage());
        }
        stmt.execute(" sleep(10000)");
        try{
            stmt.execute("startDataNode([\"192.168.0.69:18921\",\"192.168.0.69:18922\"])");
        }catch(Exception ex)
        {
            System.out.println(ex.getMessage());
        }
        stmt.execute(" sleep(15000)");
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select  top  10 * from test123");
        ResultSet resultSet = stm.executeQuery("select top  10 * from loadTable(\"dfs://test_chinese_table\",\"pt\")");
        Assert.assertTrue(resultSet.next());
        Assert.assertTrue(rs.next());
        while(rs.next() && resultSet.next()){
            System.out.println(rs.getString(1)+" "+rs.getString(2));
            Assert.assertEquals(resultSet.getString(1),rs.getString(1));
            Assert.assertEquals(resultSet.getString(2),rs.getString(2));
        }
    }
}
