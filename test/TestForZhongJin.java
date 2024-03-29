//import com.alibaba.fastjson.JSON;
//import com.dolphindb.jdbc.JDBCConnection;
//import com.xxdb.DBConnection;
//import com.xxdb.data.Entity;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.sql.*;
//import java.util.Objects;
//import java.util.Properties;
//
//public class TestForZhongJin {
//
//    static String HOST = "192.168.0.52" ;
//    static int PORT = 8848 ;
//
//    Properties prop = new Properties();;
//
//    @Test
//    public void testSetSingleTableName() throws SQLException, IOException, ClassNotFoundException {
//
//        /**
//         * url：设置示例：
//         * url: jdbc:dolphindb://hostName:port?databasePath=
//         * or   jdbc:dolphindb://databasePath=
//         * jdbc:dolphindb://localhost:8848?databasePath=dfs://valuedb&partitionType=VALUE&partitionScheme=2000.01M..2019.05M
//         */
//        String exampleUrl = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&databasePath=dfs://db_testDriverManager";
//        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&databasePath=dfs://test";
//        String testUrl = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&databasePath=dfs://test&tablename=tb1";
//
//        prop.setProperty("hostName", "192.168.0.52");
//        prop.setProperty("port", "8848");
//        Class.forName("com.dolphindb.jdbc.Driver");
//
//
//        // JDBCConnection jdbcConnection = new JDBCConnection(url, prop);
//        // 获取jdbc链接
//        Connection connection = DriverManager.getConnection(testUrl);
//    }
//
//    @Test
//    public void testSetMultiTableName() throws SQLException, IOException, ClassNotFoundException {
//
//        String[] tablenames = {"tb1", "tb2"};
//        String tablename = String.join(",", tablenames);
//        StringBuilder stringBuilder = new StringBuilder();
//        String testUrl = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&databasePath=dfs://test";
//        stringBuilder.append(testUrl);
//        stringBuilder.append("&tablename=");
//        stringBuilder.append(tablename);
//        String url = stringBuilder.toString();
//
//        System.out.println(url);
//
//        Class.forName("com.dolphindb.jdbc.Driver");
//
//        // JDBCConnection jdbcConnection = new JDBCConnection(url, prop);
//        // 获取jdbc链接
//        Connection connection = DriverManager.getConnection(url);
//    }
//
//    @Test
//    public void testSetMultiTableName2() throws SQLException, IOException, ClassNotFoundException {
//
//        // String[] tablenames = {"pt1", "pt2"};
//        String[] tablenames = {"pt1"};
//        String tablename = String.join(",", tablenames);
//        StringBuilder stringBuilder = new StringBuilder();
//        String testUrl = "jdbc:dolphindb://"+"192.168.100.43"+":"+8903+"?user=admin&password=123456&databasePath=dfs://TEST";
//        stringBuilder.append(testUrl);
//        stringBuilder.append("&tablename=");
//        stringBuilder.append(tablename);
//        String url = stringBuilder.toString();
//
//        System.out.println(url);
//
//        Class.forName("com.dolphindb.jdbc.Driver");
//
//        // JDBCConnection jdbcConnection = new JDBCConnection(url, prop);
//        // 获取jdbc链接
//        Connection connection = DriverManager.getConnection(url);
//        DatabaseMetaData metaData = connection.getMetaData();
//    }
//
//    @Test
//    public void testStringJoint() {
//        String[] hosts = {"host1", "host2", "host3"};
//        String database = "mydb";
//        String username = "myuser";
//        String password = "mypassword";
//
//        // 将 hosts 数组转换为以逗号分隔的字符串
//        String hostsStr = String.join(",", hosts);
//
//        // 拼接 JDBC URL
//        String jdbcUrl = "jdbc:mysql://" + hostsStr + "/" + database + "?user=" + username + "&password=" + password;
//        System.out.println(jdbcUrl);
//
//        // 使用 JDBC 驱动程序连接数据库
//        // Connection connection = DriverManager.getConnection(jdbcUrl);
//    }
//
//
//    @Test
//    public void testStringJoint2() {
//        String[] tableNames = {"tb1", "tb2"};
//        String tableName = String.join(",", tableNames);
//        String testUrl = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&databasePath=dfs://test" + "&tablename" + tableName;
//        System.out.println(testUrl);
//    }
//
//    @Test
//    public void testStringSplit() {
//        String str = "tb1,tb2,tb3";
//        String[] strings = str.split(",");
//        System.out.println(strings.length);
//    }
//
//    /**
//     * 测试同学提供的case
//     */
//    public boolean CreateConnection1(String connstr) throws ClassNotFoundException, SQLException {
//        Boolean trigger = false;
//        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
//        Class.forName(JDBC_DRIVER);
//        Connection conn;
//        conn = DriverManager.getConnection(connstr);
//        Statement stmt = conn.createStatement();
//        stmt.execute("t = table(1..10 as id, 11..20 as val)");
//        ResultSet rs = stmt.executeQuery("select * from t");
//        ResultSetMetaData rsmd = rs.getMetaData();
//        if(rsmd.getColumnCount() == 2){
//            trigger = true;
//        }
//        return trigger;
//    }
//    public static void CreateDfsTable(String host, Integer port) throws IOException {
//        String script = "login(`admin, `123456); \n"+
//                "if(existsDatabase('dfs://db_testDriverManager')){ dropDatabase('dfs://db_testDriverManager')} \n"+
//                "t = table(1..10000 as id, take(1, 10000) as val) \n"+
//                "db=database('dfs://db_testDriverManager', RANGE, 1 2001 4001 6001 8001 10001) \n"+
//                "db.createPartitionedTable(t, `pt, `id).append!(t) \n";
//        DBConnection db = new DBConnection();
//        db.connect(host, port);
//        db.run(script);
//        db.close();
//    }
//
//    @Test
//    public void Test_getConnection_with_dfsdatabasePath() throws Exception {
//        CreateDfsTable(HOST,PORT);
//        String url1 = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&databasePath=dfs://db_testDriverManager";
//        boolean connected = CreateConnection1(url1);
//        org.junit.Assert.assertTrue(connected);
//    }
//
//
//}
//
////        DatabaseMetaData metaData = connection.getMetaData();
////        ResultSet tableTypes1 = metaData.getTableTypes();
////        System.out.println("tableType1: " + JSON.toJSONString(tableTypes1.getMetaData()));
////        System.out.println("metaData: " + metaData);
//////        DatabaseMetaData metaData = jdbcConnection.getMetaData();
////        String[] tableTypes = {"TABLE"};
////        ResultSet tables = metaData.getTables(null, null, "%", tableTypes);
////        while (Objects.nonNull(tables) && tables.next()) {
////            String tableName = tables.getString("TABLE_NAME");
////            String tableType = tables.getString("TABLE_TYPE");
////            String remarks = tables.getString("REMARKS");
////            System.out.println(tableName + " - " + tableType + " - " + remarks);
////        }
////        // System.out.println(metaData.toString());
////       // System.out.println(JSON.toJSONString(metaData));
////        // connection.run("login(\"admin\", \"123456\")");
////        // Entity entity = connection.run("select * from loadTable(\"dfs://test\", \"tb1\")");
////        Statement statement = connection.createStatement();
////        statement.executeQuery("select * from loadTable(\"dfs://test\", \"tb1\")");
//////        Statement statement = jdbcConnection.createStatement();
//////        statement.execute("tb1=loadTabe(\"dfs://test\", \"tb1\")");
//////        statement.executeQuery("select * from tb1");
