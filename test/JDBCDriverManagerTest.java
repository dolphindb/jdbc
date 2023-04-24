import org.junit.*;


import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

import com.xxdb.DBConnection;
import org.junit.Test;

public class JDBCDriverManagerTest {
	static String HOST = JDBCTestUtil.HOST;
	static int PORT = JDBCTestUtil.PORT;
	static String DATABASE = JDBCTestUtil.WORK_DIR+"/JDBC_driver_test_db";
    Properties LOGININFO = new Properties();
	static Connection conn = null;
	static Statement stmt = null;
	static ResultSet rs = null;

	@Before
	public void SetUp(){
		LOGININFO = new Properties();
		LOGININFO.put("user", "admin");
		LOGININFO.put("password", "123456");
	}

	@After
	public void clean() throws SQLException {
		try {
			conn.close();
			stmt.close();
			rs.close();
		}catch (Exception ex){
			System.out.println(ex);
		}
	}

	public static void CreateDiskTable(String host, Integer port) throws IOException {
		DBConnection db = null;
		String script = "login(`admin, `123456); \n"+
				"if(existsDatabase('"+DATABASE+"')){ dropDatabase('"+DATABASE+"')} \n"+
				"t = table(1..10000 as id, take(1, 10000) as val) \n"+
				"db=database('"+DATABASE+"', RANGE, 1 2001 4001 6001 8001 10001) \n"+
				"db.createPartitionedTable(t, `pt, `id).append!(t) \n";
		db = new DBConnection();
		db.connect(host, port);
		db.run(script);
	}
    public static void CreateDfsTable(String host, Integer port) throws IOException {
		String script = "login(`admin, `123456); \n"+
						"if(existsDatabase('dfs://db_testDriverManager')){ dropDatabase('dfs://db_testDriverManager')} \n"+
						"t = table(1..10000 as id, take(1, 10000) as val) \n"+
						"db=database('dfs://db_testDriverManager', RANGE, 1 2001 4001 6001 8001 10001) \n"+
						"db.createPartitionedTable(t, `pt, `id).append!(t) \n";
		DBConnection db = new DBConnection();
		db.connect(host, port);
		db.run(script);
		db.close();
    }

    public boolean CreateConnection1(String connstr) throws ClassNotFoundException, SQLException {
    	Boolean trigger = false;
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(connstr);
		stmt = conn.createStatement();
		stmt.execute("t = table(1..10 as id, 11..20 as val)");
		rs = stmt.executeQuery("select * from t");
		ResultSetMetaData rsmd = rs.getMetaData();
		if(rsmd.getColumnCount() == 2){
			trigger = true;
		}
		return trigger;
	}
    
    public static boolean CreateConnection2(String connstr, String user, String pwd) throws SQLException, ClassNotFoundException {
		Boolean trigger = false;
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(connstr, user, pwd);
		stmt = conn.createStatement();
		stmt.execute("t = table(1..10 as id, 11..20 as val)");
		rs = stmt.executeQuery("select * from t");
		ResultSetMetaData rsmd = rs.getMetaData();
		if(rsmd.getColumnCount() == 2){
			trigger = true;
		}
		return trigger;
	}
    
    public static boolean CreateConnection3(String connstr, Properties info) throws SQLException, ClassNotFoundException {
		Boolean trigger = false;
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(connstr, info);
		stmt = conn.createStatement();
		stmt.execute("t = table(1..10 as id, 11..20 as val)");
		rs = stmt.executeQuery("select * from t");
		ResultSetMetaData rsmd = rs.getMetaData();
		if(rsmd.getColumnCount() == 2){
			trigger = true;
		}
		return trigger;
    }

	@Test
	public void Test_getDriver_true() throws Exception {
		String url1 = "jdbc:dolphindb://"+HOST+":"+PORT;
		CreateConnection1(url1);
		Driver a = DriverManager.getDriver(url1);
		boolean re = a.acceptsURL(url1);
		org.junit.Assert.assertTrue(re);
	}

	@Test
	public void Test_getPropertyInfo() throws Exception {
		String url1 = "jdbc:dolphindb://"+HOST+":"+PORT;
		CreateConnection1(url1);
		Driver a = DriverManager.getDriver(url1);
		Properties pr = new Properties();
		DriverPropertyInfo[] re = a.getPropertyInfo(url1,pr);
		org.junit.Assert.assertEquals(0,re.length);
	}

	@Test
	public void Test_getMajorVersion() throws Exception {
		String url1 = "jdbc:dolphindb://"+HOST+":"+PORT;
		CreateConnection1(url1);
		Driver a = DriverManager.getDriver(url1);
		Properties pr = new Properties();
		int re = a.getMajorVersion();
		org.junit.Assert.assertEquals(2,re);
	}

	@Test
	public void Test_getMinorVersion() throws Exception {
		String url1 = "jdbc:dolphindb://"+HOST+":"+PORT;
		CreateConnection1(url1);
		Driver a = DriverManager.getDriver(url1);
		Properties pr = new Properties();
		int re = a.getMinorVersion();
		org.junit.Assert.assertEquals(0,re);
	}

	@Test
	public void Test_jdbcCompliant() throws Exception {
		String url1 = "jdbc:dolphindb://"+HOST+":"+PORT;
		CreateConnection1(url1);
		Driver a = DriverManager.getDriver(url1);
		Properties pr = new Properties();
		boolean re = a.jdbcCompliant();
		org.junit.Assert.assertEquals(false,re);
	}

	@Test
	public void Test_getParentLogger() throws Exception {
		String url1 = "jdbc:dolphindb://"+HOST+":"+PORT;
		CreateConnection1(url1);
		Driver a = DriverManager.getDriver(url1);
		Properties pr = new Properties();
		Logger re = a.getParentLogger();
		org.junit.Assert.assertEquals("dolphindb",re.getName());
	}

	@Test(expected = SQLException.class)
	public void Test_createConnection_url_null() throws Exception {
		String url1 = null;
		CreateConnection1(url1);
	}

	@Test()
	public void Test_createConnection_url_default_8848() throws Exception {
		String url1 = "jdbc:dolphindb://?";
		CreateConnection1(url1);
	}

	@Test()
	public void Test_createConnection_url_default_8848_2() throws Exception {
		String url1 = "jdbc:dolphindb://";
		CreateConnection1(url1);
	}

	@Test(expected = SQLException.class)
	public void Test_createConnection_url_default_8848_3() throws Exception {
		String url1 = "jdbc:dolphindb://===";
		CreateConnection1(url1);
	}

	@Test(expected = SQLException.class)
	public void Test_getConnection_with_error_host_port() throws Exception {
		String url1 = "jdbc:dolphindb://127.1.1.1:3214:1:1";
		CreateConnection1(url1);
	}

	@Test
    public void Test_getConnection_with_host_port() throws Exception {
        String url1 = "jdbc:dolphindb://"+HOST+":"+PORT;
        boolean connected = CreateConnection1(url1);
        org.junit.Assert.assertTrue(connected);
    }

    @Test
    public void Test_getConnection_with_dfsdatabasePath() throws Exception {
		CreateDfsTable(HOST,PORT);
        String url1 = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&databasePath=dfs://db_testDriverManager";
        boolean connected = CreateConnection1(url1);
        org.junit.Assert.assertTrue(connected);
    }
    
    @Test
    public void Test_getConnection_with_diskdatabasePath() throws Exception {
		CreateDiskTable(HOST,PORT);
        String url1 = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&databasePath="+DATABASE;
        boolean connected = CreateConnection1(url1);
        org.junit.Assert.assertTrue(connected);
    }
    
    @Test
    public void Test_getConnection_with_nothing() throws Exception {
		if(PORT == 8848 && (HOST == "localhost" ||HOST ==  "127.0.0.1" )) {
			String url1 = "jdbc:dolphindb://";//default:localhost:8848
			boolean connected = CreateConnection1(url1);
			org.junit.Assert.assertTrue(connected);
		}else{
				String url1 = "jdbc:dolphindb://"+HOST+":"+PORT;
				boolean connected = CreateConnection1(url1);
				org.junit.Assert.assertTrue(connected);
		}
	}
    
    @Test
    public void Test_getConnection_with_user_password() throws Exception {
        String url1 = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        boolean connected = CreateConnection1(url1);
        org.junit.Assert.assertTrue(connected);
    }
    
    @Test
    public void Test_getConnection_three_parameters() throws Exception {
        String url1 = "jdbc:dolphindb://"+HOST+":"+PORT;
        String username = "admin";
        String pwd = "123456";
        boolean connected = CreateConnection2(url1, username, pwd);
        org.junit.Assert.assertTrue(connected);
    }
    
    @Test
    public void Test_getConnection_two_parameters() throws Exception {
        String url1 = "jdbc:dolphindb://"+HOST+":"+PORT;
        Properties info = new Properties();
        info.put("user", "admin");
        info.put("password", "123456");
        boolean connected = CreateConnection3(url1, info);
        org.junit.Assert.assertTrue(connected);
    }
    
    
    
    @After
    public void Destroy(){
        LOGININFO = null;
    }
}
