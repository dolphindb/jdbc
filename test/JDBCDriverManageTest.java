import org.junit.*;


import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

import com.xxdb.DBConnection;
import org.junit.Test;

public class JDBCDriverManageTest {
	static String HOST = JDBCTestUtil.HOST;
	static int PORT = JDBCTestUtil.PORT;
	static String DATABASE = JDBCTestUtil.WORK_DIR+"/JDBC_driver_test_db";
    Properties LOGININFO = new Properties();


	public static boolean CreateDiskTable(String host, Integer port){
		boolean success = false;
		DBConnection db = null;
		try{
			String script = "login(`admin, `123456); \n"+
					"if(existsDatabase('"+DATABASE+"')){ dropDatabase('"+DATABASE+"')} \n"+
					"t = table(1..10000 as id, take(1, 10000) as val) \n"+
					"db=database('"+DATABASE+"', RANGE, 1 2001 4001 6001 8001 10001) \n"+
					"db.createPartitionedTable(t, `pt, `id).append!(t) \n";
			db = new DBConnection();
			db.connect(host, port);
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
    public static boolean CreateDfsTable(String host, Integer port){
    	boolean success = false;
    	DBConnection db = null;
    	try{
    		String script = "login(`admin, `123456); \n"+
    						"if(existsDatabase('dfs://db_testDriverManager')){ dropDatabase('dfs://db_testDriverManager')} \n"+
    						"t = table(1..10000 as id, take(1, 10000) as val) \n"+
    						"db=database('dfs://db_testDriverManager', RANGE, 1 2001 4001 6001 8001 10001) \n"+
    						"db.createPartitionedTable(t, `pt, `id).append!(t) \n";
    		db = new DBConnection();
    		db.connect(host, port);
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
    
    @Before
    public void SetUp(){
        LOGININFO = new Properties();
        LOGININFO.put("user", "admin");
        LOGININFO.put("password", "123456");
    }


    public static boolean CreateConnection1(String connstr) {
    	String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
    	boolean flag = false;
    	Connection conn = null;
    	Statement stmt = null;
    	ResultSet rs = null;
    	try{
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(connstr);
    		stmt = conn.createStatement();
    		stmt.execute("t = table(1..10 as id, 11..20 as val)");
    		rs = stmt.executeQuery("select * from t");
    		ResultSetMetaData rsmd = rs.getMetaData();
    		int len = rsmd.getColumnCount();
    		flag = true;
    	}catch(Exception e){
    		e.printStackTrace();
    		flag = false;
    	}finally{
    		if(rs != null){
    			try{
    				rs.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(stmt != null){
    			try{
    				stmt.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(conn != null){
    			try{
    				conn.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		return flag;
    	}
    }
    
    public static boolean CreateConnection2(String connstr, String user, String pwd) {
    	String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
    	boolean flag = false;
    	Connection conn = null;
    	Statement stmt = null;
    	ResultSet rs = null;
    	try{
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(connstr, user, pwd);
    		stmt = conn.createStatement();
			stmt.execute("t = table(1..10 as id, 11..20 as val)");
    		rs = stmt.executeQuery("select * from t");
    		ResultSetMetaData rsmd = rs.getMetaData();
    		int len = rsmd.getColumnCount();
    		flag = true;
    	}catch(Exception e){
    		e.printStackTrace();
    		flag = false;
    	}finally{
    		if(rs != null){
    			try{
    				rs.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(stmt != null){
    			try{
    				stmt.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(conn != null){
    			try{
    				conn.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		return flag;
    	}
    }
    
    public static boolean CreateConnection3(String connstr, Properties info) {
    	String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
    	boolean flag = false;
    	Connection conn = null;
    	Statement stmt = null;
    	ResultSet rs = null;
    	try{
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(connstr, info);
    		stmt = conn.createStatement();
			stmt.execute("t = table(1..10 as id, 11..20 as val)");
    		rs = stmt.executeQuery("select * from t");
    		ResultSetMetaData rsmd = rs.getMetaData();
    		int len = rsmd.getColumnCount();
    		flag = true;
    	}catch(Exception e){
    		e.printStackTrace();
    		flag = false;
    	}finally{
    		if(rs != null){
    			try{
    				rs.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(stmt != null){
    			try{
    				stmt.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(conn != null){
    			try{
    				conn.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		return flag;
    	}
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
