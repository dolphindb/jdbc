
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.containsString;

import java.awt.List;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

import com.xxdb.DBConnection;

public class JDBCStatementTest {
	String HOST; 
	int PORT;
	
	@Before
    public void SetUp(){
        HOST = "192.168.1.201" ;
        PORT = 18848 ;
    }
	
    public static boolean CreateDfsTable(String host, Integer port){
    	boolean success = false;
    	DBConnection db = null;
    	try{
    		String script = "login(`admin, `123456); \n"+
    						"if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n"+
    						"t = table(1..10000 as id, take(1, 10000) as val) \n"+
    						"db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10006) \n"+
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
	
    @Test
    public void Test_ststement_inmemory_execute() throws Exception{
    	String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
    	Connection conn = null;
    	Statement stmt = null;
    	ResultSet rs = null;
    	boolean res1=false;
    	boolean res2=true;
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("t=table(1..10 as id,11..20 as val)");
    		res1 = stmt.execute("select * from t");
    		res2 = stmt.execute("insert into t values(11,21)");
    		org.junit.Assert.assertEquals(true, res1);
    		org.junit.Assert.assertEquals(false, res2);
    	}
    	catch(Exception e) {
    		e.printStackTrace();
    	}
    	finally {
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
    	}
    }
    
    
    
    @Test
    public void Test_statement_inmemory_execQuery() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
    	Connection conn = null;
    	Statement stmt = null;
    	ResultSet rs = null;
    	try{
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("t=table(1..10 as id, 11..20 as val)");
    		rs = stmt.executeQuery("select * from t");
    		ResultSetMetaData rsmd = rs.getMetaData();
    		int len = rsmd.getColumnCount();
    		org.junit.Assert.assertEquals(len, 2);
    	}catch(Exception e){
    		e.printStackTrace();
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
    	}
	}
    
//	@Test
//	public void Test_statement_inmemory_execUpdate() throws Exception{
//		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver"; 
//		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
//	    Connection conn = null;
//	    Statement stmt = null;
//	    ResultSet rs = null;
//		try {
//			Class.forName(JDBC_DRIVER);
//			conn = DriverManager.getConnection(url);
//			stmt = conn.createStatement();
//			stmt.execute("t=table(1..10 as id,11..20 as val)");
//			String sql1 = "insert into t values(11 12 13,21 22 23)";
//			String sql2 = "delete from t where id < 4";
//			String sql3 = "update t set id=id+1";
//    		String sql4 = "drop(t,3)";
//			int result1 = stmt.executeUpdate(sql1);
//			int result2 = stmt.executeUpdate(sql2);
//			int result3 = stmt.executeUpdate(sql3);
//    		int result3 = stmt.executeUpdate(sql4);
//			System.out.println(result1);
//			System.out.println(result2);
//			System.out.println(result3);
//			System.out.println(result4);
//			org.junit.Assert.assertEquals(result1,3);
//			org.junit.Assert.assertEquals(result2, 1);
//			org.junit.Assert.assertEquals(result3, 12);
//			org.junit.Assert.assertEquals(result3, 3);
//		}
//		
//		catch(Exception e) {
//			e.printStackTrace();			
//		}
//		finally {
//			if(rs != null){
//    			try{
//    				rs.close();
//    			}catch(SQLException e){
//    				e.printStackTrace();
//    			}
//    		}
//    		if(stmt != null){
//    			try{
//    				stmt.close();
//    			}catch(SQLException e){
//    				e.printStackTrace();
//    			}
//    		}
//    		if(conn != null){
//    			try{
//    				conn.close();
//    			}catch(SQLException e){
//    				e.printStackTrace();
//    			}
//    		}
//		}
//	}
	
//	@Test
//	public void Test_statement_inmemory_execBatch() throws Exception{
//		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
//		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
//	    Connection conn = null;
//	    Statement stmt = null;
//	    ResultSet rs = null;
//	    try {
//	    	Class.forName(JDBC_DRIVER);
//	    	conn = DriverManager.getConnection(url);
//	    	stmt = conn.createStatement();
//	    	stmt.execute("t=table(1..10 as id,11..20 as val)");
//	    	stmt.addBatch("insert into t values(11 12,21 22)");
//	    	stmt.addBatch("insert into t values(13 14,45 46)");
//	    	int[] affectCount= stmt.executeBatch();
//	    	System.out.println(Arrays.toString(affectCount));
//
//	    }
//	    catch(Exception e) {
//	    	e.printStackTrace();
//	    }
//		finally {
//			if(rs != null){
//    			try{
//    				rs.close();
//    			}catch(SQLException e){
//    				e.printStackTrace();
//    			}
//    		}
//    		if(stmt != null){
//    			try{
//    				stmt.close();
//    			}catch(SQLException e){
//    				e.printStackTrace();
//    			}
//    		}
//    		if(conn != null){
//    			try{
//    				conn.close();
//    			}catch(SQLException e){
//    				e.printStackTrace();
//    			}
//    		}
//		}
//	}
	
	
	@Test
	public void Test_statement_dfs_execute() throws Exception{
		boolean success = CreateDfsTable(HOST, PORT);
		org.junit.Assert.assertTrue(success);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver"; 
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
    	Connection conn = null;
    	Statement stmt = null;
    	ResultSet rs = null;
    	boolean res1 = false;
    	boolean res2 = true;
    	try{
    		Class.forName(JDBC_DRIVER); 
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		res1 = stmt.execute("select * from pt");
    		stmt.execute("t=table(1001 as id,2 as val)");
    		res2 = stmt.execute("pt.append!(t)");
    		org.junit.Assert.assertTrue(res1);
    		org.junit.Assert.assertFalse(res2);
    	}catch(Exception e){
    		e.printStackTrace();

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
    	}
	}
	

	@Test
	public void Test_statement_dfs_execQuery() throws Exception{
		boolean success = CreateDfsTable(HOST, PORT);
		org.junit.Assert.assertTrue(success);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver"; 
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
    	Connection conn = null;
    	Statement stmt = null;
    	ResultSet rs = null;
    	try{
    		Class.forName(JDBC_DRIVER); 
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		rs = stmt.executeQuery("select * from pt");
    		ResultSetMetaData rsmd = rs.getMetaData();
    		int len = rsmd.getColumnCount();
    		org.junit.Assert.assertEquals(len, 2);
    	}catch(Exception e){
    		e.printStackTrace();
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
    	}
	}
	
	@Test
	public void Test_statement_dfs_execUpdate() throws Exception{
		boolean success = CreateDfsTable(HOST, PORT);
		org.junit.Assert.assertTrue(success);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver"; 
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
    	Connection conn = null;
    	Statement stmt = null;
    	ResultSet rs = null;
    	String s = "only local in-memory table can update";
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		stmt.executeUpdate("delete from pt where id=1");
    	}catch(Exception e) {
    		org.junit.Assert.assertThat(e.getMessage(),containsString(s));
    	}
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		stmt.executeUpdate("update pt set id=id+1");
    	}catch(Exception e) {
    		org.junit.Assert.assertThat(e.getMessage(),containsString(s));
    	}
    	try{
    		Class.forName(JDBC_DRIVER); 
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		stmt.execute("t=table(10002 as id,3 as val)");
    		int res = stmt.executeUpdate("pt.append!(t)");
//    		System.out.println(res);
    		org.junit.Assert.assertEquals(res, 1);
    	}catch(Exception e){
    		e.printStackTrace();
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
    	}
	}
	
//	@Test
//	public void Test_statement_dfs_execBatch() throws Exception{
//		boolean success = CreateDfsTable(HOST, PORT);
//		org.junit.Assert.assertTrue(success);
//		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver"; 
//		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
//    	Connection conn = null;
//    	Statement stmt = null;
//    	ResultSet rs = null;
//    	try {
//    		Class.forName(JDBC_DRIVER);
//    		conn = DriverManager.getConnection(url);
//    		stmt = conn.createStatement();
//    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
//    		stmt.execute("t=table(10001 as id,3 as val)");
//    		stmt.addBatch("append!(t)");
//    		stmt.execute("t2=table(10002 10003 as id,4 5 as val)");
//    		stmt.addBatch("append!(t)");
//    		int[] res = stmt.executeBatch();
//    		int[] a = {1,2};
//    		System.out.print(Arrays.toString(res));
//    		System.out.print(Arrays.toString(a));
//
//    		org.junit.Assert.assertArrayEquals(res, a);
//    	}catch(Exception e) {
//    		e.printStackTrace();
//    	}finally {
//    		if(rs != null){
//    			try{
//    				rs.close();
//    			}catch(SQLException e){
//    				e.printStackTrace();
//    			}
//    		}
//    		if(stmt != null){
//    			try{
//    				stmt.close();
//    			}catch(SQLException e){
//    				e.printStackTrace();
//    			}
//    		}
//    		if(conn != null){
//    			try{
//    				conn.close();
//    			}catch(SQLException e){
//    				e.printStackTrace();
//    			}
//    		}
//    	}
//	}

	

}
