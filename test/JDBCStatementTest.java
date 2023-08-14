
import com.dolphindb.jdbc.JDBCResultSet;
import com.dolphindb.jdbc.JDBCStatement;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.containsString;

import java.awt.List;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import com.xxdb.DBConnection;

public class JDBCStatementTest {
	static String HOST = JDBCTestUtil.HOST ;
	static int PORT = JDBCTestUtil.PORT ;
	
	@Before
    public void SetUp(){
    }
	
    public static boolean CreateDfsTable(String host, Integer port){
    	boolean success = false;
    	DBConnection db = null;
    	try{
    		String script = "login(`admin, `123456); \n"+
    				"if(existsDatabase(\"dfs://db_testStatement\")){ dropDatabase('dfs://db_testStatement')}\n" +
    				"t=table(`C`MS`MS`MS`IBM`IBM`C`C`C as sym,"
    				+ "49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 as price"
    				+ ",2200 1900 2100 3200 6800 5400 1300 2500 8800 as qty, "
    				+ "[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12] as timestamp)\n" +
					"t2 = table(`IBM`IBM`XM`APPL`AMZON`MS`GOOG`ORCL as sym," +
					"'a' 'b' 'c' 'd' 'e' 'f' 'g' 'h' as char," +
					"true false false true true true false false as bool," +
					"11:30m 12:30m 13:30m 14:30m 15:30m 16:30m 17:30m 18:30m as minute);"+
    				"db=database(\"dfs://db_testStatement\",VALUE,`C`AMZON`MS`IBM`XM`GOOG`ORCL`APPL);\n" +
    				"pt=db.createPartitionedTable(t, `pt, `sym).append!(t);" +
					"qt=db.createPartitionedTable(t2,`qt,`sym).append!(t2);";
//    		String script = "login(`admin, `123456); \n"+
//					"if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n"+
//					"t = table(1..10000 as id, take(1, 10000) as val) \n"+
//					"db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10006) \n"+
//					"db.createPartitionedTable(t, `pt, `id).append!(t) \n";

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
    	boolean res1=false;
    	boolean res2=false;
    	boolean res3=false;
    	boolean res4=false;
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("t=table(1..10 as id,11..20 as val)");
    		res1 = stmt.execute("select * from t");
    		res2 = stmt.execute("insert into t values(11,21)");
    		res2 = stmt.execute("delete from t where id=2");
    		res2 = stmt.execute("update t set id=id+1");
    		org.junit.Assert.assertTrue(res1);
    		org.junit.Assert.assertFalse(res2);
    		org.junit.Assert.assertFalse(res3);
    		org.junit.Assert.assertFalse(res4);
    	}
    	catch(Exception e) {
    		e.printStackTrace();
    	}
    	finally {
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
    	ResultSet rs1 = null;
    	ResultSet rs2 = null;
    	ResultSet rs3 = null;
    	ResultSet rs4 = null;
    	ResultSet rs5 = null;
    	ResultSet rs6 = null;
    	ResultSet rs7 = null;
    	int i=0;
    	String s = "the given SQL statement produces anything other than a single ResultSet object";
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("t=table(`C`MS`MS`MS`IBM`IBM`C`C`C as sym,49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 as price,"
    				+ "2200 1900 2100 3200 6800 5400 1300 2500 8800 as qty, "
    				+ "[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12] as timestamp)");
    		rs1 = stmt.executeQuery("exec count(*) from t");
    	}catch(Exception e) {
    		org.junit.Assert.assertThat(e.getMessage(),containsString(s));
    	}
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("t=table(`C`MS`MS`MS`IBM`IBM`C`C`C as sym,49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 as price,"
    				+ "2200 1900 2100 3200 6800 5400 1300 2500 8800 as qty, "
    				+ "[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12] as timestamp)");
    		rs2 = stmt.executeQuery("exec price from t");
    	}catch(Exception e) {
    		org.junit.Assert.assertThat(e.getMessage(), containsString(s));
    	}
    	try{
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("t=table(`C`MS`MS`MS`IBM`IBM`C`C`C as sym,49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 as price,"
    				+ "2200 1900 2100 3200 6800 5400 1300 2500 8800 as qty, "
    				+ "[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12] as timestamp)");
    		rs3 = stmt.executeQuery("select * from t where price>100 order by qty");
    		rs3.absolute(2);
    		org.junit.Assert.assertEquals(6800, rs3.getInt(3));	
    		rs4 = stmt.executeQuery("select max(price) from t group by sym");
    		rs4.absolute(3);
    		org.junit.Assert.assertEquals(175.23, rs4.getDouble(2),0);
    		rs5 = stmt.executeQuery("select wavg(price, qty) as wvap from t where sym = 'IBM' cgroup by timestamp order by timestamp");
    		rs5.absolute(1);
    		org.junit.Assert.assertEquals(174.97, rs5.getDouble(2),0);
    		rs6 = stmt.executeQuery("select max(price) from t context by sym having sym=`C");
    		while(rs6.next()) {
    			if(rs6.getDouble(1)==51.29) {
    				i++;
    			}
    		}
    		org.junit.Assert.assertEquals(4, i);
    		rs7 = stmt.executeQuery("select price from t pivot by timestamp,sym");
    		ResultSetMetaData rsmd = rs7.getMetaData();
    		org.junit.Assert.assertEquals(4, rsmd.getColumnCount());   		
    	}catch(Exception e){
    		e.printStackTrace();
    	}finally{
    		if(rs1 != null){
    			try{
    				rs1.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(rs2 != null){
    			try{
    				rs2.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(rs3 != null){
    			try{
    				rs3.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(rs4 != null){
    			try{
    				rs4.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(rs5 != null){
    			try{
    				rs5.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(rs6 != null){
    			try{
    				rs6.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(rs7 != null){
    			try{
    				rs7.close();
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
	public void Test_statement_inmemory_execUpdate() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver"; 
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
	    Connection conn = null;
	    Statement stmt = null;
	    int rs = 0;
	    int rs1 = 0;
	    int rs2 = 0;
	    int rs3 = 0;
	    String s = "Can not issue SELECT or EXEC via executeUpdate()";
	    try {
	    	Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			stmt.execute("t=table(1..10 as id,11..20 as val)");
			String sql = "select * from t where id>5 order by id desc";
			rs = stmt.executeUpdate(sql);
	    }catch(Exception e) {
	    	org.junit.Assert.assertThat(e.getMessage(), containsString(s));
	    }
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			stmt.execute("t=table(1..10 as id,11..20 as val)");
			String sql1 = "insert into t values(11 12 13,21 22 23)";
			String sql2 = "delete from t where id < 4";
			String sql3 = "update t set id=id+1";
			rs1 = stmt.executeUpdate(sql1);
			rs2 = stmt.executeUpdate(sql2);
			rs3 = stmt.executeUpdate(sql3);
			org.junit.Assert.assertEquals(rs1,3);
			org.junit.Assert.assertEquals(rs2, -2);
			org.junit.Assert.assertEquals(rs3, -2);
		}
		
		catch(Exception e) {
			e.printStackTrace();			
		}
		finally {
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
	public void Test_statement_inmemory_execBatch() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
	    Connection conn = null;
	    Statement stmt = null;
	    ResultSet rs = null;
	    try {
	    	Class.forName(JDBC_DRIVER);
	    	conn = DriverManager.getConnection(url);
	    	stmt = conn.createStatement();
	    	stmt.execute("t=table(`C`MS`MS`MS`IBM`IBM`C`C`C as sym,49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 as price,"
	    				+ "2200 1900 2100 3200 6800 5400 1300 2500 8800 as qty,"
	    				+ "[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12] as timestamp)");
	    	stmt.addBatch("insert into t values(`IBM,66.6,6500,09:34:15)");
	    	stmt.addBatch("update t set qty=qty+100");
	    	stmt.addBatch("delete from t where sym=`IBM");
	    	int[] expected = {1,-2,-2,0};
	    	int[] affectCount= stmt.executeBatch();
	    	org.junit.Assert.assertArrayEquals(expected, affectCount);
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
	public void Test_statement_dfs_execute() throws Exception{
		boolean success = CreateDfsTable(HOST, PORT);
		org.junit.Assert.assertTrue(success);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver"; 
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
    	Connection conn = null;
    	Statement stmt = null;
    	ResultSet rs = null;
    	boolean res1 = false;
    	boolean res2 = false;
    	String s = "only local in-memory table can update";
    	try {
    		Class.forName(JDBC_DRIVER); 
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		stmt.execute("delete from pt where sym=`C");
    	}catch(Exception e) {
    		org.junit.Assert.assertThat(e.getMessage(),containsString(s));
    	}
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		stmt.execute("update pt set price=price+100");
    	}catch(Exception e) {
    		org.junit.Assert.assertThat(e.getMessage(),containsString(s));
    	}
    	try{
    		Class.forName(JDBC_DRIVER); 
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		res1 = stmt.execute("select * from pt");
    		stmt.execute("t=table(`IBM as sym,35.16 as price,3500 as qty,09:36:48 as timestamp)");
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
    	ResultSet rs1 = null;
    	ResultSet rs2 = null;
    	ResultSet rs3 = null;
    	ResultSet rs4 = null;
    	ResultSet rs5 = null;
    	ResultSet rs6 = null;
    	ResultSet rs7 = null;
    	int i =0;
    	String s="the given SQL statement produces anything other than a single ResultSet object";
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		rs1 = stmt.executeQuery("exec count(*) from pt");
    	}catch(Exception e) {
    		org.junit.Assert.assertThat(e.getMessage(),containsString(s));
    	}
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		rs2 = stmt.executeQuery("exec price from pt");
    	}catch(Exception e) {
    		org.junit.Assert.assertThat(e.getMessage(), containsString(s));
    	}
    	try{
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		rs3 = stmt.executeQuery("select * from pt where price>100 order by qty");
    		rs3.absolute(2);
    		org.junit.Assert.assertEquals(6800, rs3.getInt(3));	
    		rs4 = stmt.executeQuery("select max(price) from pt group by sym");
    		rs4.absolute(3);
    		org.junit.Assert.assertEquals(30.02, rs4.getDouble(2),0);
    		rs5 = stmt.executeQuery("select wavg(price, qty) as wvap from pt where sym = 'IBM' cgroup by timestamp order by timestamp");
    		rs5.absolute(1);
    		org.junit.Assert.assertEquals(174.97, rs5.getDouble(2),0);
    		rs6 = stmt.executeQuery("select max(price) from pt context by sym having sym=`C");
    		while(rs6.next()) {
    			if(rs6.getDouble(1)==51.29) {
    				i++;
    			}
    		}
    		org.junit.Assert.assertEquals(4, i);
    		rs7 = stmt.executeQuery("select price from pt pivot by timestamp,sym");
    		ResultSetMetaData rsmd = rs7.getMetaData();
    		org.junit.Assert.assertEquals(4, rsmd.getColumnCount());   		
    	}catch(Exception e){
    		e.printStackTrace();
    	}finally{
    		if(rs1 != null){
    			try{
    				rs1.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(rs2 != null){
    			try{
    				rs2.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(rs3 != null){
    			try{
    				rs3.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(rs4 != null){
    			try{
    				rs4.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(rs5 != null){
    			try{
    				rs5.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(rs6 != null){
    			try{
    				rs6.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(rs7 != null){
    			try{
    				rs7.close();
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
    		stmt.executeUpdate("delete from pt where qty=1300");
    	}catch(Exception e) {
    		org.junit.Assert.assertThat(e.getMessage(),containsString(s));
    	}
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		stmt.executeUpdate("update pt set price=price+1");
    	}catch(Exception e) {
    		org.junit.Assert.assertThat(e.getMessage(),containsString(s));
    	}
    	try{
    		Class.forName(JDBC_DRIVER); 
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		stmt.execute("t=table(`IBM as sym,35.16 as price,3500 as qty,09:36:48 as timestamp)");
    		int res = stmt.executeUpdate("pt.append!(t)");
    		org.junit.Assert.assertEquals(res, 0);
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
	public void Test_statement_dfs_execBatch() throws Exception{
		boolean success = CreateDfsTable(HOST, PORT);
		org.junit.Assert.assertTrue(success);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver"; 
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
    	Connection conn = null;
    	Statement stmt = null;
    	ResultSet rs = null;
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		stmt.execute("t=table(`IBM as sym,66.6 as price,3500 as qty,09:34:05 as timestamp)");
    		stmt.addBatch("pt.append!(t)");
    		stmt.execute("t=table(`A`B as sym,45.5 31.2 as price,1000 6500 as qty,09:35:01 09:35:05 as timestamp)");
    		stmt.addBatch("pt.append!(t)");
    		int[] res = stmt.executeBatch();
    		int[] a = {0,0,0};
    		org.junit.Assert.assertArrayEquals(a, res);
    	}catch(Exception e) {
    		e.printStackTrace();
    	}finally {
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
	public void Test_statement_QueryTime() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver"; 
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
    	Connection conn = null;
    	Statement stmt = null;
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.setQueryTimeout(10);
    		int t = stmt.getQueryTimeout();
    		org.junit.Assert.assertEquals(10000, t);
    	}catch(Exception e) {
    		e.printStackTrace();
    	}finally {
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
	public void Test_statement_cancel() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver"; 
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
    	Connection conn = null;
    	Statement stmt = null;
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.cancel();
    	}catch(Exception e) {
    		org.junit.Assert.assertNotNull(e);
    	}finally {
    		if(conn !=null) {
    			try {
    				conn.close();
    			}catch(SQLException e) {
    				e.printStackTrace();
    			}
    		}
    		if(stmt!= null) {
    			try {
    				stmt.close();
    			}catch(Exception e){
    				e.printStackTrace();
    			}
    		}
    	}
	}
	
	@Test
	public void Test_statement_setcursorNames() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver"; 
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
    	Connection conn = null;
    	Statement stmt = null;
    	String na = null;
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.setCursorName(na);
    	}catch(Exception e) {
    		org.junit.Assert.assertNotNull(e);
    	}finally {
    		if(conn !=null) {
    			try {
    				conn.close();
    			}catch(SQLException e) {
    				e.printStackTrace();
    			}
    		}
    		if(stmt!= null) {
    			try {
    				stmt.close();
    			}catch(Exception e){
    				e.printStackTrace();
    			}
    		}
    	}
	}

	@Test
	public void Test_statement_getResultSet() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver"; 
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
    	Connection conn = null;
    	Statement stmt = null;
    	ResultSet rs = null;
    	try {
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("t=table(`C`MS`MS`MS`IBM`IBM`C`C`C as sym,49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 as price,"
    				+ "2200 1900 2100 3200 6800 5400 1300 2500 8800 as qty, "
    				+ "[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12] as timestamp)");
    		stmt.execute("select * from t where qty>5000 order by sym");
    		rs = stmt.getResultSet();
    		rs.absolute(2);
    		org.junit.Assert.assertEquals(174.97, rs.getDouble(2),0);
    		org.junit.Assert.assertEquals(6800, rs.getInt(3));
    		Time t = new Time(9,32,47);
    		org.junit.Assert.assertEquals(t, rs.getTime(4));
    	}catch(Exception e) {
    		e.printStackTrace();
    	}finally {
    		if(rs !=null) {
    			try {
    				rs.close();
    			}catch(SQLException e) {
    				e.printStackTrace();
    			}
    		}
    		if(stmt!= null) {
    			try {
    				stmt.close();
    			}catch(Exception e){
    				e.printStackTrace();
    			}
    		}
    		if(conn !=null) {
    			try {
    				conn.close();
    			}catch(Exception e) {
    				e.printStackTrace();
    			}
    		}
    	}
	}
	
	@Test
	public void Test_statement_getMoreResults() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn =null;
		Statement stmt = null;
		ResultSet rs = null;
		boolean res = false;
		int i = 11;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("t=table(`C`MS`MS`MS`IBM`IBM`C`C`C as sym,49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 as price,"
    				+ "2200 1900 2100 3200 6800 5400 1300 2500 8800 as qty, "
    				+ "[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12] as timestamp)");
    		stmt.execute("t2=table(1 2 3 4 5 as id,11 12 13 14 15 as x0,21 22 23 24 25 as x1,31 32 33 34 35 as  x2)");
    		stmt.execute("[select * from t,select * from t2]");
    		rs = stmt.getResultSet();
    		while(rs.next()) {
    			System.out.println(rs.getDouble(2));
    		}
    		rs.absolute(5);
    		org.junit.Assert.assertEquals(174.97, rs.getDouble(2),0);
    		res = stmt.getMoreResults();
    		if(res) {
    			ResultSet rs1 = stmt.getResultSet();
    			while(rs1.next()) {
    			System.out.println(rs1.getInt(2));
    			org.junit.Assert.assertEquals(i, rs1.getInt(2));
    			i++;
    			}
    		}
    			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(conn !=null) {
    			try {
    				conn.close();
    			}catch(SQLException e) {
    				e.printStackTrace();
    			}
    		}
    		if(stmt !=null) {
    			try {
    				stmt.close();
    			}catch(SQLException e) {
    				e.printStackTrace();
    			}
    		}
		}
	}
	
	@Test
	public void Test_statement_getUpdateCount() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn =null;
		Statement stmt = null;
		int rs = 0;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("t=table(`C`MS`MS`MS`IBM`IBM`C`C`C as sym,49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 as price,"
    				+ "2200 1900 2100 3200 6800 5400 1300 2500 8800 as qty, "
    				+ "[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12] as timestamp)");
    		stmt.execute("insert into t values(`IBM,20.0,1000,09:35:07)");
    		rs = stmt.getUpdateCount();		
    		org.junit.Assert.assertEquals(1, rs);
    		stmt.execute("delete from t where qty<5000");
    		rs = stmt.getUpdateCount();
    		System.out.println(rs);
    		org.junit.Assert.assertEquals(-2, rs);
		}catch(Exception e) {
    		e.printStackTrace();
    	}finally {
    		if(conn !=null) {
    			try {
    				conn.close();
    			}catch(SQLException e) {
    				e.printStackTrace();
    			}
    		}
    		if(stmt !=null) {
    			try {
    				stmt.close();
    			}catch(SQLException e) {
    				e.printStackTrace();
    			}
    		}
    	}
	}

	@Test
	public void Test_statement_isclosed() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn =null;
		Statement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			conn.close();
			org.junit.Assert.assertTrue(conn.isClosed());
			org.junit.Assert.assertFalse(stmt.isClosed());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(conn !=null) {
				try {
					conn.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stmt !=null) {
				try {
					stmt.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	@Test
	public void Test_statement_blob() throws Exception {
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		int rs = 0;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			stmt.execute("t=table(20000:0,`id`val,[INT,BLOB]);" +
					"\n" +
					"t1 = table(1..10000000 as id, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,10000000) as name, take(`aaaaadsfasdfaa`bbbbasdfasbbbbbb`cccasdfasdfasfcccccccccc,10000000) as name1)\n" +
					"a=t1.toJson()\n");
			stmt.execute("insert into t values(1,a)");
			rs = stmt.getUpdateCount();
			org.junit.Assert.assertEquals(1, rs);
			ResultSet rst = stmt.executeQuery("select val from t");
			//rst = stmt.executeQuery("select id from t");
			while (rst.next()){
				//System.out.println(rst.getString("id"));
				System.out.println(rst.getString("val"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void test_JDBCStatement_executeTable(){
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		ResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			stm.execute("use testOuRui");
			stm.execute("query()");
			rs = stm.getResultSet();
			Assert.assertTrue(rs.next());
			Assert.assertEquals("2",rs.getString(1));
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			if(conn !=null) {
				try {
					conn.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	@Test
	public void test_JDBCStatement_executeScalar(){
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			stm.execute("use testOuRui");
			stm.execute("printStr('123')");
			rs = (JDBCResultSet) stm.getResultSet();
			Assert.assertEquals("b123",rs.getResult().getString());
			Assert.assertTrue(rs.getResult().isScalar());
			Assert.assertFalse(rs.getResult().isTable());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			if(conn !=null) {
				try {
					conn.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void test_JDBCStatement_executeVector(){
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			stm.execute("use testOuRui");
			stm.execute("printVector()");
			rs = (JDBCResultSet) stm.getResultSet();
			Assert.assertEquals("[1,2,3,4]",rs.getResult().getString());
			Assert.assertTrue(rs.getResult().isVector());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			if(conn !=null) {
				try {
					conn.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void test_JDBCStatement_length(){
		Assert.assertTrue(CreateDfsTable(HOST,PORT));
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			rs = (JDBCResultSet) stm.executeQuery("select length(sym) from loadTable(\"dfs://db_testStatement\",\"pt\");");
			Assert.assertTrue(rs.next());
			BasicTable bt = (BasicTable) rs.getResult();
			ArrayList<String> colNames = new ArrayList<>();
			colNames.add("strlen_sym");
			ArrayList<Vector> cols = new ArrayList<>();
			cols.add(new BasicIntVector(new int[]{1,1,1,1,3,3,2,2,2}));
			BasicTable ex = new BasicTable(colNames,cols);
			Assert.assertEquals(ex.getString(),bt.getString());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			if(conn !=null) {
				try {
					conn.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void test_JDBCStatement_Nvl() throws IOException {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(`C`MS`MS`MS`IBM`IBM`C`C`C`APPL`XM as sym," +
				"49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 NULL NULL as price," +
				"2200 1900 2100 3200 6800 5400 1300 2500 8800 1080 9000 as qty, " +
				"[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12,09:40:35,09:42:27] as timestamp)" +
				"share t as st";
		db.run(script);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
            rs = (JDBCResultSet) stm.executeQuery("select sym,nvl(price,100),qty from st where qty>1000");
			BasicTable bt = (BasicTable) rs.getResult();
			System.out.println(bt.getString());
			Assert.assertEquals("100",bt.getColumn(1).get(9).getString());
			Assert.assertEquals("100",bt.getColumn(1).get(10).getString());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			db.run("undef(`st,SHARED)");
			if(conn !=null) {
				try {
					conn.close();
					db.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void test_JDBCStatement_replace() throws IOException {
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		ResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			stm.execute("x = replace(\"abcde\",\"cd\",\"Fg\");");
			rs = stm.executeQuery("x;");
			Assert.assertEquals("abFge",rs.getString(1));
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			if(conn !=null) {
				try {
					conn.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void test_JDBCStatement_replaceTable() throws IOException {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(`C`MS`MS`MS`IBM`IBM`C`C`C`APPL`XM as sym," +
				"49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 NULL NULL as price," +
				"2200 1900 2100 3200 6800 5400 1300 2500 8800 1080 9000 as qty, " +
				"[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12,09:40:35,09:42:27] as timestamp)" +
				"share t as st";
		db.run(script);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			rs = (JDBCResultSet) stm.executeQuery("select price,qty,replace(sym,\"IBM\",\"BMI\") from st");
			BasicTable bt = (BasicTable) rs.getResult();
			System.out.println(bt.getString());
			Assert.assertEquals("BMI",bt.getColumn(2).get(4).getString());
			Assert.assertEquals("BMI",bt.getColumn(2).get(5).getString());
			Assert.assertNotEquals("IBM",bt.getColumn(2).get(4).getString());
			Assert.assertNotEquals("IBM",bt.getColumn(2).get(5).getString());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			db.run("undef(`st,SHARED)");
			if(conn !=null) {
				try {
					conn.close();
					db.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void test_JDBCStatement_count() throws IOException {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(`C`MS`MS`MS`IBM`IBM`C`C`C`APPL`XM as sym," +
				"49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 NULL NULL as price," +
				"2200 1900 2100 3200 6800 5400 1300 2500 8800 1080 9000 as qty, " +
				"[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12,09:40:35,09:42:27] as timestamp)" +
				"share t as st";
		db.run(script);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			rs = (JDBCResultSet) stm.executeQuery("select count(sym),count(price),count(qty),count(timestamp ) from st");
			BasicTable bt = (BasicTable) rs.getResult();
			System.out.println(bt.getString());
			Assert.assertEquals(11,((Scalar)bt.getColumn(0).get(0)).getNumber());
			Assert.assertEquals(9,((Scalar)bt.getColumn(1).get(0)).getNumber());
			Assert.assertEquals(11,((Scalar)bt.getColumn(2).get(0)).getNumber());
			Assert.assertEquals(11,((Scalar)bt.getColumn(3).get(0)).getNumber());
			JDBCResultSet JR = (JDBCResultSet) stm.executeQuery("select count(*) from st;");
			System.out.println(JR.getResult().getString());
			JDBCResultSet JRS = (JDBCResultSet) stm.executeQuery("select count(1) from st");
			System.out.println(JRS.getResult().getString());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			db.run("undef(`st,SHARED)");
			if(conn !=null) {
				try {
					conn.close();
					db.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void test_JDBCStatement_avg_min_max_sum() throws IOException {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(`C`MS`MS`MS`IBM`IBM`C`C`C`APPL`XM as sym," +
				"49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 NULL NULL as price," +
				"2200 1900 2100 3200 6800 5400 1300 2500 8800 1080 9000 as qty, " +
				"[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12,09:40:35,09:42:27] as timestamp)" +
				"share t as st";
		db.run(script);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			rs = (JDBCResultSet) stm.executeQuery("select avg(price),avg(qty) from st");
			BasicTable bt = (BasicTable) rs.getResult();
			System.out.println(bt.getString());
            JDBCResultSet JR = (JDBCResultSet) stm.executeQuery("select min(price),MAX(qty) from st;");
			BasicTable bt2 = (BasicTable) JR.getResult();
			Assert.assertEquals(29.46,((Scalar)bt2.getColumn(0).get(0)).getNumber());
			Assert.assertEquals(9000,((Scalar)bt2.getColumn(1).get(0)).getNumber());
			JDBCResultSet JRS = (JDBCResultSet) stm.executeQuery("select sum(price),sum(qty) from st;");
			System.out.println(JRS.getResult().getString());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			db.run("undef(`st,SHARED)");
			if(conn !=null) {
				try {
					conn.close();
					db.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void test_JDBCStatement_distinct_desc_asc() throws IOException {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(`C`MS`MS`MS`IBM`IBM`C`C`C`APPL`XM as sym," +
				"49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 NULL NULL as price," +
				"2200 1900 2100 3200 6800 5400 1300 2500 8800 1080 9000 as qty, " +
				"[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12,09:40:35,09:42:27] as timestamp)" +
				"share t as st";
		db.run(script);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			rs = (JDBCResultSet) stm.executeQuery("select distinct sym from st");
			BasicTable bt = (BasicTable) rs.getResult();
			System.out.println(bt.getString());
			Assert.assertEquals(5,bt.rows());
			JDBCResultSet JR = (JDBCResultSet) stm.executeQuery("select * from st order by price desc;");
			BasicTable bt2 = (BasicTable) JR.getResult();
			Assert.assertEquals(175.23,((Scalar)bt2.getColumn(1).get(0)).getNumber());
			JDBCResultSet jrs = (JDBCResultSet) stm.executeQuery("select * from st order by qty asc;");
			BasicTable bt3 = (BasicTable) jrs.getResult();
			Assert.assertEquals(1080,((Scalar)bt3.getColumn(2).get(0)).getNumber());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			db.run("undef(`st,SHARED)");
			if(conn !=null) {
				try {
					conn.close();
					db.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void test_JDBCStatement_left_outer_join() throws IOException {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(`C`MS`MS`MS`IBM`IBM`C`C`C`APPL`XM as sym," +
				"49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 NULL NULL as price," +
				"2200 1900 2100 3200 6800 5400 1300 2500 8800 1080 9000 as qty, " +
				"[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12,09:40:35,09:42:27] as timestamp)" +
				"share t as st";
		String script2 = "t2 = table(`IBM`IBM`XM`APPL`AMZON`MS`GOOG`ORCL as sym," +
				"'a' 'b' 'c' 'd' 'e' 'f' 'g' 'h' as char," +
				"true false false true true true false false as bool," +
				"11:30m 12:30m 13:30m 14:30m 15:30m 16:30m 17:30m 18:30m as minute)" +
				"share t2 as st2";
		db.run(script);
		db.run(script2);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			rs = (JDBCResultSet) stm.executeQuery("select sym,qty,price,timestamp,char,bool,minute from st2 left outer join st on st.sym = st2.sym;");
			BasicTable bt = (BasicTable) rs.getResult();
			System.out.println(bt.getString());
			Assert.assertEquals(12,bt.rows());
            Assert.assertTrue(((Scalar)bt.getColumn(1).get(6)).isNull());
			Assert.assertTrue(((Scalar)bt.getColumn(2).get(4)).isNull());
			Assert.assertTrue(((Scalar)bt.getColumn(3).get(6)).isNull());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			db.run("undef(`st,SHARED)");
			if(conn !=null) {
				try {
					conn.close();
					db.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void test_JDBCStatement_right_outer_join() throws IOException {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(`C`MS`MS`MS`IBM`IBM`C`C`C`APPL`XM as sym," +
				"49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 NULL NULL as price," +
				"2200 1900 2100 3200 6800 5400 1300 2500 8800 1080 9000 as qty, " +
				"[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12,09:40:35,09:42:27] as timestamp)" +
				"share t as st";
		String script2 = "t2 = table(`IBM`IBM`XM`APPL`AMZON`MS`GOOG`ORCL as sym," +
				"'a' 'b' 'c' 'd' 'e' 'f' 'g' 'h' as char," +
				"true false false true true true false false as bool," +
				"11:30m 12:30m 13:30m 14:30m 15:30m 16:30m 17:30m 18:30m as minute)" +
				"share t2 as st2";
		db.run(script);
		db.run(script2);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			rs = (JDBCResultSet) stm.executeQuery("select sym,qty,price,timestamp,char,bool,minute from st2 right outer join st on st2.sym = st.sym;");
			BasicTable bt = (BasicTable) rs.getResult();
			System.out.println(bt.getString());
			Assert.assertEquals(13,bt.rows());
			Assert.assertTrue(((Scalar)bt.getColumn(4).get(0)).isNull());
			Assert.assertTrue(((Scalar)bt.getColumn(5).get(8)).isNull());
			Assert.assertTrue(((Scalar)bt.getColumn(6).get(9)).isNull());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			db.run("undef(`st,SHARED)");
			if(conn !=null) {
				try {
					conn.close();
					db.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void test_JDBCStatement_outer_join() throws IOException {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(`C`MS`MS`MS`IBM`IBM`C`C`C`APPL`XM as sym," +
				"49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 NULL NULL as price," +
				"2200 1900 2100 3200 6800 5400 1300 2500 8800 1080 9000 as qty, " +
				"[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12,09:40:35,09:42:27] as timestamp)" +
				"share t as st";
		String script2 = "t2 = table(`IBM`IBM`XM`APPL`AMZON`MS`GOOG`ORCL as sym," +
				"'a' 'b' 'c' 'd' 'e' 'f' 'g' 'h' as char," +
				"true false false true true true false false as bool," +
				"11:30m 12:30m 13:30m 14:30m 15:30m 16:30m 17:30m 18:30m as minute)" +
				"share t2 as st2";
		db.run(script);
		db.run(script2);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			rs = (JDBCResultSet) stm.executeQuery("select sym,qty,price,timestamp,char,bool,minute from st2 outer join st on st.sym = st2.sym;");
			BasicTable bt = (BasicTable) rs.getResult();
			System.out.println(bt.getString());
			Assert.assertEquals(16,bt.rows());
			Assert.assertTrue(((Scalar)bt.getColumn(1).get(6)).isNull());
			Assert.assertTrue(((Scalar)bt.getColumn(2).get(4)).isNull());
			Assert.assertTrue(((Scalar)bt.getColumn(3).get(6)).isNull());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			db.run("undef(`st,SHARED)");
			if(conn !=null) {
				try {
					conn.close();
					db.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test(timeout = 120000)
	public void test_JDBCStatement_BigData_outer_join() throws IOException {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(take(`C`AMZON`IBM`XM`GOOG`APPL`ORCL,50000) as sym," +
				"rand(198.99,50000) as price," +
				"take(1..2000,50000) as qty, " +
				"take(01:01:01..23:59:59,50000) as timestamp)" +
				"share t as st";
		String script2 = "t2 = table(take(`C`IBM`GOOG`APPL`ORCL`AMZON,5000) as sym," +
				"take('a'..'z',5000) as char," +
				"take(true false false true true true false false,5000) as bool," +
				"take(01:01m..23:59m,5000) as minute)" +
				"share t2 as st2";
		db.run(script);
		db.run(script2);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			rs = (JDBCResultSet) stm.executeQuery("select sym,qty,price,timestamp,char,bool,minute from st2 left outer join st on st.sym = st2.sym;");
			Map<String,Entity> map = new HashMap<>();
			BasicTable bt = (BasicTable) rs.getResult();
			map.put("JoinTable",bt);
			db.upload(map);
			Assert.assertEquals(0,db.run("select * from JoinTable where sym=`XM").rows());
			JDBCResultSet jr = (JDBCResultSet) stm.executeQuery("select sym,qty,price,timestamp,char,bool,minute from st2 outer join st on st.sym = st2.sym");
			System.out.println(jr.getResult().rows());
			JDBCResultSet jd = (JDBCResultSet) stm.executeQuery("select sym,qty,price,timestamp,char,bool,minute from st2 right outer join st on st.sym = st2.sym");
			System.out.println(jd.getResult().rows());
			Map<String,Entity> map2 = new HashMap<>();
			BasicTable bt2 = (BasicTable) jd.getResult();
			map2.put("RJTable",bt2);
			db.upload(map2);
			Assert.assertTrue(db.run("select * from RJTable where sym=`XM").rows()>0);
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			db.run("undef(`st,SHARED)");
			db.run("undef(`st2,SHARED)");
			if(conn !=null) {
				try {
					conn.close();
					db.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void test_JDBCStatement_DFS_oracle_function() throws IOException {
		DBConnection db1 = new DBConnection();
		db1.connect(HOST,PORT,"admin","123456");
		Assert.assertTrue(CreateDfsTable(HOST,PORT));
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456"+"&databasePath=dfs://db_testStatement";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			rs = (JDBCResultSet) stm.executeQuery("select sym,qty,price,timestamp,char,bool,minute from qt left outer join pt on qt.sym = pt.sym;");
			BasicTable bt = (BasicTable) rs.getResult();
			System.out.println(bt.getString());
			Assert.assertTrue(((Scalar)bt.getColumn(1).get(0)).isNull());
			Assert.assertTrue(((Scalar)bt.getColumn(2).get(1)).isNull());
			Assert.assertTrue(((Scalar)bt.getColumn(3).get(2)).isNull());
			JDBCResultSet JR = (JDBCResultSet) stm.executeQuery("select price,qty,replace(sym,\"IBM\",\"BMI\") from pt");
			BasicTable a = (BasicTable) JR.getResult();
			Map<String,Entity> map = new HashMap<>();
			map.put("replaceTable",a);
			db1.upload(map);
			Assert.assertEquals(0,db1.run("select * from replaceTable where strReplace_sym=`IBM;").rows());
			JDBCResultSet jrs = (JDBCResultSet) stm.executeQuery("select couNt(sym),COUNT(price),COUNT(qty),count(timestamp) from pt");
 			BasicTable jt = (BasicTable) jrs.getResult();
			Assert.assertEquals(9L,((Scalar)jt.getColumn(0).get(0)).getNumber());
			Assert.assertEquals(9L,((Scalar)jt.getColumn(1).get(0)).getNumber());
			Assert.assertEquals(9L,((Scalar)jt.getColumn(2).get(0)).getNumber());
			Assert.assertEquals(9L,((Scalar)jt.getColumn(3).get(0)).getNumber());
			JDBCResultSet jr = (JDBCResultSet) stm.executeQuery("select count(*) from qt");
			System.out.println(jr.getResult().getString());
			JDBCResultSet s = (JDBCResultSet) stm.executeQuery("select COUNT(1) from qt");
			System.out.println(s.getResult().getString());
			JDBCResultSet re = (JDBCResultSet) stm.executeQuery("select Avg(price),MAX(qty),MIN(price),suM(qty) from pt;");
			System.out.println(re.getResult().getString());
			JDBCResultSet je = (JDBCResultSet) stm.executeQuery("select Distinct(sym) from qt");
			BasicTable jet = (BasicTable) je.getResult();
			Assert.assertEquals(7,jet.rows());
			JDBCResultSet js = (JDBCResultSet) stm.executeQuery("select * from pt order by price aSc;");
			BasicTable jst = (BasicTable) js.getResult();
			Assert.assertEquals(29.46,((Scalar)jst.getColumn(1).get(0)).getNumber());
			JDBCResultSet jc = (JDBCResultSet) stm.executeQuery("select * from pt order by qty dEsc;");
			BasicTable jct = (BasicTable) jc.getResult();
			Assert.assertEquals(8800,((Scalar)jct.getColumn(2).get(0)).getNumber());
			JDBCResultSet oj = (JDBCResultSet) stm.executeQuery("select sym,qty,price,timestamp,char,bool,minute from qt  outer join pt on qt.sym = pt.sym;");
			System.out.println(oj.getResult().getString());
			BasicTable ojt = (BasicTable) oj.getResult();
			Assert.assertEquals(16,ojt.rows());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			if(conn !=null) {
				try {
					conn.close();
					db1.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test(timeout = 120000)
	public void test_JDBCStatement_DFS_bigdata() throws IOException {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(take(`C`AMZON`IBM`XM`GOOG`APPL`ORCL,50000) as sym," +
				"rand(198.99,50000) as price," +
				"take(1..2000,50000) as qty, " +
				"take(01:01:01..23:59:59,50000) as timestamp);" +
				"t2 = table(take(`C`IBM`GOOG`APPL`ORCL`AMZON,5000) as sym," +
				"take('a'..'z',5000) as char," +
				"take(true false false true true true false false,5000) as bool," +
				"take(01:01m..23:59m,5000) as minute);" +
				"if(existsDatabase(\"dfs://db_testStatement\")){dropDatabase(\"dfs://db_testStatement\")}" +
				"db=database(\"dfs://db_testStatement\",VALUE,`C`AMZON`IBM`GOOG`APPL`ORCL`XM);" +
				"pt=db.createTable(t,`pt).append!(t);" +
				"qt=db.createTable(t2,`qt).append!(t2);";
		db.run(script);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456"+"&databasePath=dfs://db_testStatement";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stm = (JDBCStatement) conn.createStatement();
			rs = (JDBCResultSet) stm.executeQuery("select sym,qty,price,timestamp,char,bool,minute from qt left outer join pt on pt.sym = qt.sym;");
			Map<String,Entity> map = new HashMap<>();
			BasicTable bt = (BasicTable) rs.getResult();
			System.out.println(bt.rows());
			map.put("JoinTable",bt);
			db.upload(map);
			Assert.assertEquals(0,db.run("select * from JoinTable where sym=`XM").rows());
            JDBCResultSet jr = (JDBCResultSet) stm.executeQuery("select sym,qty,price,timestamp,char,bool,minute from qt outer join pt on pt.sym = qt.sym");
			System.out.println(jr.getResult().rows());
			JDBCResultSet je = (JDBCResultSet) stm.executeQuery("select Count(1) from pt");
			System.out.println(je.getResult().getString());
			JDBCResultSet jd = (JDBCResultSet) stm.executeQuery("select sym,char,bool,minute,price,qty,timestamp from qt right outer join pt on qt.sym = pt.sym");
			Map<String,Entity> map2 = new HashMap<>();
			BasicTable bt2 = (BasicTable) jd.getResult();
			map2.put("RJTable",bt2);
			db.upload(map2);
			Assert.assertEquals(7143,db.run("select * from RJTable where sym=`XM").rows());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(rs !=null) {
				try {
					rs.close();
				}catch(SQLException e) {
					e.printStackTrace();
				}
			}
			if(stm!= null) {
				try {
					stm.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			if(conn !=null) {
				try {
					conn.close();
					db.close();
				}catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

}
