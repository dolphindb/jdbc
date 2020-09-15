
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
        HOST = "localhost" ;
        PORT = 8848 ;
    }
	
    public static boolean CreateDfsTable(String host, Integer port){
    	boolean success = false;
    	DBConnection db = null;
    	try{
    		String script = "login(`admin, `123456); \n"+
    				"if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')}\n" + 
    				"t=table(`C`MS`MS`MS`IBM`IBM`C`C`C as sym,"
    				+ "49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 as price"
    				+ ",2200 1900 2100 3200 6800 5400 1300 2500 8800 as qty, "
    				+ "[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12] as timestamp)\n" + 
    				"db=database('dfs://db_testStatement', RANGE, 0 3000 9000)\n" + 
    				"pt=db.createPartitionedTable(t, `pt, `qty).append!(t)";
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
	    String s = "Can not issue SELECT via executeUpdate()";
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
    		stmt.execute("update pt set qty=qty+100");
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
    		org.junit.Assert.assertEquals(175.23, rs4.getDouble(2),0);
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
	
	
	
}
