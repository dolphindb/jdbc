
import com.dolphindb.jdbc.JDBCResultSet;
import com.dolphindb.jdbc.JDBCStatement;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import org.junit.*;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.CoreMatchers.containsString;

import java.awt.List;
import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.*;
import java.util.logging.Logger;

import com.xxdb.DBConnection;

public class JDBCStatementTest {
	static String HOST = JDBCTestUtil.HOST ;
	static int PORT = JDBCTestUtil.PORT ;
	String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
	String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
	Connection conn = null;
	Statement stmt = null;
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
	public static boolean createMemoryTable(String dataType){
		boolean success = false;
		DBConnection db = null;
		try{
			String script = "login(`admin, `123456); \n"+
					"try{undef(`tt,SHARED)}catch(ex){};\n" +
					"share table(10:0,`id`dataType,[INT,"+dataType+"])  as tt;\n";
			db = new DBConnection();
			db.connect(HOST, PORT,"admin","123456");
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
	public static boolean createMemoryTable1() {
		boolean success = false;
		DBConnection db = null;
		try{
			String script = "login(`admin, `123456); \n"+
					"colNames=\"col\"+string(1..29);\n" +
					"colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
					"share table(1:0,colNames,colTypes) as tt;\n" ;
			db = new DBConnection();
			db.connect(HOST, PORT,"admin","123456");
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

	public static boolean createMemoryTable_Array() {
		boolean success = false;
		DBConnection db = null;
		try{
			String script = "login(`admin, `123456); \n"+
                    "colNames=\"col\"+string(1..26);\n" +
					//"colNames=\"col\"+string(1..2);\n" +
					//"colTypes=[INT,"+dataType+"[]];\n" +
                    "colTypes=[INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],UUID[],DATEHOUR[],IPADDR[],INT128[],COMPLEX[],POINT[],DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(19)[]];\n" +
					"share table(1:0,colNames,colTypes) as tt;\n" ;
			db = new DBConnection();
			db.connect(HOST, PORT,"admin","123456");
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
	public static boolean createWideTable(int intColNum, int doubleColNum) throws IOException {
		boolean success = false;
		DBConnection db = null;
		try{
			db = new DBConnection();
			db.connect(HOST, PORT,"admin","123456");
			StringBuilder colNames = new StringBuilder("`time`id");
			StringBuilder colTypes = new StringBuilder("`TIMESTAMP`SYMBOL");
			for(int i = 0; i < intColNum; ++i) {
				colNames.append("`int_" + i);
				colTypes.append("`INT");
			}
			for(int i = 0; i < doubleColNum; ++i) {
				colNames.append("`double_" + i);
				colTypes.append("`DOUBLE");
			}
			System.out.println(colNames.toString());
			System.out.println(colTypes.toString());
			String script = "login(`admin, `123456); \n"+
					"share table(1:0," + colNames + "," + colTypes + ") as tt;\n";
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
			org.junit.Assert.assertEquals(rs1,-2);
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
	    	int[] expected = {-2,-2,-2,0};
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
    		org.junit.Assert.assertEquals(-2, rs);
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
			org.junit.Assert.assertEquals(-2, rs);
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

	//@Test(timeout = 120000)
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

	//@Test(timeout = 120000)
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
	@Test
	public void test_JDBCStatement_setMaxRows_negative() throws Exception {
		DBConnection db1 = new DBConnection();
		db1.connect(HOST,PORT,"admin","123456");
		Assert.assertTrue(CreateDfsTable(HOST,PORT));
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456"+"&databasePath=dfs://db_testStatement";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stm = (JDBCStatement) conn.createStatement();
		String re = null;
		try {
			stm.setMaxRows(-1);
		}catch(Exception e){
			re = e.getMessage();
		}
		Assert.assertEquals("The param max cannot less than 0.",re);
	}
	@Test
	public void test_JDBCStatement_setMaxRows_0() throws Exception {
		DBConnection db1 = new DBConnection();
		db1.connect(HOST,PORT,"admin","123456");
		Assert.assertTrue(CreateDfsTable(HOST,PORT));
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456"+"&databasePath=dfs://db_testStatement";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stm = (JDBCStatement) conn.createStatement();
		stm.setMaxRows(0);
		rs = (JDBCResultSet) stm.executeQuery("select sym,qty,price,timestamp,char,bool,minute from qt left outer join pt on qt.sym = pt.sym;");
		BasicTable bt = (BasicTable) rs.getResult();
		Assert.assertEquals(12,bt.rows());
	}
	@Test
	public void test_JDBCStatement_setMaxRows_5() throws Exception {
		DBConnection db1 = new DBConnection();
		db1.connect(HOST,PORT,"admin","123456");
		Assert.assertTrue(CreateDfsTable(HOST,PORT));
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456"+"&databasePath=dfs://db_testStatement";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stm = (JDBCStatement) conn.createStatement();
		stm.setMaxRows(5);
		rs = (JDBCResultSet) stm.executeQuery("select sym,qty,price,timestamp,char,bool,minute from qt left outer join pt on qt.sym = pt.sym;");
		BasicTable bt = (BasicTable) rs.getResult();
		Assert.assertEquals(5,bt.rows());
	}
	@Test
	public void test_JDBCStatement_setMaxRows_1000000() throws Exception {
		DBConnection db1 = new DBConnection();
		db1.connect(HOST,PORT,"admin","123456");
		Assert.assertTrue(CreateDfsTable(HOST,PORT));
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456"+"&databasePath=dfs://db_testStatement";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stm = (JDBCStatement) conn.createStatement();
		stm.setMaxRows(1000000);
		rs = (JDBCResultSet) stm.executeQuery("select sym,qty,price,timestamp,char,bool,minute from qt left outer join pt on qt.sym = pt.sym;");
		BasicTable bt = (BasicTable) rs.getResult();
		Assert.assertEquals(12,bt.rows());
	}
	@Test
	public void test_JDBCStatement_getMaxRows_0() throws Exception {
		DBConnection db1 = new DBConnection();
		db1.connect(HOST,PORT,"admin","123456");
		Assert.assertTrue(CreateDfsTable(HOST,PORT));
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456"+"&databasePath=dfs://db_testStatement";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stm = (JDBCStatement) conn.createStatement();
		Assert.assertEquals(0,stm.getMaxRows());
		stm.setMaxRows(0);
		Assert.assertEquals(0,stm.getMaxRows());

	}
	@Test
	public void test_JDBCStatement_getMaxRows_5() throws Exception {
		DBConnection db1 = new DBConnection();
		db1.connect(HOST,PORT,"admin","123456");
		Assert.assertTrue(CreateDfsTable(HOST,PORT));
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456"+"&databasePath=dfs://db_testStatement";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stm = (JDBCStatement) conn.createStatement();
		Assert.assertEquals(0,stm.getMaxRows());
		stm.setMaxRows(5);
		Assert.assertEquals(5,stm.getMaxRows());
	}
	@Test
	public void test_JDBCStatement_getMaxRows_1000000() throws Exception {
		DBConnection db1 = new DBConnection();
		db1.connect(HOST,PORT,"admin","123456");
		Assert.assertTrue(CreateDfsTable(HOST,PORT));
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456"+"&databasePath=dfs://db_testStatement";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stm = (JDBCStatement) conn.createStatement();
		Assert.assertEquals(0,stm.getMaxRows());
		stm.setMaxRows(1000000);
		Assert.assertEquals(1000000,stm.getMaxRows());
	}
	@Test
	public void test_JDBCStatement_setMaxRows_setFetchSize_1() throws Exception {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(take(`C`AMZON`IBM`XM`GOOG`APPL`ORCL,50000) as sym," +
				"rand(198.99,50000) as price," +
				"take(1..2000,50000) as qty, " +
				"take(01:01:01..23:59:59,50000) as timestamp)" +
				"share t as st";
		db.run(script);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stm = (JDBCStatement) conn.createStatement();
		stm.setMaxRows(20000);
		stm.setFetchSize(30000);
		rs = (JDBCResultSet) stm.executeQuery("select * from st;");
		BasicTable bt = (BasicTable) rs.getResult();
		Assert.assertEquals(20000,bt.rows());
		int flag = 0;
		while(rs.next()){
			flag++;
		}
		Assert.assertEquals(20000,flag);
		Assert.assertEquals(20000,stm.getMaxRows());
	}
	@Test
	public void test_JDBCStatement_setMaxRows_setFetchSize_2() throws Exception {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(take(`C`AMZON`IBM`XM`GOOG`APPL`ORCL,50000) as sym," +
				"rand(198.99,50000) as price," +
				"take(1..2000,50000) as qty, " +
				"take(01:01:01..23:59:59,50000) as timestamp)" +
				"share t as st";
		db.run(script);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stm = (JDBCStatement) conn.createStatement();
		stm.setMaxRows(20000);
		stm.setFetchSize(10000);
		rs = (JDBCResultSet) stm.executeQuery("select * from st;");
		BasicTable bt = (BasicTable) rs.getResult();
		Assert.assertEquals(10000,bt.rows());
		int flag = 0;
		while(rs.next()){
			flag++;
		}
		Assert.assertEquals(20000,flag);
		Assert.assertEquals(20000,stm.getMaxRows());
	}

	@Test
	public void test_JDBCStatement_setMaxRows_setFetchSize_3() throws Exception {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(take(`C`AMZON`IBM`XM`GOOG`APPL`ORCL,50000) as sym," +
				"rand(198.99,50000) as price," +
				"take(1..2000,50000) as qty, " +
				"take(01:01:01..23:59:59,50000) as timestamp)" +
				"share t as st";
		db.run(script);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stm = (JDBCStatement) conn.createStatement();
		stm.setMaxRows(20000);
		stm.setFetchSize(20000);
		rs = (JDBCResultSet) stm.executeQuery("select * from st;");
		BasicTable bt = (BasicTable) rs.getResult();
		Assert.assertEquals(20000,bt.rows());
		int flag = 0;
		while(rs.next()){
			flag++;
		}
		Assert.assertEquals(20000,flag);
		Assert.assertEquals(20000,stm.getMaxRows());
	}
	@Test
	public void test_JDBCStatement_setMaxRows_setFetchSize_4() throws Exception {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(take(`C`AMZON`IBM`XM`GOOG`APPL`ORCL,50000) as sym," +
				"rand(198.99,50000) as price," +
				"take(1..2000,50000) as qty, " +
				"take(01:01:01..23:59:59,50000) as timestamp)" +
				"share t as st";
		db.run(script);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stm = (JDBCStatement) conn.createStatement();
		stm.setMaxRows(20000);
		stm.setFetchSize(8192);
		rs = (JDBCResultSet) stm.executeQuery("select * from st;");
		BasicTable bt = (BasicTable) rs.getResult();
		//Assert.assertEquals(20000,bt.rows());
		int flag = 0;
		while(rs.next()){
			flag++;
		}
		Assert.assertEquals(20000,flag);
		Assert.assertEquals(20000,stm.getMaxRows());
	}
	@Test
	public void test_JDBCStatement_setFetchSize() throws Exception {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(take(`C`AMZON`IBM`XM`GOOG`APPL`ORCL,50000) as sym," +
				"rand(198.99,50000) as price," +
				"take(1..2000,50000) as qty, " +
				"take(01:01:01..23:59:59,50000) as timestamp)" +
				"share t as st";
		db.run(script);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		JDBCStatement stm = null;
		JDBCResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stm = (JDBCStatement) conn.createStatement();
//		stm.setMaxRows(20000);
		stm.setFetchSize(10000);
		rs = (JDBCResultSet) stm.executeQuery("select * from st;");
		BasicTable bt = (BasicTable) rs.getResult();
		Assert.assertEquals(10000,bt.rows());
		int flag = 0;
		while(rs.next()){
			flag++;
		}
		Assert.assertEquals(50000,flag);
	}

	@Test
	public void test_execute_insert_into_Boolean() throws SQLException, ClassNotFoundException {
		createMemoryTable("BOOL");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1,true)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getBoolean("dataType"), true);
		rs.next();
		rs.getBoolean("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Int() throws SQLException, ClassNotFoundException {
		createMemoryTable("INT");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1,100)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
		rs.next();
		rs.getInt("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_Int_1() throws SQLException, ClassNotFoundException {
		createMemoryTable("INT");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("INSERt inTO tt Values(1,100)");
		stmt.execute("inserT iNto tt valueS(2,NULL)");
		ResultSet rs = stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
		rs.next();
		rs.getInt("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_Int_2() throws SQLException, ClassNotFoundException {
		createMemoryTable("INT");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(id,dataType) values(1,100)");
		stmt.execute("insert into tt(id,dataType) values(2,NULL)");
		ResultSet rs = stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
		rs.next();
		rs.getInt("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_Int_3() throws SQLException, ClassNotFoundException {
		createMemoryTable("INT");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt (id,dataType) values(1,100)");
		stmt.execute("insert into tt (id,dataType) values(2,NULL)");
		ResultSet rs = stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
		rs.next();
		rs.getInt("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_Int_line_break() throws SQLException, ClassNotFoundException {
		createMemoryTable("INT");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert  into \ntt (id,dataType) \nvalues(1,100);\n");
		stmt.execute("\ninsert into\n tt \n(id,dataType)\n values(2,NULL);");
		ResultSet rs = stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
		rs.next();
		rs.getInt("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_Char() throws SQLException, ClassNotFoundException {
		createMemoryTable("CHAR");
		createMemoryTable("CHAR");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1,'1')");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(49,rs.getByte("dataType"));
		rs.next();
		rs.getByte("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Short() throws SQLException, ClassNotFoundException {
		createMemoryTable("SHORT");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1,12)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getShort("dataType"), 12);
		rs.next();
		rs.getShort("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Long() throws SQLException, ClassNotFoundException {
		createMemoryTable("LONG");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1,12)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getLong("dataType"), 12);
		rs.next();
		rs.getLong("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Float() throws SQLException, ClassNotFoundException {
		createMemoryTable("FLOAT");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1,12.23)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("dataType"),4);
		rs.next();
		rs.getFloat("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Double() throws SQLException, ClassNotFoundException {
		createMemoryTable("DOUBLE");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1,12.23)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("dataType"),4);
		rs.next();
		rs.getDouble("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_String() throws SQLException, ClassNotFoundException {
		createMemoryTable("STRING");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1,\"test1\")");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("test1",rs.getString("dataType"));
		rs.next();
		rs.getString("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Symbol() throws SQLException, ClassNotFoundException {
		createMemoryTable("SYMBOL");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1,\"test1\")");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("test1",rs.getString("dataType"));
		rs.next();
		rs.getString("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Date() throws SQLException, ClassNotFoundException {
		createMemoryTable("DATE");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, 2021.01.01)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("dataType"));
		rs.next();
		rs.getDate("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Month() throws SQLException, ClassNotFoundException {
		createMemoryTable("MONTH");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, 2021.01M)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("dataType"));
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Time() throws SQLException, ClassNotFoundException {
		createMemoryTable("TIME");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, 01:01:01.010)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		System.out.println();
		org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1,010000000)),rs.getTime("dataType"));
		rs.next();
		rs.getTime("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Minute() throws SQLException, ClassNotFoundException {
		createMemoryTable("MINUTE");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, 01:01m)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("dataType"));
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Second() throws SQLException, ClassNotFoundException {
		createMemoryTable("SECOND");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, 13:30:10)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(LocalTime.of(13,30,10),rs.getObject("dataType"));
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Datetime() throws SQLException, ClassNotFoundException {
		createMemoryTable("DATETIME");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, 2011.01.01 01:01:01)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("2011-01-01T01:01:01",rs.getObject("dataType").toString());
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Timestamp() throws SQLException, ClassNotFoundException {
		createMemoryTable("TIMESTAMP");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, 2011.01.01 01:01:01.001)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("2011-01-01T01:01:01.001",rs.getObject("dataType").toString());
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Nanotime() throws SQLException, ClassNotFoundException {
		createMemoryTable("NANOTIME");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, 01:01:01.001000000)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("01:01:01.001",rs.getObject("dataType").toString());
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_Nanotimestamp() throws SQLException, ClassNotFoundException {
		createMemoryTable("NANOTIMESTAMP");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, 2021.01.01 01:01:01.001123456)");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("2021-01-01T01:01:01.001123456",rs.getObject("dataType").toString());
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Datehour() throws SQLException, ClassNotFoundException {
		createMemoryTable("DATEHOUR");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, datehour(2021.01.01 01:01:01.001123456))");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("dataType"));
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Uuid() throws SQLException, ClassNotFoundException {
		createMemoryTable("UUID");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, uuid(\"00000000-0000-0001-0000-000000000002\"))");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("dataType"));
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Ipaddr() throws SQLException, ClassNotFoundException {
		createMemoryTable("IPADDR");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, ipaddr(\"0::1:0:0:0:2\"))");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("dataType"));
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Int128() throws SQLException, ClassNotFoundException {
		createMemoryTable("INT128");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, int128(\"00000000000000010000000000000002\"))");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("dataType"));
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Blob() throws SQLException, ClassNotFoundException {
		createMemoryTable("BLOB");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, \"TEST BLOB\")");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("TEST BLOB",rs.getString("dataType"));
		//getBlob还没有实现,等实现后替换getString
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Complex() throws SQLException, ClassNotFoundException {
		createMemoryTable("COMPLEX");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, complex(1,2));");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("dataType"));
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_Point() throws SQLException, ClassNotFoundException {
		createMemoryTable("POINT");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1, point(0.0, 0.0));");
		stmt.execute("insert into tt values(2,NULL);");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("dataType"));
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_Mul() throws SQLException, ClassNotFoundException {
		createMemoryTable("INT");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		for(int i =1;i<=10;i++) {
			stmt.execute("insert into tt values(" + i + ", " + i*100 + ")");
		}
		ResultSet rs = (ResultSet)stmt.executeQuery("select count(*) from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getInt("count"), 10);
	}
	@Test
	public void test_execute_insert_into_Decimal32() throws SQLException, ClassNotFoundException {
		createMemoryTable("DECIMAL32(4)");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1000, decimal32(123421.00012,4))");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("123421.0001",rs.getObject("dataType").toString());
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_Decimal64() throws SQLException, ClassNotFoundException {
		createMemoryTable("DECIMAL64(4)");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1000, decimal64(123421.00012,4))");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("123421.0001",rs.getObject("dataType").toString());
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_Decimal128() throws SQLException, ClassNotFoundException {
		createMemoryTable("DECIMAL128(4)");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1000, decimal128(123421.00012,4))");
		stmt.execute("insert into tt values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("123421.0001",rs.getObject("dataType").toString());
		rs.next();
		rs.getObject("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_col_Boolean() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col2) values(1,true)");
		stmt.execute("insert into tt(col1,col2) values(2,NULL)");
		ResultSet rs = stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
		rs.next();
		rs.getBoolean("col2");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_col_Char() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col3) values(1,'1')");
		stmt.execute("insert into tt(col1,col3) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(49, rs.getByte("col3"));
		rs.next();
		rs.getByte("col3");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Short() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col4) values(1,12)");
		stmt.execute("insert into tt(col1,col4) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
		rs.next();
		rs.getShort("col4");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Int() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col5) values(1,100)");
		stmt.execute("insert into tt(col1,col5) values(2,NULL)");
		ResultSet rs = stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
		rs.next();
		rs.getInt("col5");
		org.junit.Assert.assertTrue(rs.wasNull());
	}


	@Test
	public void test_execute_insert_into_col_Long() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col6) values(1,12)");
		stmt.execute("insert into tt(col1,col6) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
		rs.next();
		rs.getLong("col6");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_col_Date() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col7) values(1, 2021.01.01)");
		stmt.execute("insert into tt(col1,col7) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
		rs.next();
		rs.getDate("col7");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Month() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col8) values(1, 2021.01M)");
		stmt.execute("insert into tt(col1,col8) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
		rs.next();
		rs.getObject("col8");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Time() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col9) values(1, 01:01:01.010)");
		stmt.execute("insert into tt(col1,col9) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
		rs.next();
		rs.getTime("col9");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Minute() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col10) values(1, 01:01m)");
		stmt.execute("insert into tt(col1,col10) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
		rs.next();
		rs.getObject("col10");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Second() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col11) values(1, 13:30:10)");
		stmt.execute("insert into tt(col1,col11) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(LocalTime.of(13,30,10),rs.getObject("col11"));
		rs.next();
		rs.getObject("col11");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Datetime() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col12) values(1, 2011.01.01 01:01:01)");
		stmt.execute("insert into tt(col1,col12) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(LocalDateTime.of(2011,1,1,1,1,1),rs.getObject("col12"));
		rs.next();
		rs.getObject("col12");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Timestamp() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col13) values(1, 2011.01.01 01:01:01.100)");
		stmt.execute("insert into tt(col1,col13) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("2011-01-01T01:01:01.100",rs.getObject("col13").toString());
		rs.next();
		rs.getObject("col13");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Nanotime() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col14) values(1, 01:01:01.001000022)");
		stmt.execute("insert into tt(col1,col14) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("01:01:01.001000022",rs.getObject("col14").toString());
		rs.next();
		rs.getObject("col14");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Nanotimestamp() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col15) values(1, 2021.01.01 01:01:01.001123456)");
		stmt.execute("insert into tt(col1,col15) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("2021-01-01T01:01:01.001123456",rs.getObject("col15").toString());
		rs.next();
		rs.getObject("col15");
		org.junit.Assert.assertTrue(rs.wasNull());
	}


	@Test
	public void test_execute_insert_into_col_Float() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col16) values(1,12.23)");
		stmt.execute("insert into tt(col1,col16) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
		rs.next();
		rs.getFloat("col16");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Double() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col17) values(1,12.23)");
		stmt.execute("insert into tt(col1,col17) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
		rs.next();
		rs.getDouble("col17");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_col_Symbol() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col18) values(1,\"test1\")");
		stmt.execute("insert into tt(col1,col18) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("test1",rs.getString("col18"));
		rs.next();
		rs.getString("col18");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_String() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col19) values(1,\"test1\")");
		stmt.execute("insert into tt(col1,col19) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("test1",rs.getString("col19"));
		rs.next();
		rs.getString("col19");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_col_Uuid() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col20) values(1, uuid(\"00000000-0000-0001-0000-000000000002\"))");
		stmt.execute("insert into tt(col1,col20) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
		rs.next();
		rs.getObject("col20");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Datehour() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col21) values(1, datehour(2021.01.01 01:01:01.001123456))");
		stmt.execute("insert into tt(col1,col21) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
		rs.next();
		rs.getObject("col21");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Ipaddr() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col22) values(1, ipaddr(\"0::1:0:0:0:2\"))");
		stmt.execute("insert into tt(col1,col22) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
		rs.next();
		rs.getObject("col22");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Int128() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col23) values(1, int128(\"00000000000000010000000000000002\"))");
		stmt.execute("insert into tt(col1,col23) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
		rs.next();
		rs.getObject("col23");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Blob() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col24) values(1, \"TEST BLOB\")");
		stmt.execute("insert into tt(col1,col24) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
		//getBlob还没有实现,等实现后替换getString
		rs.next();
		rs.getObject("col24");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Complex() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col25) values(1, complex(1,2));");
		stmt.execute("insert into tt(col1,col25) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
		rs.next();
		rs.getObject("col25");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Point() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col26) values(1, point(0.0, 0.0));");
		stmt.execute("insert into tt(col1,col26) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
		rs.next();
		rs.getObject("col26");
		org.junit.Assert.assertTrue(rs.wasNull());
	}

	@Test
	public void test_execute_insert_into_col_Decimal32() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col27) values(1000, decimal32(123421.00012,4))");
		stmt.execute("insert into tt(col1,col27) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("123421.00",rs.getObject("col27").toString());
		rs.next();
		rs.getObject("col27");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_col_Decimal64() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col28) values(1000, decimal64(123421.00012,4))");
		stmt.execute("insert into tt(col1,col28) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("123421.0001000",rs.getObject("col28").toString());
		rs.next();
		rs.getObject("col28");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_col_Decimal128() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col29) values(1000, decimal128(123421.00012,4))");
		stmt.execute("insert into tt(col1,col29) values(2,NULL)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals("123421.0001000000000000000",rs.getObject("col29").toString());
		rs.next();
		rs.getObject("col29");
		org.junit.Assert.assertTrue(rs.wasNull());
	}
	@Test
	public void test_execute_insert_into_memoryTable_all_dateType1() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt values(1,true,'1',12,100,12,2021.01.01,2021.01M,01:01:01.000,01:01m,01:01:01,2021.01.01 01:01:01,2021.01.01 01:01:01.001,01:01:01.001,2021.01.01 01:01:01.000123456,12.23,12.23,\"test1\",\"test1\",uuid(\"00000000-0000-0001-0000-000000000002\"),datehour(2021.01.01 01:01:01),ipaddr(\"0::1:0:0:0:2\"),int128(\"00000000000000010000000000000002\"),\"TEST BLOB\",complex(1.0,2.0),point(0.0, 0.0),decimal32(1421.00012,5),decimal64(1421.00012,5),decimal128(1421.00012,5))");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
		org.junit.Assert.assertEquals(rs.getByte("col3"), 49);
		org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
		org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
		org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
		org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
		org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
		org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
		org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
		org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("col11"));
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col12"));
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,1000000),rs.getObject("col13"));
		org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1000000),rs.getObject("col14"));
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("col15"));
		org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
		org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
		org.junit.Assert.assertEquals("test1",rs.getString("col18"));
		org.junit.Assert.assertEquals("test1",rs.getString("col19"));
		org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
		org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
		org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
		org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
		org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
		org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
		org.junit.Assert.assertEquals("1421.00",rs.getObject("col27").toString());
		org.junit.Assert.assertEquals("1421.0001200",rs.getObject("col28").toString());
		org.junit.Assert.assertEquals("1421.0001200000000000000",rs.getObject("col29").toString());
	}

	@Test
	public void test_execute_insert_into_memoryTable_all_dateType_2() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28,col29) values(1,true,'1',12,100,12,2021.01.01,2021.01M,01:01:01.000,01:01m,01:01:01,2021.01.01 01:01:01,2021.01.01 01:01:01.001,01:01:01.001,2021.01.01 01:01:01.000123456,12.23,12.23,\"test1\",\"test1\",uuid(\"00000000-0000-0001-0000-000000000002\"),datehour(2021.01.01 01:01:01),ipaddr(\"0::1:0:0:0:2\"),int128(\"00000000000000010000000000000002\"),\"TEST BLOB\",complex(1.0,2.0),point(0.0, 0.0),decimal32(1421.00012,5),decimal64(1421.00012,5),decimal128(1421.00012,5))");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
		org.junit.Assert.assertEquals(rs.getByte("col3"), 49);
		org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
		org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
		org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
		org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
		org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
		org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
		org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
		org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("col11"));
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col12"));
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,1000000),rs.getObject("col13"));
		org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1000000),rs.getObject("col14"));
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("col15"));
		org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
		org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
		org.junit.Assert.assertEquals("test1",rs.getString("col18"));
		org.junit.Assert.assertEquals("test1",rs.getString("col19"));
		org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
		org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
		org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
		org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
		org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
		org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
		org.junit.Assert.assertEquals("1421.00",rs.getObject("col27").toString());
		org.junit.Assert.assertEquals("1421.0001200",rs.getObject("col28").toString());
		org.junit.Assert.assertEquals("1421.0001200000000000000",rs.getObject("col29").toString());
	}

	@Test
	public void test_execute_insert_into_memoryTable_all_dateType_3() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col21,col22,col23,col24,col25,col26,col27,col28,col29,col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20) values(datehour(2021.01.01 01:01:01),ipaddr(\"0::1:0:0:0:2\"),int128(\"00000000000000010000000000000002\"),\"TEST BLOB\",complex(1.0,2.0),point(0.0, 0.0),decimal32(1421.00012,5),decimal64(1421.00012,5),decimal128(1421.00012,5),1,true,'1',12,100,12,2021.01.01,2021.01M,01:01:01.000,01:01m,01:01:01,2021.01.01 01:01:01,2021.01.01 01:01:01.001,01:01:01.001,2021.01.01 01:01:01.000123456,12.23,12.23,\"test1\",\"test1\",uuid(\"00000000-0000-0001-0000-000000000002\"))");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
		org.junit.Assert.assertEquals(rs.getByte("col3"), 49);
		org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
		org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
		org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
		org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
		org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
		org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
		org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
		org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("col11"));
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col12"));
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,1000000),rs.getObject("col13"));
		org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1000000),rs.getObject("col14"));
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("col15"));
		org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
		org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
		org.junit.Assert.assertEquals("test1",rs.getString("col18"));
		org.junit.Assert.assertEquals("test1",rs.getString("col19"));
		org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
		org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
		org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
		org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
		org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
		org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
		org.junit.Assert.assertEquals("1421.00",rs.getObject("col27").toString());
		org.junit.Assert.assertEquals("1421.0001200",rs.getObject("col28").toString());
		org.junit.Assert.assertEquals("1421.0001200000000000000",rs.getObject("col29").toString());
	}
	@Test
	public void test_execute_insert_into_memoryTable_all_dateType_4() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col21,col22,col23,col24,col25,col26,col27,col28,col29,col1 ) values(datehour(2021.01.01 01:01:01),ipaddr(\"0::1:0:0:0:2\"),int128(\"00000000000000010000000000000002\"),\"TEST BLOB\",complex(1.0,2.0),point(0.0, 0.0),decimal32(1421.00012,5),decimal64(1421.00012,5),decimal128(1421.00012,5),1)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
		org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
		org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
		org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
		org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
		org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
		org.junit.Assert.assertEquals("1421.00",rs.getObject("col27").toString());
		org.junit.Assert.assertEquals("1421.0001200",rs.getObject("col28").toString());
		org.junit.Assert.assertEquals("1421.0001200000000000000",rs.getObject("col29").toString());
	}
	@Test
	public void test_execute_insert_into_memoryTable_all_dateType_5() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("INSERT INTO tt (col21,col22,col23,col24,col25,col26,col27,col28,col29,col1 ) VALUES (datehour(2021.01.01 01:01:01),ipaddr(\"0::1:0:0:0:2\"),int128(\"00000000000000010000000000000002\"),\"TEST BLOB\",complex(1.0,2.0),point(0.0, 0.0),decimal32(1421.00012,5),decimal64(1421.00012,5),decimal128(1421.00012,5),1)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
		org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
		org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
		org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
		org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
		org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
		org.junit.Assert.assertEquals("1421.00",rs.getObject("col27").toString());
		org.junit.Assert.assertEquals("1421.0001200",rs.getObject("col28").toString());
		org.junit.Assert.assertEquals("1421.0001200000000000000",rs.getObject("col29").toString());
	}
	@Test
	public void test_execute_insert_into_memoryTable_all_dateType_6() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28,col29)  values(1,,,,,,,,,,,,,,,,,,,,,,,,,,,,)");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(1,rs.getObject("col1"));
		org.junit.Assert.assertNull(rs.getObject("col2") );
		org.junit.Assert.assertNull(rs.getObject("col3") );
		org.junit.Assert.assertNull(rs.getObject("col4"));
		org.junit.Assert.assertNull(rs.getObject("col5"));
		org.junit.Assert.assertNull(rs.getObject("col6"));
		org.junit.Assert.assertNull(rs.getObject("col7"));
		org.junit.Assert.assertNull(rs.getObject("col8"));
		org.junit.Assert.assertNull(rs.getObject("col9"));
		org.junit.Assert.assertNull(rs.getObject("col10"));
		org.junit.Assert.assertNull(rs.getObject("col11"));
		org.junit.Assert.assertNull(rs.getObject("col12"));
		org.junit.Assert.assertNull(rs.getObject("col13"));
		org.junit.Assert.assertNull(rs.getObject("col14"));
		org.junit.Assert.assertNull(rs.getObject("col15"));
		org.junit.Assert.assertNull(rs.getObject("col16"));
		org.junit.Assert.assertNull(rs.getObject("col17"));
		org.junit.Assert.assertNull(rs.getString("col18"));
		org.junit.Assert.assertNull(rs.getString("col19"));
		org.junit.Assert.assertNull(rs.getObject("col20"));
		org.junit.Assert.assertNull(rs.getObject("col21"));
		org.junit.Assert.assertEquals("0.0.0.0",rs.getObject("col22").toString());
		org.junit.Assert.assertNull(rs.getObject("col23"));
		org.junit.Assert.assertNull(rs.getString("col24"));
		org.junit.Assert.assertNull(rs.getObject("col25"));
		org.junit.Assert.assertEquals("(,)",rs.getObject("col26").toString());
		org.junit.Assert.assertNull(rs.getObject("col27"));
		org.junit.Assert.assertNull(rs.getObject("col28"));
		org.junit.Assert.assertNull(rs.getObject("col29"));

	}
	@Test
	public void test_execute_insert_into_memoryTable_all_dateType_mul() throws SQLException, ClassNotFoundException {
		createMemoryTable1();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		for(int i =1;i<=100000;i++) {
			stmt.execute("insert into tt values(" + i +",true,'1',12,100,12,2021.01.01,2021.01M,01:01:01.000,01:01m,01:01:01,2021.01.01 01:01:01,2021.01.01 01:01:01.001,01:01:01.001,2021.01.01 01:01:01.000123456,12.23,12.23,\"test1\",\"test1\",uuid(\"00000000-0000-0001-0000-000000000002\"),datehour(2021.01.01 01:01:01),ipaddr(\"0::1:0:0:0:2\"),int128(\"00000000000000010000000000000002\"),\"TEST BLOB\",complex(1.0,2.0),point(0.0, 0.0),decimal32(1421.00012,5),decimal64(1421.00012,5),decimal128(1421.00012,5))");
		}
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("select * from tt");
		org.junit.Assert.assertEquals(100000, rs.getResult().rows());
		while (rs.next()) {
			org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
			org.junit.Assert.assertEquals(rs.getByte("col3"), 49);
			org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
			org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
			org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
			org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
			org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
			org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
			org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
			org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("col11"));
			org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col12"));
			org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,1000000),rs.getObject("col13"));
			org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1000000),rs.getObject("col14"));
			org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("col15"));
			org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
			org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
			org.junit.Assert.assertEquals("test1",rs.getString("col18"));
			org.junit.Assert.assertEquals("test1",rs.getString("col19"));
			org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
			org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
			org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
			org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
			org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
			org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
			org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
			org.junit.Assert.assertEquals("1421.00",rs.getObject("col27").toString());
			org.junit.Assert.assertEquals("1421.0001200",rs.getObject("col28").toString());
			org.junit.Assert.assertEquals("1421.0001200000000000000",rs.getObject("col29").toString());
		}
	}
	@Test
	public void test_execute_insert_into_memoryTable_wideTable() throws SQLException, IOException, ClassNotFoundException {
		createWideTable(100,100);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt(time,id) values(2012.06.13 13:30:10.008, `1)");
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("select * from tt");
		org.junit.Assert.assertEquals(1, rs.getResult().rows());
		rs.next();
		for(int i =3;i<=202;i++) {
			org.junit.Assert.assertNull( rs.getObject(i));
		}
	}
	@Test
	public void test_execute_insert_into_memoryTable_arrayVector_all_dateType() throws SQLException, IOException, ClassNotFoundException {
		createMemoryTable_Array();
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "cbool = array(BOOL[]).append!(cut(take([true, false, NULL], 1000), 100))\n" +
				"cchar = array(CHAR[]).append!(cut(take(char(-100..100 join NULL), 1000), 100))\n" +
				"cshort = array(SHORT[]).append!(cut(take(short(-100..100 join NULL), 1000), 100))\n" +
				"cint = array(INT[]).append!(cut(take(-100..100 join NULL, 1000), 100))\n" +
				"clong = array(LONG[]).append!(cut(take(long(-100..100 join NULL), 1000), 100))\n" +
				"cdouble = array(DOUBLE[]).append!(cut(take(-100..100 join NULL, 1000) + 0.254, 100))\n" +
				"cfloat = array(FLOAT[]).append!(cut(take(-100..100 join NULL, 1000) + 0.254f, 100))\n" +
				"cdate = array(DATE[]).append!(cut(take(2012.01.01..2012.02.29, 1000), 100))\n" +
				"cmonth = array(MONTH[]).append!(cut(take(2012.01M..2013.12M, 1000), 100))\n" +
				"ctime = array(TIME[]).append!(cut(take(09:00:00.000 + 0..99 * 1000, 1000), 100))\n" +
				"cminute = array(MINUTE[]).append!(cut(take(09:00m..15:59m, 1000), 100))\n" +
				"csecond = array(SECOND[]).append!(cut(take(09:00:00 + 0..999, 1000), 100))\n" +
				"cdatetime = array(DATETIME[]).append!(cut(take(2012.01.01T09:00:00 + 0..999, 1000), 100))\n" +
				"ctimestamp = array(TIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000 + 0..999 * 1000, 1000), 100))\n" +
				"cnanotime =array(NANOTIME[]).append!(cut(take(09:00:00.000000000 + 0..999 * 1000000000, 1000), 100))\n" +
				"cnanotimestamp = array(NANOTIMESTAMP[]).append!(cut(take(2012.01.01T09:00:00.000000000 + 0..999 * 1000000000, 1000), 100))\n" +
				"cuuid = array(UUID[]).append!(cut(take(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\", \"5d212a78-cc48-e3b1-4235-b4d91473ee88\", \"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]), 1000), 100))\n" +
				"cdatehour = array(DATEHOUR[]).append!(cut(take(datehour(1..10 join NULL), 1000), 100))\n" +
				"cipaddr = array(IPADDR[]).append!(cut(take(ipaddr([\"192.168.100.10\", \"192.168.100.11\", \"192.168.100.14\", \"\"]), 1000), 100))\n" +
				"cint128 = array(INT128[]).append!(cut(take(int128([\"e1671797c52e15f763380b45e841ec32\", \"e1671797c52e15f763380b45e841ec33\", \"e1671797c52e15f763380b45e841ec35\", \"\"]), 1000), 100))\n" +
				"ccomplex = array(	COMPLEX[]).append!(cut(rand(complex(rand(100, 1000), rand(100, 1000)) join NULL, 1000), 100))\n" +
				"cpoint = array(POINT[]).append!(cut(rand(point(rand(100, 1000), rand(100, 1000)) join NULL, 1000), 100))\n" +
				"cdecimal32 = array(DECIMAL32(2)[]).append!(cut(decimal32(take(-100..100 join NULL, 1000) + 0.254, 3), 100))\n" +
				"cdecimal64 = array(DECIMAL64(7)[]).append!(cut(decimal64(take(-100..100 join NULL, 1000) + 0.25467, 4), 100))\n" +
				"cdecimal128 = array(DECIMAL128(19)[]).append!(cut(decimal128(take(-100..100 join NULL, 1000) + 0.25467, 5), 100))\n" +
				"data = table(cbool, cchar, cshort, cint, clong, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cfloat, cdouble, cuuid, cdatehour,cipaddr, cint128,  ccomplex,cpoint,cdecimal32,cdecimal64,cdecimal128)\n" +
				"insert into tt(col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26) values(cbool, cchar, cshort, cint, clong, cdate, cmonth, ctime, cminute, csecond, cdatetime, ctimestamp, cnanotime, cnanotimestamp, cfloat, cdouble, cuuid, cdatehour,cipaddr, cint128,  ccomplex, cpoint, cdecimal32, cdecimal64, cdecimal128);";
		stmt.execute(script);
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("select col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26 from tt");
		BasicTable re = (BasicTable) rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("select * from data");
		BasicTable re1 = (BasicTable) rs1.getResult();
		Assert.assertEquals(10, re.rows());
		for (int i = 0; i < 25; i++) {
			for (int j = 0; j < 10; j++) {
				Assert.assertEquals(re1.getColumn(i).get(j).getString(), re.getColumn(i).get(j).getString());
			}
		}
	}
	@Test
	public void test_execute_colume_no_case_limit() throws SQLException, ClassNotFoundException {
		createMemoryTable("INT");
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into tt (id,daTAType) values(1,100)");
		stmt.execute("insert into tt (id,DATATYPE) values(2,NULL)");
		ResultSet rs = stmt.executeQuery("select * from tt");
		rs.next();
		org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
		rs.next();
		rs.getInt("dataType");
		org.junit.Assert.assertTrue(rs.wasNull());
		stmt.execute("update tt set DATAType =101,ID = 11 where Id = 1");
		ResultSet rs1 = stmt.executeQuery("select * from tt");
		rs1.next();
		org.junit.Assert.assertEquals(rs1.getInt("dataType"), 101);
		org.junit.Assert.assertEquals(rs1.getInt("id"), 11);
		stmt.execute("delete from  tt where daTAType = 101 and ID = 11");
		JDBCResultSet rs2 = (JDBCResultSet)stmt.executeQuery("select * from tt");
		BasicTable re = (BasicTable)rs2.getResult();
		org.junit.Assert.assertEquals(1, re.rows());
	}
	@Test
	public void test_JDBCStatement_setFetchDirection() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.setFetchDirection(1000);
		int re = stmt.getFetchDirection();
		Assert.assertEquals(1000,re);
	}
	@Test
	public void test_JDBCStatement_setFetchDirection_not_support() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String re = null;
		try{
			stmt.setFetchDirection(1002);
		}catch(Exception ex){
			re = ex.getMessage();
		}
		Assert.assertEquals("DolpinDB JDBC Statement direction only suppport FETCH_FORWARD.",re);
	}
	@Test
	public void Test_JDBCStatement_dfs_execUpdate() throws Exception{
		boolean success = CreateDfsTable(HOST, PORT);
		org.junit.Assert.assertTrue(success);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.executeUpdate("update loadTable('dfs://db_testStatement', 'pt') set price=price+1");
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("select * from loadTable('dfs://db_testStatement', 'pt')");
		BasicTable re = (BasicTable)rs.getResult();
		Assert.assertEquals("[50.6,51.76,51.32,52.29,175.97,176.23,30.46,30.52,31.02]", re.getColumn(1).getString());
	}
	@Test
	public void Test_JDBCStatement_dfs_execUpdate_1() throws Exception{
		boolean success = CreateDfsTable(HOST, PORT);
		org.junit.Assert.assertTrue(success);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.executeUpdate("update loadTable(\"dfs://db_testStatement\", `pt) set price=price+1");
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("select * from loadTable('dfs://db_testStatement', 'pt')");
		BasicTable re = (BasicTable)rs.getResult();
		Assert.assertEquals("[50.6,51.76,51.32,52.29,175.97,176.23,30.46,30.52,31.02]", re.getColumn(1).getString());
	}
	@Test
	public void Test_JDBCStatement_dfs_execUpdate_2() throws Exception{
		boolean success = CreateDfsTable(HOST, PORT);
		org.junit.Assert.assertTrue(success);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.executeUpdate("update loadTable(\"dfs://db_testStatement\", `pt) set price=price+1,qty=NULL,timestamp=00:34:07 where sym in (\"IBM\",\"MS\");");
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("select * from loadTable('dfs://db_testStatement', 'pt')");
		BasicTable re = (BasicTable)rs.getResult();
		Assert.assertEquals("[49.6,50.76,50.32,51.29,175.97,176.23,30.46,30.52,31.02]", re.getColumn(1).getString());
		Assert.assertEquals("[2200,1300,2500,8800,,,,,]", re.getColumn(2).getString());
		Assert.assertEquals("[09:34:07,09:34:16,09:34:26,09:38:12,00:34:07,00:34:07,00:34:07,00:34:07,00:34:07]", re.getColumn(3).getString());
	}
	@Test
	public void Test_JDBCStatement_dfs_execute_1() throws Exception{
		boolean success = CreateDfsTable(HOST, PORT);
		org.junit.Assert.assertTrue(success);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("update loadTable(\"dfs://db_testStatement\", `pt) set price=price+1,qty=NULL,timestamp=00:34:07 where sym in (\"IBM\",\"MS\");");
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("select * from loadTable('dfs://db_testStatement', 'pt')");
		BasicTable re = (BasicTable)rs.getResult();
		Assert.assertEquals("[49.6,50.76,50.32,51.29,175.97,176.23,30.46,30.52,31.02]", re.getColumn(1).getString());
		Assert.assertEquals("[2200,1300,2500,8800,,,,,]", re.getColumn(2).getString());
		Assert.assertEquals("[09:34:07,09:34:16,09:34:26,09:38:12,00:34:07,00:34:07,00:34:07,00:34:07,00:34:07]", re.getColumn(3).getString());
	}
	@Test
	public void test_JDBCStatement_dfs_allDataType_executeUpdate() throws SQLException, IOException, ClassNotFoundException {
		DBConnection db = new DBConnection();
		db.connect(HOST, PORT,"admin","123456");
		db.run("colNames=\"col\"+string(1..28)\n" +
				"colTypes=[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(18)]\n" +
				"t=table(1:0,colNames,colTypes)\n" +
				"try{dropDatabase('dfs://test_allDataType')\n}catch(ex){}\n" +
				"db=database('dfs://test_allDataType', RANGE, -1000 0 1000,,'TSDB')\n"+
				"db.createPartitionedTable(t, `pt, `col4,,`col4) \n");
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.executeUpdate("insert into loadTable('dfs://test_allDataType','pt') values(true,'a',2h,2,22l,2012.12.06,2012.06M,12:30:00.008,12:30m,12:30:00,2012.06.12 12:30:00,2012.06.12 12:30:00.008,13:30:10.008007006,2012.06.13 13:30:10.008007006,2.1f,2.1,\"hello\",\"world\",uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\"),datehour(2012.06.13 13:30:10),ipaddr(\"192.168.1.253\"),int128(\"e1671797c52e15f763380b45e841ec32\"),blob(\"123\"),complex(111,1),point(1,2),decimal32(1.1,2),decimal64(1.1,7),decimal128(1.1,18)) ");
		stmt.executeUpdate(" update loadTable('dfs://test_allDataType','pt') set col1 = false ,col2='3' ,col3 =-2, col5=-100, col6=2012.12.07, col7=2012.07M, col8=13:30:00.008, col9=13:30m, col10=13:30:00, col11=2013.06.12 13:30:00, col12=2013.06.12 12:30:00.008, col13=14:30:10.008007006, col14=2013.06.13 13:30:10.008007006, col15=4.1f, col16=4.1, col17=\"hello2323\", col18=\"world2323\", col19=uuid(\"3d457e79-1bed-d6c2-3612-b0d31c1881f6\"), col20=datehour(2013.06.13 13:30:10), col21=ipaddr(\"192.168.0.253\"), col22=int128(\"e1221797c52e15f763380b45e841ec32\"), col23=blob(\"123fff\"), col24=complex(-111,-1), col25=point(-1,-2), col26=decimal32(-1.1,2), col27=decimal64(-1.1,7), col28=decimal128(-1.1,18) where col4 = 2");
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("select * from loadTable('dfs://test_allDataType', 'pt')");
		BasicTable re = (BasicTable)rs.getResult();
		Assert.assertEquals(1, re.rows());
		stmt.executeUpdate(" delete from  loadTable('dfs://test_allDataType','pt') where col1 = false ,col2='3' ,col3 =-2, col5=-100, col6=2012.12.07, col7=2012.07M, col8=13:30:00.008, col9=13:30m, col10=13:30:00, col11=2013.06.12 13:30:00, col12=2013.06.12 12:30:00.008, col13=14:30:10.008007006, col14=2013.06.13 13:30:10.008007006, col15=4.1f, col16=4.1, col17=\"hello2323\", col18=\"world2323\", col19=uuid(\"3d457e79-1bed-d6c2-3612-b0d31c1881f6\"), col20=datehour(2013.06.13 13:30:10), col21=ipaddr(\"192.168.0.253\"), col22=int128(\"e1221797c52e15f763380b45e841ec32\"), col23=blob(\"123fff\"), col24=complex(-111,-1), col25=point(-1,-2), col26=decimal32(-1.1,2), col27=decimal64(-1.1,7), col28=decimal128(-1.1,18)");
		JDBCResultSet rs2 = (JDBCResultSet)stmt.executeQuery("select * from loadTable('dfs://test_allDataType', 'pt')");
		BasicTable re2 = (BasicTable)rs2.getResult();
		Assert.assertEquals(0, re2.rows());
	}
	@Test
	public void test_JDBCStatement_dfs_allDataType_execute() throws SQLException, IOException, ClassNotFoundException {
		DBConnection db = new DBConnection();
		db.connect(HOST, PORT,"admin","123456");
		db.run("colNames=\"col\"+string(1..28)\n" +
				"colTypes=[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(18)]\n" +
				"t=table(1:0,colNames,colTypes)\n" +
				"try{dropDatabase('dfs://test_allDataType')\n}catch(ex){}\n" +
				"db=database('dfs://test_allDataType', RANGE, -1000 0 1000,,'TSDB')\n"+
				"db.createPartitionedTable(t, `pt, `col4,,`col4) \n");
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into loadTable('dfs://test_allDataType','pt') values(true,'a',2h,2,22l,2012.12.06,2012.06M,12:30:00.008,12:30m,12:30:00,2012.06.12 12:30:00,2012.06.12 12:30:00.008,13:30:10.008007006,2012.06.13 13:30:10.008007006,2.1f,2.1,\"hello\",\"world\",uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\"),datehour(2012.06.13 13:30:10),ipaddr(\"192.168.1.253\"),int128(\"e1671797c52e15f763380b45e841ec32\"),blob(\"123\"),complex(111,1),point(1,2),decimal32(1.1,2),decimal64(1.1,7),decimal128(1.1,18)) ");
		stmt.execute(" update loadTable('dfs://test_allDataType','pt') set col1 = false ,col2='3' ,col3 =-2, col5=-100, col6=2012.12.07, col7=2012.07M, col8=13:30:00.008, col9=13:30m, col10=13:30:00, col11=2013.06.12 13:30:00, col12=2013.06.12 12:30:00.008, col13=14:30:10.008007006, col14=2013.06.13 13:30:10.008007006, col15=4.1f, col16=4.1, col17=\"hello2323\", col18=\"world2323\", col19=uuid(\"3d457e79-1bed-d6c2-3612-b0d31c1881f6\"), col20=datehour(2013.06.13 13:30:10), col21=ipaddr(\"192.168.0.253\"), col22=int128(\"e1221797c52e15f763380b45e841ec32\"), col23=blob(\"123fff\"), col24=complex(-111,-1), col25=point(-1,-2), col26=decimal32(-1.1,2), col27=decimal64(-1.1,7), col28=decimal128(-1.1,18) where col4 = 2");
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("select * from loadTable('dfs://test_allDataType', 'pt')");
		BasicTable re = (BasicTable)rs.getResult();
		Assert.assertEquals(1, re.rows());
		stmt.execute(" delete from  loadTable('dfs://test_allDataType','pt') where col1 = false ,col2='3' ,col3 =-2, col5=-100, col6=2012.12.07, col7=2012.07M, col8=13:30:00.008, col9=13:30m, col10=13:30:00, col11=2013.06.12 13:30:00, col12=2013.06.12 12:30:00.008, col13=14:30:10.008007006, col14=2013.06.13 13:30:10.008007006, col15=4.1f, col16=4.1, col17=\"hello2323\", col18=\"world2323\", col19=uuid(\"3d457e79-1bed-d6c2-3612-b0d31c1881f6\"), col20=datehour(2013.06.13 13:30:10), col21=ipaddr(\"192.168.0.253\"), col22=int128(\"e1221797c52e15f763380b45e841ec32\"), col23=blob(\"123fff\"), col24=complex(-111,-1), col25=point(-1,-2), col26=decimal32(-1.1,2), col27=decimal64(-1.1,7), col28=decimal128(-1.1,18)");
		JDBCResultSet rs2 = (JDBCResultSet)stmt.executeQuery("select * from loadTable('dfs://test_allDataType', 'pt')");
		BasicTable re2 = (BasicTable)rs2.getResult();
		Assert.assertEquals(0, re2.rows());
	}
	@Test
	public void test_JDBCStatement_dfs_allDataType_executeUpdate_insert_into_null() throws SQLException, IOException, ClassNotFoundException {
		DBConnection db = new DBConnection();
		db.connect(HOST, PORT,"admin","123456");
		db.run("colNames=\"col\"+string(1..28)\n" +
				"colTypes=[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(18)]\n" +
				"t=table(1:0,colNames,colTypes)\n" +
				"try{dropDatabase('dfs://test_allDataType')\n}catch(ex){}\n" +
				"db=database('dfs://test_allDataType', RANGE, -1000 0 1000,,'TSDB')\n"+
				"db.createPartitionedTable(t, `pt, `col4,,`col4) \n");
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.executeUpdate("insert into loadTable('dfs://test_allDataType','pt') values(,,,2,,,,,,,,,,,,,,,,,,,,,,,,) ");
		stmt.executeUpdate("insert into loadTable('dfs://test_allDataType',`pt) ValUes(NULL,NULL,NULL,3,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL) ");
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("select * from loadTable('dfs://test_allDataType', 'pt')");
		BasicTable re = (BasicTable)rs.getResult();
		System.out.println(re.getString());
		Assert.assertEquals(2, re.rows());
		stmt.executeUpdate("update  pt set col5 = 101  where col1 = NULL ,col2=NULL ,col3=NULL ,col4 =2, col5=NULL, col6=NULL, col7=NULL, col8=NULL, col9=NULL, col10=NULL, col11=NULL, col12=NULL, col13=NULL, col14=NULL, col15=NULL, col16=NULL, col17=NULL, col18=NULL, col19=NULL, col20=NULL, col21=NULL, col22=NULL, col23=NULL, col24=NULL, col25=NULL, col26=NULL, col27=NULL, col28=NULL");
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("select * from pt where col4 =2;");
		BasicTable re1 = (BasicTable)rs1.getResult();
		org.junit.Assert.assertEquals("101", re1.getColumn("col5").getString());
		stmt.executeUpdate("delete from pt where col4 in (select col4 from pt where col1 = NULL ,col2=NULL ,col3=NULL ,col4 =3, col5=NULL, col6=NULL, col7=NULL, col8=NULL, col9=NULL, col10=NULL, col11=NULL, col12=NULL, col13=NULL, col14=NULL, col15=NULL, col16=NULL, col17=NULL, col18=NULL, col19=NULL, col20=NULL, col21=NULL, col22=NULL, col23=NULL, col24=NULL, col25=NULL, col26=NULL, col27=NULL, col28=NULL)");
		JDBCResultSet rs2 = (JDBCResultSet)stmt.executeQuery("select * from pt");
		BasicTable re2 = (BasicTable)rs2.getResult();
		org.junit.Assert.assertEquals(1,re2.rows());
	}
	@Test
	public void test_JDBCStatement_dfs_allDataType_execute_insert_into_null() throws SQLException, IOException, ClassNotFoundException {
		DBConnection db = new DBConnection();
		db.connect(HOST, PORT,"admin","123456");
		db.run("colNames=\"col\"+string(1..28)\n" +
				"colTypes=[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(18)]\n" +
				"t=table(1:0,colNames,colTypes)\n" +
				"try{dropDatabase('dfs://test_allDataType')\n}catch(ex){}\n" +
				"db=database('dfs://test_allDataType', RANGE, -1000 0 1000,,'TSDB')\n"+
				"db.createPartitionedTable(t, `pt, `col4,,`col4) \n");
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("insert into loadTable('dfs://test_allDataType','pt') values(,,,2,,,,,,,,,,,,,,,,,,,,,,,,) ");
		stmt.execute("insert into loadTable('dfs://test_allDataType',`pt) ValUes(NULL,NULL,NULL,3,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL) ");
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("select * from loadTable('dfs://test_allDataType', 'pt')");
		BasicTable re = (BasicTable)rs.getResult();
		System.out.println(re.getString());
		Assert.assertEquals(2, re.rows());
		stmt.executeUpdate("update  pt set col5 = 101  where col1 = NULL ,col2=NULL ,col3=NULL ,col4 =2, col5=NULL, col6=NULL, col7=NULL, col8=NULL, col9=NULL, col10=NULL, col11=NULL, col12=NULL, col13=NULL, col14=NULL, col15=NULL, col16=NULL, col17=NULL, col18=NULL, col19=NULL, col20=NULL, col21=NULL, col22=NULL, col23=NULL, col24=NULL, col25=NULL, col26=NULL, col27=NULL, col28=NULL");
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("select * from pt where col4 =2;");
		BasicTable re1 = (BasicTable)rs1.getResult();
		org.junit.Assert.assertEquals("101", re1.getColumn("col5").getString());
		stmt.executeUpdate("delete from pt where col4 in (select col4 from pt where col1 = NULL ,col2=NULL ,col3=NULL ,col4 =3, col5=NULL, col6=NULL, col7=NULL, col8=NULL, col9=NULL, col10=NULL, col11=NULL, col12=NULL, col13=NULL, col14=NULL, col15=NULL, col16=NULL, col17=NULL, col18=NULL, col19=NULL, col20=NULL, col21=NULL, col22=NULL, col23=NULL, col24=NULL, col25=NULL, col26=NULL, col27=NULL, col28=NULL)");
		JDBCResultSet rs2 = (JDBCResultSet)stmt.executeQuery("select * from pt");
		BasicTable re2 = (BasicTable)rs2.getResult();
		org.junit.Assert.assertEquals(1,re2.rows());
	}
}
