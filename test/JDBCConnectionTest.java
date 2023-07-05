import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.dolphindb.jdbc.JDBCConnection;
import com.dolphindb.jdbc.JDBCResultSet;
import com.xxdb.DBConnection;
import com.xxdb.comm.SqlStdEnum;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JDBCConnectionTest {
	static String HOST = JDBCTestUtil.HOST;
	static int PORT = JDBCTestUtil.PORT ;
	static int PORT1 = JDBCTestUtil.PORT1 ;
	static int COLPORT = JDBCTestUtil.COLPORT ;

	static String SITE1 = JDBCTestUtil.SITE1 ;
	static String SITES = JDBCTestUtil.SITES ;

	static String SITE2 = JDBCTestUtil.SITE2 ;
	private String url = null;
	Properties prop = new Properties();
	Connection conn;
	Statement stm ;

	@Before
	public void SetUp() throws SQLException {
		prop.setProperty("hostName",HOST);
		prop.setProperty("port",String.valueOf(PORT));
		//prop.setProperty("sqlStd", String.valueOf(SqlStdEnum.Oracle));
		url = "jdbc:dolphindb://"+JDBCTestUtil.HOST+":"+JDBCTestUtil.PORT;
		conn = new JDBCConnection(url,prop);
	}
	@After
	public void tearDown() throws SQLException {
		conn.close();
	}
	@Test
	public void Test_nativeSQL() throws SQLException {
		org.junit.Assert.assertEquals("ddd", conn.nativeSQL("ddd"));
	}
	@Test
	public void Test_setAutoCommit() throws SQLException {
		conn.setAutoCommit(true);
		org.junit.Assert.assertEquals(true, conn.getAutoCommit());

	}
	@Test
	public void Test_commit() throws SQLException {
		try{
			conn.commit();
		}catch(SQLException e){
			org.junit.Assert.assertEquals("commit not implemented", e.getMessage());
		}
	}
	@Test
	public void Test_rollback() throws SQLException {
		try{
			conn.rollback();
		}catch(SQLException e){
			org.junit.Assert.assertEquals("rollback not implemented", e.getMessage());
		}
	}
	@Test
	public void Test_getMetaData() throws SQLException {
		conn.getMetaData();
	}
	@Test
	public void Test_setReadOnly() throws SQLException {
		conn.setReadOnly(false);
	}
	@Test
	public void Test_isReadOnly() throws SQLException {
		org.junit.Assert.assertEquals(false, conn.isReadOnly());
	}
	@Test
	public void Test_setCatalog() throws SQLException {
		try{
			conn.setCatalog("string");
		}catch(SQLException e){
			org.junit.Assert.assertEquals("setCatalog not implemented", e.getMessage());
		}
	}
	@Test
	public void Test_getCatalog() throws SQLException {
		conn.getCatalog();
		System.out.println(conn.getCatalog().toString());
//		org.junit.Assert.assertEquals(false, conn.isReadOnly());
	}
	@Test
	public void Test_setTransactionIsolation() throws SQLException {
		conn.getCatalog();
		System.out.println(conn.getCatalog().toString());
//		org.junit.Assert.assertEquals(false, conn.isReadOnly());
	}
	@Test
	public void contextLoads() throws Exception {

		BasicAnyVector basicAnyVector = new BasicAnyVector(2);
		BasicMonthVector basicMonthVector = new BasicMonthVector(2);

		YearMonth yearMonth = YearMonth.of(2023, 3);
		YearMonth yearMonth1 = YearMonth.of(2023, 4);
		basicMonthVector.setMonth(0, yearMonth);
		basicMonthVector.setMonth(1,yearMonth1);

		basicAnyVector.setEntity(0, basicMonthVector);
		basicAnyVector.setEntity(1,new BasicInt(Integer.parseInt("30")));
		System.out.println(VectorToString(basicAnyVector));

	}

	public static String VectorToString(Entity entity) {
		String result = "";
		if (entity instanceof AbstractVector) {
			int length = ((Vector)entity).rows();

			for(int i = 0; i < length; ++i) {
				System.out.println("11");
				String s = String.valueOf(((Vector)entity).get(i));
				result = result + s + " ";
			}
		}

		return result;
	}
	@Test
	public void Test_getConnection_table_not_grant() throws SQLException, ClassNotFoundException {
		Class.forName("com.dolphindb.jdbc.Driver");
		String script = "n = 10000;" +
				"SecurityID = rand(`st0001`st0002`st0003`st0004`st0005, n);" +
				"sym = rand(`A`B, n);" +
				"TradeDate = 2022.01.01 + rand(100,n);" +
				"TotalVolumeTrade = rand(1000..3000, n);"+
				"TotalValueTrade = rand(100.0, n); "+
				"schemaTable_snap = table(SecurityID, TradeDate, TotalVolumeTrade, TotalValueTrade).sortBy!(`SecurityID`TradeDate);"+
				"dbPath = \"dfs://TSDB_STOCK\"; "+
				"if(existsDatabase(dbPath)){dropDatabase(dbPath)}; "+
				"db_snap = database(dbPath, VALUE, 2022.01.01..2022.01.05, engine='TSDB'); "+
				"snap=createPartitionedTable(dbHandle=db_snap, table=schemaTable_snap, tableName=\"snap\", partitionColumns=`TradeDate, sortColumns=`SecurityID`TradeDate, keepDuplicates=ALL, sortKeyMappingFunction=[hashBucket{,5}]); "+
				"snap.append!(schemaTable_snap); "+
				"flushTSDBCache(); "+
				"snap1=createPartitionedTable(dbHandle=db_snap, table=schemaTable_snap, tableName=\"snap1\", partitionColumns=`TradeDate, sortColumns=`SecurityID`TradeDate, keepDuplicates=ALL, sortKeyMappingFunction=[hashBucket{,5}]); "+
				"snap1.append!(schemaTable_snap); "+
				"flushTSDBCache(); "+
				"try{createUser(`user1, `123456)}catch(ex){print(ex)}; ";
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		conn.equals(true);
		stmt = conn.createStatement();
		stmt.execute(script);
		Connection conn1 = null;
		String url1 = "jdbc:dolphindb://"+HOST+":"+PORT+"?databasePath=dfs://TSDB_STOCK";
		conn1 = DriverManager.getConnection(url1,"user1","123456");
		conn1.equals(true);
		System.out.println( "Connect success!" );
		conn.close();
	}

	@Test
	public void Test_getConnection_highAvailability_false() throws SQLException, ClassNotFoundException {
		String script = "def restart(n)\n" +
				"{\n" +
				"try{\n" +
				"stopDataNode(\""+HOST+":"+PORT+"\");\n" +
				"}catch(ex)\n"+
				"{}\n"+
				"sleep(n);\n"+
				"try{\n" +
				"stopDataNode(\""+HOST+":"+PORT+"\");\n" +
				"}catch(ex)\n"+
				"{}\n"+
				"}\n" +
				"submitJob(\"restart\",\"restart\",restart,1000);";

		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&highAvailability=false";
		String url1 = "jdbc:dolphindb://"+HOST+":"+COLPORT+"?user=admin&password=123456&highAvailability=false";
		Connection conn = null;
		Connection conn1 = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url1);
		stmt = conn.createStatement();
		//stmt.execute(script);

		conn1 = DriverManager.getConnection(url);
		conn1.equals(false);

		conn1 = DriverManager.getConnection(url);
		conn1.equals(true);
		conn.close();
	}
	@Test
	public void Test_getConnection_highAvailability_true_highAvailabilitySites_null() throws SQLException, ClassNotFoundException {
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&highAvailability=true";
		String url1 = "jdbc:dolphindb://"+HOST+":"+COLPORT+"?user=admin&password=123456";
		Connection conn = null;
		Connection conn1 = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url1);
		stmt = conn.createStatement();
		conn1 = DriverManager.getConnection(url);
		conn1.equals(true);
		try{
			stmt.execute("stopDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(500)");
		Statement s = conn1.createStatement();
		s.execute("trade=table(`XOM`GS`AAPL as id, 102.1 33.4 73.6 as x);");
		ResultSet rs1 =s.executeQuery("SElect * fROM trade ;");
		Assert.assertTrue(rs1.next());
		stmt = conn.createStatement();
		try{
			stmt.execute("startDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(5000)");
		conn1 = DriverManager.getConnection(url);
		//conn1.equals(true);
		conn.close();
		conn1.close();
	}
	@Test
	public void Test_getConnection_highAvailability_true_highAvailabilitySites_notNull() throws SQLException, ClassNotFoundException {
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "123456");
		info.put("highAvailability", "true");
		info.put("highAvailabilitySites", SITES);

		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&highAvailability=true&highAvailabilitySites="+SITES;
		String url1 = "jdbc:dolphindb://"+HOST+":"+COLPORT+"?user=admin&password=123456";
		Connection conn = null;
		Connection conn1 = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url1);
		stmt = conn.createStatement();
		try{
			stmt.execute("stopDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(500)");
		conn1 = DriverManager.getConnection(url);
		conn1.equals(true);
		Statement s = conn1.createStatement();
		s.execute("trade=table(`XOM`GS`AAPL as id, 102.1 33.4 73.6 as x);");
		ResultSet rs1 =s.executeQuery("SElect * fROM trade ;");
		Assert.assertTrue(rs1.next());
		stmt = conn.createStatement();
		try{
			stmt.execute("startDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(5000)");
		conn1 = DriverManager.getConnection(url);
		//conn1.equals(true);
		conn1.close();
	}

	@Test
	public void Test_getConnection_highAvailability_true_highAvailabilitySites_notNull_1() throws SQLException, ClassNotFoundException {
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "123456");
		info.put("highAvailability", "true");
		info.put("highAvailabilitySites", SITES);
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&highAvailability=true&highAvailabilitySites="+SITES;
		String url1 = "jdbc:dolphindb://"+HOST+":"+COLPORT+"?user=admin&password=123456";
		Connection conn = null;
		Connection conn1 = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url1);
		stmt = conn.createStatement();
		try{
			stmt.execute("stopDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		try{
			stmt.execute("stopDataNode(\""+HOST+":"+PORT1+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(500)");
		conn1 = DriverManager.getConnection(url);
		conn1.equals(true);
		Statement s = conn1.createStatement();
		s.execute("trade=table(`XOM`GS`AAPL as id, 102.1 33.4 73.6 as x);");
		ResultSet rs1 =s.executeQuery("SElect * fROM trade ;");
		Assert.assertTrue(rs1.next());
		stmt = conn.createStatement();
		try{
			stmt.execute("startDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		try{
			stmt.execute("startDataNode(\""+HOST+":"+PORT1+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(5000)");
		conn1 = DriverManager.getConnection(url);
		//conn1.equals(true);
		conn1.close();
	}

	//@Test
	public void Test_getConnection_enableHighAvailability_false() throws SQLException, ClassNotFoundException {
		String script = "def restart(n)\n" +
				"{\n" +
				"try{\n" +
				"stopDataNode(\""+HOST+":"+PORT+"\");\n" +
				"}catch(ex)\n"+
				"{}\n"+
				"sleep(n);\n"+
				"try{\n" +
				"startDataNode(\""+HOST+":"+PORT+"\");\n" +
				"}catch(ex)\n"+
				"{}\n"+
				"}\n" +
				"submitJob(\"restart\",\"restart\",restart,10000);";

		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&enableHighAvailability=false";
		String url1 = "jdbc:dolphindb://"+HOST+":"+COLPORT+"?user=admin&password=123456&enableHighAvailability=false";
		Connection conn = null;
		Connection conn1 = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url1);
		stmt = conn.createStatement();
		stmt.execute(script);
		stmt = conn.createStatement();
		stmt.execute("sleep(1000)");
		conn1 = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("sleep(10000)");
		conn.close();
	}
	@Test
	public void Test_getConnection_enableHighAvailability_true_highAvailabilitySites_null() throws SQLException, ClassNotFoundException {
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&enableHighAvailability=true";
		String url1 = "jdbc:dolphindb://"+HOST+":"+COLPORT+"?user=admin&password=123456";
		Connection conn = null;
		Connection conn1 = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url1);
		stmt = conn.createStatement();
		conn1 = DriverManager.getConnection(url);
		conn1.equals(true);
		try{
			stmt.execute("stopDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(500)");
		Statement s = conn1.createStatement();
		s.execute("trade=table(`XOM`GS`AAPL as id, 102.1 33.4 73.6 as x);");
		ResultSet rs1 =s.executeQuery("SElect * fROM trade ;");
		Assert.assertTrue(rs1.next());
		stmt = conn.createStatement();
		try{
			stmt.execute("startDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(5000)");
		conn1 = DriverManager.getConnection(url);
		//conn1.equals(true);
		conn.close();
		conn1.close();
	}
	@Test
	public void Test_getConnection_enableHighAvailability_true_highAvailabilitySites_notNull() throws SQLException, ClassNotFoundException {
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "123456");
		info.put("highAvailability", "true");
		info.put("highAvailabilitySites", SITES);

		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&enableHighAvailability=true&highAvailabilitySites="+SITES;
		String url1 = "jdbc:dolphindb://"+HOST+":"+COLPORT+"?user=admin&password=123456";
		Connection conn = null;
		Connection conn1 = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url1);
		stmt = conn.createStatement();
		try{
			stmt.execute("stopDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(500)");
		conn1 = DriverManager.getConnection(url);
		conn1.equals(true);
		Statement s = conn1.createStatement();
		s.execute("trade=table(`XOM`GS`AAPL as id, 102.1 33.4 73.6 as x);");
		ResultSet rs1 =s.executeQuery("SElect * fROM trade ;");
		Assert.assertTrue(rs1.next());
		stmt = conn.createStatement();
		try{
			stmt.execute("startDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(5000)");
		conn1 = DriverManager.getConnection(url);
		//conn1.equals(true);
		conn1.close();
	}

	@Test
	public void Test_getConnection_enableHighAvailability_true_highAvailabilitySites_notNull_1() throws SQLException, ClassNotFoundException {
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "123456");
		info.put("highAvailability", "true");
		info.put("highAvailabilitySites", SITES);
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&enableHighAvailability=true&highAvailabilitySites="+SITES;
		String url1 = "jdbc:dolphindb://"+HOST+":"+COLPORT+"?user=admin&password=123456";
		Connection conn = null;
		Connection conn1 = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url1);
		stmt = conn.createStatement();
		try{
			stmt.execute("stopDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		try{
			stmt.execute("stopDataNode(\""+HOST+":"+PORT1+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(500)");
		conn1 = DriverManager.getConnection(url);
		conn1.equals(true);
		Statement s = conn1.createStatement();
		s.execute("trade=table(`XOM`GS`AAPL as id, 102.1 33.4 73.6 as x);");
		ResultSet rs1 =s.executeQuery("SElect * fROM trade ;");
		Assert.assertTrue(rs1.next());
		stmt = conn.createStatement();
		try{
			stmt.execute("startDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		try{
			stmt.execute("startDataNode(\""+HOST+":"+PORT1+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(5000)");
		conn1 = DriverManager.getConnection(url);
		//conn1.equals(true);
		conn1.close();
	}
	@Test(expected = RuntimeException.class)
	public void Test_getConnection_enableHighAvailability_highAvailability_not_same() throws SQLException, ClassNotFoundException {
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&enableHighAvailability=true&highAvailability=false";
		Connection conn = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
	}
	@Test()
	public void Test_JDBCConnection_in_case_when() throws SQLException, ClassNotFoundException {
		prop.setProperty("user","admin");
		prop.setProperty("password","123456");
		prop.setProperty("sqlStd", String.valueOf(SqlStdEnum.Oracle));
		url = "jdbc:dolphindb://"+JDBCTestUtil.HOST+":"+JDBCTestUtil.PORT;
		conn = new JDBCConnection(url,prop);
		stm = conn.createStatement();
		String sql = "t1 = table(1 2 3 as id, 2012.12.01 2012.12.02 2012.12.02 as date, 'BOND' 'BOND' 'BOND' as grp);\n " +
				"t2 = table(1 2 3 as id,true false false as flag);\n" +
				"dbName = \"dfs://test\";\n" +
				"if(existsDatabase(dbName)){dropDatabase(dbName)};\n" +
				"db=database(\"dfs://test\", VALUE, 1..3)\n;\n" +
				"pt1 = db.createPartitionedTable(t1, `pt1, `id);;\n" +
				"pt1.append!(t1);\n" +
				"pt2 = db.createPartitionedTable(t2, `pt2, `id);\n" +
				"pt2.append!(t2);" ;
		stm.execute(sql);
		Statement s = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) s.executeQuery("select *  from loadTable(\"dfs://test\",`pt1) as m1 cross join loadTable(\"dfs://test\",`pt2) as  m2 where m1.date in (exec case when to_date(\"2022-10-01\",'YYYY-MM-DD') >  max(date) then max(date) else to_date(\"2022-10-01\",\"YYYY-MM-DD\")-1 end as D from loadTable(\"dfs://test\",`pt1)); ");
		BasicTable rs1 = (BasicTable) rs.getResult();
		System.out.println(rs1.rows());
		Assert.assertEquals(6,rs1.rows());
	}
	@Test()
	public void Test_JDBCConnection_SqlStdEnum_DolphinDB() throws SQLException, ClassNotFoundException {
		prop.setProperty("user","admin");
		prop.setProperty("password","123456");
		prop.setProperty("sqlStd", String.valueOf(SqlStdEnum.DolphinDB));
		url = "jdbc:dolphindb://"+JDBCTestUtil.HOST+":"+JDBCTestUtil.PORT;
		conn = new JDBCConnection(url,prop);
		Statement s = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) s.executeQuery("cumavg(1 2 3); ");
		BasicDoubleVector rs1 = (BasicDoubleVector) rs.getResult();
		assertEquals("[1,1.5,2]", rs1.getString());
		String e = null;
		try{
			Statement s1 = conn.createStatement();
			JDBCResultSet rs2 = (JDBCResultSet) s1.executeQuery("SYSDATE();");
		}catch(Exception E){
			e = E.getMessage();
			System.out.println(e);

		}
		assertNotNull(e);
		conn.close();
	}
	@Test()
	public void Test_JDBCConnection_SqlStdEnum_default() throws SQLException, ClassNotFoundException {
		prop.setProperty("user","admin");
		prop.setProperty("password","123456");
//		prop.setProperty("sqlStd", String.valueOf(SqlStdEnum.DolphinDB));
		url = "jdbc:dolphindb://"+JDBCTestUtil.HOST+":"+JDBCTestUtil.PORT;
		conn = new JDBCConnection(url,prop);
		Statement s = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) s.executeQuery("cumavg(1 2 3); ");
		BasicDoubleVector rs1 = (BasicDoubleVector) rs.getResult();
		assertEquals("[1,1.5,2]", rs1.getString());
		String e = null;
		try{
			Statement s1 = conn.createStatement();
			JDBCResultSet rs2 = (JDBCResultSet) s1.executeQuery("SYSDATE();");
		}catch(Exception E){
			e = E.getMessage();
			System.out.println(e);
		}
		assertNotNull(e);
		conn.close();
	}
	@Test()
	public void Test_JDBCConnection_SqlStdEnum_Oracle() throws SQLException, ClassNotFoundException {
		prop.setProperty("user","admin");
		prop.setProperty("password","123456");
		prop.setProperty("sqlStd", String.valueOf(SqlStdEnum.Oracle));
		url = "jdbc:dolphindb://"+JDBCTestUtil.HOST+":"+JDBCTestUtil.PORT;
		conn = new JDBCConnection(url,prop);
		Statement s = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) s.executeQuery("cumavg(1 2 3); ");
		BasicDoubleVector rs1 = (BasicDoubleVector) rs.getResult();
		assertEquals("[1,1.5,2]", rs1.getString());

		Statement s1 = conn.createStatement();
		JDBCResultSet rs2 = (JDBCResultSet) s1.executeQuery("SYSDATE(); ");
		BasicDate rs3 = (BasicDate) rs2.getResult();
		Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		String currentTime1 = formatter.format(currentTime);
		assertEquals(currentTime1.replace("-",""), rs3.toString().replace(".",""));

		Statement s3 = conn.createStatement();
		JDBCResultSet rs4 = (JDBCResultSet) s3.executeQuery("concat(string(1 2 3), string(4 5 6));");
		BasicStringVector rs5 = (BasicStringVector) rs4.getResult();
		assertEquals("[14,25,36]", rs5.getString());
		conn.close();
	}
	@Test()
	public void Test_JDBCConnection_SqlStdEnum_MySQL() throws SQLException, ClassNotFoundException {
		prop.setProperty("user","admin");
		prop.setProperty("password","123456");
		prop.setProperty("sqlStd", String.valueOf(SqlStdEnum.MySQL));
		url = "jdbc:dolphindb://"+JDBCTestUtil.HOST+":"+JDBCTestUtil.PORT;
		conn = new JDBCConnection(url,prop);
		Statement s = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) s.executeQuery("cumavg(1 2 3); ");
		BasicDoubleVector rs1 = (BasicDoubleVector) rs.getResult();
		assertEquals("[1,1.5,2]", rs1.getString());

		Statement s1 = conn.createStatement();
		JDBCResultSet rs2 = (JDBCResultSet) s1.executeQuery("SYSDATE(); ");
		BasicDate rs3 = (BasicDate) rs2.getResult();
		Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		String currentTime1 = formatter.format(currentTime);
		assertEquals(currentTime1.replace("-",""), rs3.toString().replace(".",""));

		String e = null;
		try{
			Statement s3 = conn.createStatement();
			JDBCResultSet rs4 = (JDBCResultSet) s3.executeQuery("concat(string(1 2 3), string(4 5 6));");
		}catch(Exception E){
			e = E.getMessage();
		}
		assertNotNull(e);
		conn.close();
	}
	@Test
	public void Test_getConnection_highAvailabilitySites_comma() throws SQLException, ClassNotFoundException {
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "123456");
		info.put("highAvailability", "true");
		info.put("highAvailabilitySites", SITE2);

		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456&enableHighAvailability=true&highAvailabilitySites="+SITE2;
		String url1 = "jdbc:dolphindb://"+HOST+":"+COLPORT+"?user=admin&password=123456";
		Connection conn = null;
		Connection conn1 = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url1);
		stmt = conn.createStatement();
		try{
			stmt.execute("stopDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(500)");
		conn1 = DriverManager.getConnection(url);
		conn1.equals(true);
		Statement s = conn1.createStatement();
		s.execute("trade=table(`XOM`GS`AAPL as id, 102.1 33.4 73.6 as x);");
		ResultSet rs1 =s.executeQuery("SElect * fROM trade ;");
		Assert.assertTrue(rs1.next());
		stmt = conn.createStatement();
		try{
			stmt.execute("startDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(5000)");
		conn1 = DriverManager.getConnection(url);
		//conn1.equals(true);
		conn1.close();
	}
	@Test
	public void Test_getConnection_highAvailabilitySites_comma_1() throws SQLException, ClassNotFoundException {
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "123456");
		info.put("highAvailability", "true");
		info.put("highAvailabilitySites", SITE2);

		String url = "jdbc:dolphindb://"+HOST+":"+PORT;
		String url1 = "jdbc:dolphindb://"+HOST+":"+COLPORT+"?user=admin&password=123456";
		Connection conn = null;
		Connection conn1 = null;
		Statement stmt = null;
		ResultSet rs = null;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url1);
		stmt = conn.createStatement();
		try{
			stmt.execute("stopDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(500)");
		conn1 = DriverManager.getConnection(url,info);
		conn1.equals(true);
		Statement s = conn1.createStatement();
		s.execute("trade=table(`XOM`GS`AAPL as id, 102.1 33.4 73.6 as x);");
		ResultSet rs1 =s.executeQuery("SElect * fROM trade ;");
		Assert.assertTrue(rs1.next());
		stmt = conn.createStatement();
		try{
			stmt.execute("startDataNode(\""+HOST+":"+PORT+"\")");
		}catch(Exception ex)
		{}
		stmt.execute(" sleep(5000)");
		conn1 = DriverManager.getConnection(url);
		//conn1.equals(true);
		conn1.close();
	}
}
