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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.dolphindb.jdbc.JDBCConnection;
import com.dolphindb.jdbc.JDBCResultSet;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JDBCConnectionTest {
	static String HOST = JDBCTestUtil.HOST;
	static int PORT = JDBCTestUtil.PORT ;
	private String url = null;
	Properties prop = new Properties();
	Connection conn;
	@Before
	public void SetUp() throws SQLException {
		prop.setProperty("hostName",HOST);
		prop.setProperty("port",String.valueOf(PORT));
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
}
