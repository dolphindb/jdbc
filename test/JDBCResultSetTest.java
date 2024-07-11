//import jdk.internal.dynalink.beans.StaticClass;
import com.dolphindb.jdbc.JDBCConnection;
import com.dolphindb.jdbc.JDBCResultSet;
import com.dolphindb.jdbc.JDBCStatement;

import com.xxdb.data.BasicDateHour;
import com.xxdb.data.BasicIntMatrix;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import junit.framework.TestCase;
import org.junit.*;
import org.junit.Assert.*;


import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import com.xxdb.DBConnection;
import org.junit.Test;

import static com.dolphindb.jdbc.Main.printData;
import static org.junit.Assert.assertEquals;


public class JDBCResultSetTest {
	static String HOST = JDBCTestUtil.HOST;
	static int PORT = JDBCTestUtil.PORT;
	static String  PATH = JDBCTestUtil.WORK_DIR;
	static String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
	static  String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
	@Before
	public void SetUp() throws IOException {
	}


	public static boolean CreateMemoryTable(String host, Integer port) {
		boolean success = false;
		DBConnection db = null;
		try {
			String script = "login(`admin, `123456); \n" +
					"c=bool(0 -2 301 NULL);\n" +
					"d=char(0 2 301 NULL);\n" +
					"f=short(0 -2 301 NULL);\n" +
					"g=int(1 2 3 4);\n" +
					"h=long(0 -2 301 NULL);\n" +
					"i=float(0.102 -2.2 301 NULL);\n" +
					"j=double(0.102 -2 301 NULL);\n" +
					"k=string(0 -2 301 NULL);\n" +
					"l=[2000.01.01,2011.01.01,2020.11.01,2021.11.01]\n" +
					"m=[00:00:00.000,NULL,13:41:39.989,13:41:29.989]\n" +
					"n=[2012.06.13 13:30:10.008,2013.06.13 13:30:10.007,2012.06.13 13:35:10.008,2019.06.13 13:35:19.008]\n" +
					"o=[2012.06M,2012.07M,2012.08M,]\n" +
					"p=[2012.06.13 13:30:10,,2013.04.13 13:30:10,2012.06.14 13:34:50]\n" +
					"q=[2012.06.13 13:30:10.008007006,2013.06.14 12:36:10.003007006,2002.06.14 13:35:12.058007006,]\n" +
					"r=13:30m 14:32m 11:38m NULL\n" +
					"s=13:30:10 13:32:10 13:30:13 NULL\n" +
					"u=13:30:10.008007006 13:31:12.008002006 NULL 13:32:10.008207006\n" +
					"t= table(c as a3,d as a4, f as a5,g as a6,h as a7,i as a8,j as a9,k as a10,l as a11,m as a12,n as a13,o as a14,p as a15,q as a16,r as a17,s as a18,u as a19);\n" +
					"db =database(\""+PATH+"/db1\");\n" +
					"saveTable(db,t,`tb)";
			db = new DBConnection();
			db.connect(host, port);
			db.run(script);
			success = true;
		} catch (Exception e) {
			e.printStackTrace();
			success = false;
		} finally {
			if (db != null) {
				db.close();
			}
			return success;
		}
	}
	public static boolean CreateDfsTable(String host, Integer port) {
		boolean success = false;
		DBConnection db = new DBConnection();
		try {
			String script = "n=4;\n" +
					" id = take(1 2 3 4,n);\n" +
					" cbool = take(true NULL false false,n);\n" +
					" cchar = take('a' '0' 'z' NULL,n);\n" +
					" cshort = take(-1h 200h 0 NULL,n);\n" +
					" cint = take(-1 1000 0 NULL,n);\n" +
					" clong = take(200l 2000l 0 NULL,n)\n" +
					" cdate = take(1969.08.16 1970.01.01 2022.09.30 NULL,n)\n" +
					" cmonth = take(1969.01M 1970.01M  2022.10M NULL,n)\n" +
					" ctime = take(00:00:00.001 00:00:00.001 23:59:59.999  NULL,n)\n" +
					" cminute = take(00:01m 00:01m 23:59m NULL,n)\n" +
					" csecond = take(00:00:01 12:00:01  23:59:59 NULL,n)\n" +
					" cdatetime = take(1969.01.01 00:00:01  1970.01.01 00:00:01 2022.09.30 23:59:59 NULL,n)\n" +
					" ctimestamp = take(1969.09.30 00:00:00.001 1970.09.30 00:00:00.001 2022.09.30 23:59:59.999 NULL,n)\n" +
					" cnanotime = take(23:59:58.000000001 00:00:00.000000001 23:59:58.000007016 NULL,n)\n" +
					" cnanotimestamp = take(1969.09.30 23:59:58.000000001 1970.01.01 23:59:58.000000001 2022.09.30 23:59:58.000001112 NULL,n)\n" +
					" cfloat = take(300.0f 0 -2.0f NULL,n)\n" +
					" cdouble = take(230.0 0 -230.0 NULL,n)\n" +
					" cstring = take(\"123\" \"0\" \"-123\" NULL,n)\n" +
					" cstring1 = take(\"hello\" \"\" \"!@#$%^&*()_+QWERTYUIOP{}|ASDFGHJKL:ZXCVBNM<>?1234567890-=\\][poikjhhgfdsazxcvbnm,./\" NULL,n)\n" +
					" cdatehour = datehour(take(1969.01.01 01:00:00 1970.01.01 01:00:00 2024.01.01 01:00:00  NULL,n))\n" +
					" cdecimal32 = decimal32(take(-1 0 2022.9999 NULL,n),4)\n" +
					" cdecimal64 = decimal64(take(-2022 0 4044.00008 NULL,n),4)\n" +
					" cdecimal128 = decimal128(take(-2022 0 4044.00008 NULL,n),10)\n" +
					" t = table(id,cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,cstring,cstring1,cdatehour,cdecimal32,cdecimal64,cdecimal128)\n" +
					" if(existsDatabase(\"dfs://testResult\")){\n" +
					"     dropDatabase(\"dfs://testResult\")\n" +
					" }\n" +
					" db = database(\"dfs://testResult\",HASH,[INT, 2])\n" +
					" pt = db.createPartitionedTable(t,`pt,`id)\n" +
					" pt.append!(t)";
			db.connect(HOST,PORT,"admin","123456");
			db.run(script);
			success = true;
		} catch (Exception e) {
			e.printStackTrace();
			success = false;
		} finally {
			if (db != null) {
				db.close();
			}
			return success;
		}
	}

	Connection conn = null;
	Statement stmt = null;
	ResultSet rs = null;
	@Before
	public void setUp(){
	}

	@After
	public void clean() throws SQLException {
		try{
			rs.close();
			stmt.close();
			conn.close();
		}catch(Exception e){
		}

	}

	@Test
	public void Test_ResultSet_dfs_getFloat() throws Exception {
		String[] fValueStr = {"0.102", "-2.2", "301.0", ""};
		String[] dValueStr = {"0.102", "-2.0", "301.0", ""};
		float[] fValue = new float[4];
		fValue[0] = 0.102f;		fValue[1] = -2.2f;		fValue[2] = 301f;
		double[] dValue = new double[4];
		dValue[0] = 0.102;		dValue[1] = -2.0;		dValue[2] = 301.0;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "f=float(0.102 -2.2 301 NULL);\n" +
				"d=double(0.102 -2 301 NULL);\n" +
				"t= table(f as f1,d as d1);\n" +
				"db =database(\"" + PATH + "/db1\");\n" +
				"saveTable(db,t,`tb)";
		stmt.execute(script);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");
		rs.next();
		for (int i=0;i<3;rs.next()) {
			TestCase.assertEquals((fValue[i]), rs.getFloat("f1"),3);
			TestCase.assertEquals((fValueStr[i]), rs.getString(1));
			TestCase.assertEquals((dValue[i]), rs.getDouble("d1"),3);
			TestCase.assertEquals((dValueStr[i]), rs.getString(2));
			i++;
		}
		int i=3;
		TestCase.assertEquals((fValue[i]), rs.getFloat("f1"),3);
		TestCase.assertEquals(null, rs.getString(1));
		TestCase.assertEquals((dValue[i]), rs.getDouble("d1"),3);
		TestCase.assertEquals(null, rs.getString(2));
	}

	@Test
	public void Test_ResultSet_dfs_getBool() throws Exception {
		DBConnection myConn= new DBConnection();
		myConn.connect(HOST, PORT, "admin", "123456");
		String[] bValueStr = {"false", "true", "true", ""};
		boolean[] bValue = new boolean[4];
		bValue[0] = false;		bValue[1] = true;		bValue[2] = true;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "c=bool(0 -2 301 NULL);\n" +
				"t= table(c as bol);\n" +
				"db =database(\"" + PATH + "/db1\");\n" +
				"saveTable(db,t,`tb)";
		stmt.execute(script);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");
		rs.next();
		for (int i=0;i<3;rs.next()) {
			TestCase.assertEquals((bValue[i]), rs.getBoolean("bol"));
			TestCase.assertEquals((bValueStr[i]), rs.getObject("bol").toString());
			TestCase.assertEquals((bValueStr[i]), rs.getObject(1).toString());
			TestCase.assertEquals((bValueStr[i]), rs.getString(1));
			i++;
		}
		int i=3;
		TestCase.assertEquals((bValue[i]), rs.getBoolean("bol"));
		TestCase.assertEquals(null, rs.getObject("bol"));
		TestCase.assertEquals(null, rs.getObject(1));
		TestCase.assertEquals(null, rs.getString(1));
	}

	@Test
	public void Test_ResultSet_dfs_getChar() throws Exception {
		String[] cValueStr = {"0", "2", "45", ""};
		char[] cValue = new char[4];
		cValue[0] = 0;		cValue[1] = 2;		cValue[2] = 45;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "c=char(0 2 301 NULL);\n" +
				"t= table(c as char);\n" +
				"db =database(\"" + PATH + "/db1\");\n" +
				"saveTable(db,t,`tb)";
		stmt.execute(script);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");
		rs.next();
		for (int i=0;i<3;rs.next()) {
			TestCase.assertEquals((cValue[i]), rs.getByte("char"));
			TestCase.assertEquals((cValueStr[i]), rs.getString(1));
			TestCase.assertEquals((cValueStr[i]), rs.getObject("char").toString());
			TestCase.assertEquals((cValueStr[i]), rs.getObject(1).toString());
			i++;
		}
		int i=3;
		TestCase.assertEquals((cValue[i]), rs.getByte("char"));
		TestCase.assertEquals(null, rs.getString(1));
		TestCase.assertEquals(null, rs.getObject("char"));
		TestCase.assertEquals(null, rs.getObject(1));

	}

	@Test
	public void Test_ResultSet_dfs_getInt() throws Exception {
		String[] sValueStr = {"0", "-2", "301", ""};
		String[] iValueStr = {"1", "2", "3", ""};
		String[] lValueStr = {"0", "-2", "301", ""};
		short[] sValue = new short[4];
		sValue[0] = 0;		sValue[1] = -2;		sValue[2] = 301;
		int[] iValue = new int[4];
		iValue[0] = 1;		iValue[1] = 2;		iValue[2] = 3;		iValue[3] = 4;
		long[] lValue = new long[4];
		lValue[0] = 0;		lValue[1] = -2;		lValue[2] = 301;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "\n" +
				"f=short(0 -2 301 NULL);\n" +
				"g=int(1 2 3 NULL);\n" +
				"h=long(0 -2 301 NULL);" +
				"t= table(f as s,g as i,h as l);\n" +
				"db =database(\"" + PATH + "/db1\");\n" +
				"saveTable(db,t,`tb)";
		stmt.execute(script);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");
		rs.next();
		for (int i=0;i<3;rs.next()) {
			TestCase.assertEquals((sValue[i]), rs.getShort("s"));
			TestCase.assertEquals((sValueStr[i]), rs.getString(1));
			TestCase.assertEquals((iValue[i]), rs.getInt("i"));
			TestCase.assertEquals((iValueStr[i]), rs.getString(2));
			TestCase.assertEquals((lValue[i]), rs.getLong("l"));
			TestCase.assertEquals((lValueStr[i]), rs.getString(3));
			TestCase.assertEquals((sValueStr[i]), rs.getObject("s").toString());
			TestCase.assertEquals((sValueStr[i]), rs.getObject(1).toString());
			TestCase.assertEquals((iValueStr[i]), rs.getObject("i").toString());
			TestCase.assertEquals((iValueStr[i]), rs.getObject(2).toString());
			TestCase.assertEquals((lValueStr[i]), rs.getObject("l").toString());
			TestCase.assertEquals((lValueStr[i]), rs.getObject(3).toString());
			i++;
		}
		int i=3;
		TestCase.assertEquals((sValue[i]), rs.getShort("s"));
		TestCase.assertEquals(null, rs.getString(1));
		TestCase.assertEquals(0, rs.getInt("i"));
		TestCase.assertEquals(null, rs.getString(2));
		TestCase.assertEquals((lValue[i]), rs.getLong("l"));
		TestCase.assertEquals(null, rs.getString(3));
		TestCase.assertEquals(null, rs.getObject("s"));
		TestCase.assertEquals(null, rs.getObject(1));
		TestCase.assertEquals(null, rs.getObject("i"));
		TestCase.assertEquals(null, rs.getObject(2));
		TestCase.assertEquals(null, rs.getObject("l"));
		TestCase.assertEquals(null, rs.getObject(3));
	}

	@Test
	public void Test_ResultSet_dfs_getString() throws Exception {
		String[] StrValue = {"0", "-2", "301", ""};
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "k=string(0 -2 301 NULL);\n" +
				"t= table(k as str);\n" +
				"db =database(\"" + PATH + "/db1\");\n" +
				"saveTable(db,t,`tb)";
		stmt.execute(script);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");
		rs.next();
		for (int i=0;i<3;rs.next()) {
			TestCase.assertEquals((StrValue[i]), rs.getString("str"));
			TestCase.assertEquals((StrValue[i]), rs.getString(1));
			TestCase.assertEquals((StrValue[i]), rs.getObject("str").toString());
			TestCase.assertEquals((StrValue[i]), rs.getObject(1).toString());
			i++;
		}
		int i=3;
		TestCase.assertEquals(null, rs.getString("str"));
		TestCase.assertEquals(null, rs.getString(1));
		TestCase.assertEquals(null, rs.getObject("str"));
		TestCase.assertEquals(null, rs.getObject(1));
	}

	@Test
	public void Test_ResultSet_dfs_getDate() throws Exception {
		String[] DValue = {"2000-01-01", "2011-01-01", "2020-11-01", "2021-11-01"};
		String[] DValueObj = {"2000.01.01", "2011.01.01", "2020.11.01", "2021.11.01"};
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "c=[2000.01.01,2011.01.01,2020.11.01,2021.11.01];\n" +
				"t= table(c as date);\n" +
				"db =database(\"" + PATH + "/db1\");\n" +
				"saveTable(db,t,`tb)";
		stmt.execute(script);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");
		int i=0;
		while (rs.next()) {
			TestCase.assertEquals((DValue[i]), rs.getDate("date").toLocalDate().toString());
			TestCase.assertEquals((DValue[i]), rs.getDate(1).toLocalDate().toString());
			TestCase.assertEquals((DValue[i]), rs.getObject("date").toString());
			TestCase.assertEquals((DValue[i]), rs.getObject(1).toString());
			i++;
		}
	}

	@Test
	public void Test_ResultSet_dfs_getTime() throws Exception {
		Time[] TValue = {new Time(0, 0, 0), new Time(13, 41, 39), new Time(13, 41, 29), null};
		String[] TValueStr = {"00:00:00", "13:41:39", "13:41:29",""};
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "m=[00:00:00.000,13:41:39.989,13:41:29.989,NULL];\n" +
				"t= table(m as time);\n" +
				"db =database(\"" + PATH + "/db1\");\n" +
				"saveTable(db,t,`tb)";
		stmt.execute(script);
		stmt.execute("pt=loadTable('" + PATH + "/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");
		rs.next();
		for (int i = 0; i < 3; rs.next()) {
			TestCase.assertEquals((TValue[i]), rs.getTime("time"));
			TestCase.assertEquals((TValueStr[i]), rs.getString(1));
			TestCase.assertEquals((TValueStr[i]), rs.getObject("time").toString());
			TestCase.assertEquals((TValueStr[i]), rs.getObject(1).toString());
			i++;
		}
		int i = 3;
		TestCase.assertEquals((TValue[i]), rs.getTime("time"));
		TestCase.assertEquals(null, rs.getString(1));
		TestCase.assertEquals(null, rs.getObject("time"));
		TestCase.assertEquals(null, rs.getObject(1));
	}

	@Test
	public void Test_ResultSet_dfs_getTimeStamp() throws Exception {
		Timestamp[] TsValue = {new Timestamp(2012 - 1900, 5, 13, 13, 30, 10, 8000000), new Timestamp(2013 - 1900, 5, 13, 13, 30, 10, 7000000), new Timestamp(2012 - 1900, 5, 13, 13, 35, 10, 8000000), null};
		String[] TsValueStr = {"2012.06.13T13:30:10.008", "2013.06.13T13:30:10.007", "2012.06.13T13:35:10.008", ""};
		String[] TsValueStr1 = {"2012-06-13T13:30:10.008", "2013-06-13T13:30:10.007", "2012-06-13T13:35:10.008", ""};

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "n=[2012.06.13 13:30:10.008,2013.06.13 13:30:10.007,2012.06.13 13:35:10.008,];\n" +
				"t= table(n as ts);\n" +
				"db =database(\"" + PATH + "/db1\");\n" +
				"saveTable(db,t,`tb)";
		stmt.execute(script);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");
		rs.next();
		for (int i=0;i<3;rs.next()) {
			TestCase.assertEquals((TsValue[i]), rs.getTimestamp("ts"));
			TestCase.assertEquals((TsValueStr1[i]), rs.getString(1));
			TestCase.assertEquals((TsValueStr1[i]), rs.getObject("ts").toString());
			TestCase.assertEquals((TsValueStr1[i]), rs.getObject(1).toString());
			i++;
		}
		int i=3;
		TestCase.assertEquals((TsValue[i]), rs.getTimestamp("ts"));
		TestCase.assertEquals(null, rs.getString(1));
		TestCase.assertEquals(null, rs.getObject("ts"));
		TestCase.assertEquals(null, rs.getObject(1));

	}

	@Test
	public void Test_ResultSet_dfs_getMonth() throws Exception {
		int[] MValueY = new int[4];
		MValueY[0] = 2012;		MValueY[1] = 2012;		MValueY[2] = 2012;
		int[] MValueM = new int[4];
		MValueM[0] = 6;		MValueM[1] = 7;		MValueM[2] = 8;
		String[] MValueStr = {"2012.06M", "2012.07M", "2012.08M", ""};
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "o=[2012.06M,2012.07M,2012.08M,];\n" +
				"t= table(o as mon);\n" +
				"db =database(\"" + PATH + "/db1\");\n" +
				"saveTable(db,t,`tb)";
		stmt.execute(script);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");
		int i=0;
		while (rs.next()) {
			if (rs.getDate("mon") != null) {
				TestCase.assertEquals((MValueY[i]), rs.getDate("mon").toLocalDate().getYear());
				TestCase.assertEquals((MValueM[i]), rs.getDate(1).toLocalDate().getMonthValue());
				TestCase.assertEquals((MValueStr[i]), rs.getString(1));
				TestCase.assertEquals((MValueStr[i]), rs.getObject(1).toString());
				TestCase.assertEquals((MValueStr[i]), rs.getObject("mon").toString());
			}
			i++;
		}
	}

	@Test
	public void Test_ResultSet_dfs_getDateTime() throws Exception {
		String[] DtValueStr = {"2012-06-13T13:30:10", "2013-04-13T13:30:10", "2012-06-14T13:34:50", ""};
		String[] DtValueDate = {"2012-06-13", "2013-04-13", "2012-06-14", ""};
		String[] DtValueTime = {"13:30:10", "13:30:10", "13:34:50", ""};

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "q=[2012.06.13 13:30:10,2013.04.13 13:30:10,2012.06.14 13:34:50,];\n" +
				"t= table(q as dt);\n" +
				"db =database(\"" + PATH + "/db1\");\n" +
				"saveTable(db,t,`tb)";
		stmt.execute(script);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");
		rs.next();
		for (int i=0;i<3;rs.next()) {
				if (rs.getDate(1) != null) {
					TestCase.assertEquals(DtValueDate[i], rs.getDate(1).toLocalDate().toString());
					TestCase.assertEquals(DtValueTime[i], rs.getTime(1).toString());
				}
			TestCase.assertEquals((DtValueStr[i]), rs.getString("dt"));
			TestCase.assertEquals((DtValueStr[i]), rs.getObject("dt").toString());
			TestCase.assertEquals((DtValueStr[i]), rs.getObject(1).toString());
			i++;
		}
		int i=3;
		TestCase.assertEquals(null, rs.getString("dt"));
		TestCase.assertEquals(null, rs.getObject("dt"));
		TestCase.assertEquals(null, rs.getObject(1));
	}

	@Test
	public void Test_ResultSet_dfs_getMinute() throws Exception {
		String[] MinValueStr = {"13:30", "14:32", "11:38", ""};
		LocalTime[] SeValueL = new LocalTime[4];
		SeValueL[0] = LocalTime.of(13,30,00);		SeValueL[1] = LocalTime.of(14,32,00);		SeValueL[2] = LocalTime.of(11,38,00);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "r=13:30m 14:32m 11:38m NULL;\n" +
				"t= table(r as min);\n" +
				"db =database(\"" + PATH + "/db1\");\n" +
				"saveTable(db,t,`tb)";
		stmt.execute(script);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");
		rs.next();
		for (int i=0;i<3;rs.next()) {
			TestCase.assertEquals((java.sql.Time.valueOf(SeValueL[i])), rs.getTime(1));
			TestCase.assertEquals((MinValueStr[i]), rs.getString("min"));
			TestCase.assertEquals((MinValueStr[i]), rs.getObject("min").toString());
			TestCase.assertEquals((MinValueStr[i]), rs.getObject(1).toString());
			i++;
		}
		int i=3;
		TestCase.assertEquals(((SeValueL[i])), rs.getTime(1));
		TestCase.assertEquals(null, rs.getString("min"));
		TestCase.assertEquals(null, rs.getObject("min"));
		TestCase.assertEquals(null, rs.getObject(1));
	}

	@Test
	public void Test_ResultSet_dfs_getSecond() throws Exception {
		String[] SeValueStr = {"13:30:10", "13:32:10", "13:30:13", ""};
		LocalTime[] SeValueL = new LocalTime[4];
		SeValueL[0] = LocalTime.of(13,30,10);		SeValueL[1] = LocalTime.of(13,32,10);		SeValueL[2] = LocalTime.of(13,30,13);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "s=13:30:10 13:32:10 13:30:13 NULL\n" +
				"t= table(s as sec);\n" +
				"db =database(\"" + PATH + "/db1\");\n" +
				"saveTable(db,t,`tb)";
		stmt.execute(script);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");
		rs.next();
		for (int i=0;i<3;rs.next()) {
			TestCase.assertEquals((java.sql.Time.valueOf(SeValueL[i])), rs.getTime(1));
			TestCase.assertEquals((SeValueStr[i]), rs.getString("sec"));
			TestCase.assertEquals((SeValueStr[i]), rs.getObject("sec").toString());
			TestCase.assertEquals((SeValueStr[i]), rs.getObject(1).toString());
			i++;
		}
		int i=3;
		TestCase.assertEquals(null, rs.getTime(1));
		TestCase.assertEquals(null, rs.getString("sec"));
		TestCase.assertEquals(null, rs.getObject("sec"));
		TestCase.assertEquals(null, rs.getObject(1));
	}

	@Test
	public void Test_ResultSet_dfs_getNanotime() throws Exception {
		String[] NanoSValueStr = {"2012-06-13T13:30:10.008007006", "2013-06-14T12:36:10.003007006", "2002-06-14T13:35:12.058007006", ""};
		String[] NanoSValueL = {"2012-06-13 13:30:10.008007006", "2013-06-14 12:36:10.003007006", "2002-06-14 13:35:12.058007006", ""};
		LocalDateTime[] NanoSValueL1 = new LocalDateTime[4];
		NanoSValueL1[0] = LocalDateTime.of(2012,6,13,13,30,10,8007006);
		NanoSValueL1[1] = LocalDateTime.of(2013,06,14,12,36,10,3007006);
		NanoSValueL1[2] = LocalDateTime.of(2002,06,14,13,35,12,58007006);
		String[] NanoTValueStr = {"13:30:10.008007006", "13:31:12.008002006", "13:32:10.008207006", ""};
		LocalTime[] NanoTValueL = new LocalTime[4];
		NanoTValueL[0] = LocalTime.of(13,30,10,8007006);
		NanoTValueL[1] = LocalTime.of(13,31,12,8002006);
		NanoTValueL[3] = LocalTime.of(13,32,10,8207006);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "u=13:30:10.008007006 13:31:12.008002006  13:32:10.008207006 NULL\n" +
				"q=[2012.06.13 13:30:10.008007006,2013.06.14 12:36:10.003007006,2002.06.14 13:35:12.058007006,]\n" +
				"t= table(u as nanoT,q as nanoTS);\n" +
				"db =database(\"" + PATH + "/db1\");\n" +
				"saveTable(db,t,`tb)";
		stmt.execute(script);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");
		rs.next();
		for (int i=0;i<3;rs.next()) {
			TestCase.assertEquals(((NanoSValueL[i])), rs.getTimestamp(2).toString());
			TestCase.assertEquals((NanoSValueStr[i]), rs.getString("nanoTS"));
			//TestCase.assertEquals(((NanoTValueL[i])), rs.getTime(1));
			TestCase.assertEquals((NanoTValueStr[i]), rs.getString("nanoT"));
			TestCase.assertEquals(((NanoSValueStr[i])), rs.getObject(2).toString());
			TestCase.assertEquals((NanoSValueStr[i]), rs.getObject("nanoTS").toString());
			TestCase.assertEquals(((NanoTValueStr[i])), rs.getObject(1).toString());
			TestCase.assertEquals((NanoTValueStr[i]), rs.getObject("nanoT").toString());
			//System.out.println(rs.getType());
			i++;
		}
		int i=3;
		TestCase.assertEquals(null, rs.getTime(2));
		TestCase.assertEquals(null, rs.getString("nanoTS"));
		TestCase.assertEquals(null, rs.getTimestamp(1));
		TestCase.assertEquals(null, rs.getString("nanoT"));
		TestCase.assertEquals(null, rs.getObject(2));
		TestCase.assertEquals(null, rs.getObject("nanoTS"));
		TestCase.assertEquals(null, rs.getObject(1));
		TestCase.assertEquals(null, rs.getObject("nanoT"));
	}
	@Test
	public void Test_ResultSet_dfs_getBigDecimal() throws Exception {
		String[] MinValueStr = {"13:30", "14:32", "11:38", ""};
		LocalTime[] SeValueL = new LocalTime[4];
		SeValueL[0] = LocalTime.of(13,30,00);		SeValueL[1] = LocalTime.of(14,32,00);		SeValueL[2] = LocalTime.of(11,38,00);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "r=13:30m 14:32m 11:38m NULL;\n" +
				"t= table(r as min);\n" +
				"db =database(\"" + PATH + "/db1\");\n" +
				"saveTable(db,t,`tb)";
		stmt.execute(script);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");
		rs.next();
		for (int i=0;i<3;rs.next()) {
			TestCase.assertEquals((java.sql.Time.valueOf(SeValueL[i])), rs.getTime(1));
			TestCase.assertEquals((MinValueStr[i]), rs.getString("min"));
			TestCase.assertEquals((MinValueStr[i]), rs.getObject("min").toString());
			TestCase.assertEquals((MinValueStr[i]), rs.getObject(1).toString());
			i++;
		}
		int i=3;
		TestCase.assertEquals(((SeValueL[i])), rs.getTime(1));
		TestCase.assertEquals(null, rs.getString("min"));
		TestCase.assertEquals(null, rs.getObject("min"));
		TestCase.assertEquals(null, rs.getObject(1));
	}
	@Test
	public void Test_ResultSet_MetaData() throws Exception {
		CreateMemoryTable(HOST, PORT);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt ");

		int[] ColumnType = {16, 1, -6, 4, -5, 6, 8, 12, 91, 92, 93, 12, 93, 12, 12, 12, 12};
		String[] ColumnTypeName = {"DT_BOOL", "DT_BYTE", "DT_SHORT", "DT_INT", "DT_LONG", "DT_FLOAT", "DT_DOUBLE", "DT_STRING", "DT_DATE", "DT_TIME", "DT_TIMESTAMP", "DT_MONTH", "DT_DATETIME", "DT_NANOTIMESTAMP", "DT_MINUTE", "DT_SECOND", "DT_NANOTIME"};
		String[] ColName = new String[18];
		String [] CatalogName={"LOGICAL","INTEGRAL","INTEGRAL","INTEGRAL","INTEGRAL","FLOATING","FLOATING","LITERAL","TEMPORAL","TEMPORAL","TEMPORAL","TEMPORAL","TEMPORAL","TEMPORAL","TEMPORAL","TEMPORAL","TEMPORAL"};
		String [] colClassName = {"BasicBooleanVector","BasicByteVector","BasicShortVector","BasicIntVector","BasicLongVector","BasicFloatVector","BasicDoubleVector","BasicStringVector","BasicDateVector","BasicTimeVector","BasicTimestampVector","BasicMonthVector",
				"BasicDateTimeVector","BasicNanoTimestampVector","BasicMinuteVector","BasicSecondVector","BasicNanoTimeVector"};
		//getMetaData
		ResultSetMetaData rsmd = rs.getMetaData();
		int len = rsmd.getColumnCount();
		for (int j = 0; j < len; j++) {
			ColName[j] = "a" + (j + 3);
			TestCase.assertEquals(ColName[j], rsmd.getColumnName(j + 1));
			TestCase.assertEquals(ColName[j], rsmd.getColumnLabel(j + 1));
			TestCase.assertEquals(ColumnType[j], rsmd.getColumnType(j + 1));
			System.out.println( rsmd.getColumnType(j + 1));
			TestCase.assertEquals(ColumnTypeName[j], rsmd.getColumnTypeName(j + 1));
			TestCase.assertEquals(CatalogName[j], rsmd.getCatalogName(j + 1));
			TestCase.assertEquals(ColName[j], rsmd.getTableName(j + 1));
			TestCase.assertEquals("com.xxdb.data."+colClassName[j], rsmd.getColumnClassName(j + 1));
			TestCase.assertEquals(2,rsmd.isNullable(j+1));
			//TestCase.assertEquals(rsmd.getColumnDisplaySize(j+1));
			//TestCase.assertEquals(,rsmd.getPrecision(j+1));
			//TestCase.assertEquals(,rsmd.getScale(j+1));
			//TestCase.assertEquals(,rsmd.getSchemaName(j+1));
			TestCase.assertFalse(rsmd.isAutoIncrement(j+1));
			TestCase.assertTrue(rsmd.isCaseSensitive(j+1));
			TestCase.assertFalse(rsmd.isCurrency(j+1));
			//TestCase.assertTrue(rsmd.isDefinitelyWritable(j+1));
			TestCase.assertTrue(rsmd.isSearchable(j+1));
		//	TestCase.assertFalse(rsmd.isSigned(j+1));
			TestCase.assertFalse(rsmd.isWritable(j+1));
			//TestCase.assertTrue(rsmd.isReadOnly(j+1));
		}
		assertEquals(len, 17);
		//get alias
		Statement stmt1 = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		stmt1.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		ResultSet rs1 = stmt1.executeQuery("select a3 as a1,a4 as a2 from pt ");
		String[] ColLabel1 = {"a1","a2"};
		ResultSetMetaData rsmd1 = rs1.getMetaData();
		int len1 = rsmd1.getColumnCount();
		for (int j = 0; j < len1; j++) {
			TestCase.assertEquals(ColLabel1[j], rsmd1.getColumnName(j + 1));
			TestCase.assertEquals(ColLabel1[j], rsmd1.getColumnLabel(j + 1));
			TestCase.assertFalse(rsmd1.isReadOnly(j+1));
			//TestCase.assertTrue(rsmd1.isWritable(j+1));
		}
		assertEquals(len1, 2);
	}


	@Test
	public void Test_ResultSet_update() throws Exception {
		CreateMemoryTable(HOST, PORT);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_UPDATABLE,ResultSet.HOLD_CURSORS_OVER_COMMIT);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17 from pt");
		rs.next();
		rs.updateNString("a10", "ss");
		try{
			rs.updateString("a10","wew");
		}
		catch(Exception ex){
			System.out.println(ex.toString());
			assertEquals("java.sql.SQLException: Updating the table of ResultSet is not currently supported", ex.toString());
		}
		try{
			rs.updateRow();
		}
		catch(Exception ex){
			System.out.println(ex.toString());
			assertEquals("java.sql.SQLException: Updating the table of ResultSet is not currently supported", ex.toString());
		}
		try{
			rs.moveToInsertRow();
		}
		catch(Exception ex){
			System.out.println(ex.toString());
			assertEquals("java.sql.SQLException: Updating the table of ResultSet is not currently supported", ex.toString());
		}
		try{
			rs.moveToCurrentRow();
		}
		catch(Exception ex){
			System.out.println(ex.toString());
			assertEquals("java.sql.SQLException: Updating the table of ResultSet is not currently supported", ex.toString());
		}
	}


	@Test
	public void Test_ResultSet_wasNull() throws Exception {
		CreateMemoryTable(HOST, PORT);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_UPDATABLE,ResultSet.HOLD_CURSORS_OVER_COMMIT);
		stmt.execute("pt=loadTable('"+PATH +"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt");
		rs.next();
		int a = rs.getInt("a6");
		TestCase.assertFalse(rs.wasNull());
		rs.absolute(4);
		boolean b = rs.getBoolean(1);
		TestCase.assertTrue(rs.wasNull());
	}

	@Test
	public void Test_ResultSet_Browse() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "t= table(1..4 as id);\n" +"select * from t";
		rs = stmt.executeQuery(script);
		rs.beforeFirst();
		TestCase.assertTrue(rs.isBeforeFirst());
		rs.afterLast();
		TestCase.assertTrue(rs.isAfterLast());
		rs.last();
		TestCase.assertTrue(rs.isLast());
		rs.previous();
		TestCase.assertEquals(3, rs.getRow());
		rs.first();
		TestCase.assertTrue(rs.isFirst());
		rs.absolute(-2);
		TestCase.assertEquals(3, rs.getRow());
		rs.absolute(2);
		TestCase.assertEquals(2, rs.getRow());
		rs.relative(-1);
		TestCase.assertEquals(1, rs.getRow());
		rs.relative(3);
		TestCase.assertEquals(4, rs.getRow());
		rs.relative(0);
		TestCase.assertEquals(4, rs.getRow());
		//findColumn
		TestCase.assertEquals(1,rs.findColumn("id"));
	//	System.out.println(rs.getFetchSize());
		//TestCase.assertEquals(1,rs.getFetchSize());
	}

	@Test
	public void Test_ResultSet_setType() throws Exception {
		String script = "c=char(0 2 301 NULL);\n" +
			"t= table(c as char);\n" +
			"db =database(\"" + PATH + "/db1\");\n" +
			"saveTable(db,t,`tb)";
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.execute(script);
		stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
		rs = stmt.executeQuery("select * from pt");
		rs.last();
		rs.first();
	}

	//@Test(timeout = 60000)
	public void Test_ResultSet_bigData() throws Exception {
		String script = "\n" +
				"a = take(1 2 3 4 5,500000000);\n" +
				"t = table(a as id)\n" +
				"if(existsDatabase(\"dfs://db1\")){\n" +
				"\tdropDatabase(\"dfs://db1\")\n" +
				"}\n" +
				"db=database(\"dfs://db1\", VALUE, 1 2 3 4 5)\n" +
				"pt=db.createPartitionedTable(t, `pt, `id)\n" +
				"saveTable(\"dfs://db1\",t,`tb)";
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.execute(script);
		stmt.execute("tb=loadTable(\"dfs://db1\",`pt)");
		rs = stmt.executeQuery("select * from tb");
		int i=1;
		while (rs.next()){
			TestCase.assertEquals(i,rs.getRow());
			i++;
		}
	}

	@Test
	public void Test_ResultSet_Multiple_lines() throws SQLException, ClassNotFoundException {
		String script = "a = 1..10;" +
				"b = take(`Q`W`E,10);" +
				"c=1..10;" +
				"t = table(a as id,b as val,c as val1);" +
				"select id as ids "+
				",val as value, val1 as value1 "
				+" from t where id =1 and val1 =1;";
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		//stmt.execute(script);
		rs = stmt.executeQuery(script);
		int i=1;
		while (rs.next()){
			TestCase.assertEquals(i,rs.getRow());
			i++;
		}
	}

	@Test
	public void Test_ResultSet_Others() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "k=string(0 -2 301 NULL);\n" +
				"t= table(k as str);\n" +"select * from t ";
		rs = stmt.executeQuery(script);
		int i = 0;
		while (rs.next()) {
			TestCase.assertEquals(null, rs.getAsciiStream(1));
			TestCase.assertEquals(null, rs.getAsciiStream("str"));
			TestCase.assertEquals(null, rs.getUnicodeStream(1));
			TestCase.assertEquals(null, rs.getUnicodeStream("str"));
			TestCase.assertEquals(null, rs.getBinaryStream(1));
			TestCase.assertEquals(null, rs.getBinaryStream("str"));
			i++;
		}
		rs.clearWarnings();
		TestCase.assertEquals(null, rs.getWarnings());
	}
	@Test
	public void Test_ResultSet_getBigDecimal() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "k=string(0 -2 301111111 NULL);\n" +
				"t= table(k as str);\n" +"select * from t ";
		rs = stmt.executeQuery(script);
		rs.next();
		TestCase.assertEquals("0", rs.getBigDecimal("str" ).toString());
		rs.next();
		TestCase.assertEquals("-2", rs.getBigDecimal("str" ).toString());
		rs.next();
		TestCase.assertEquals("301111111", rs.getBigDecimal("str" ).toString());
		rs.next();
		TestCase.assertEquals(null, rs.getBigDecimal("str" ));
		rs.clearWarnings();
		TestCase.assertEquals(null, rs.getWarnings());
		//System.out.println(rs.getCursorName());
	}

	@Test
	public void Test_ResultSet_getObject_decimal32() throws Exception {
		String script = "dbName = \"dfs://test_resultSet_decimal\"\n" +
				"if(existsDatabase(dbName)){\n" +
				"\tdropDB(dbName)\n" +
				"}\n" +
				"db = database(dbName,VALUE,1..101)\n" +
				"t = table(1..100 as id,take(`a`b`c`d,100) as sym,decimal32(take(0..99,100),4) as decimal32)\n" +
				"pt = db.createPartitionedTable(t,`pt,`id).append!(t)";
		DBConnection DBconn = new DBConnection();
		DBconn.connect(HOST,PORT,"admin","123456");
		DBconn.run(script);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_UPDATABLE,ResultSet.HOLD_CURSORS_OVER_COMMIT);
		stmt.execute("pt=loadTable(\"dfs://test_resultSet_decimal\", 'pt')");
		rs = stmt.executeQuery("select * from pt");
		int i = 0;
		while (rs.next()) {
			TestCase.assertEquals("class java.math.BigDecimal", rs.getObject("decimal32").getClass().toString());
			TestCase.assertEquals(i + ".0000", rs.getObject("decimal32").toString());
			i++;
		}
	}
	@Test
	public void Test_ResultSet_getBigDecimal_decimal32() throws Exception {
		String script = "dbName = \"dfs://test_resultSet_decimal\"\n" +
				"if(existsDatabase(dbName)){\n" +
				"\tdropDB(dbName)\n" +
				"}\n" +
				"db = database(dbName,VALUE,1..101)\n" +
				"t = table(1..100 as id,take(`a`b`c`d,100) as sym,decimal32(take(0..99,100),4) as decimal32)\n" +
				"pt = db.createPartitionedTable(t,`pt,`id).append!(t)";
		DBConnection DBconn = new DBConnection();
		DBconn.connect(HOST,PORT,"admin","123456");
		DBconn.run(script);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_UPDATABLE,ResultSet.HOLD_CURSORS_OVER_COMMIT);
		stmt.execute("pt=loadTable(\"dfs://test_resultSet_decimal\", 'pt')");
		rs = stmt.executeQuery("select * from pt");
		int i = 0;
		while (rs.next()) {
			TestCase.assertEquals(i + ".0000", rs.getBigDecimal("decimal32").toString());
			i++;
		}
	}
	@Test
	public void Test_ResultSet_getObject_decimal64() throws Exception {
		String script = "dbName = \"dfs://test_resultSet_decimal\"\n" +
				"if(existsDatabase(dbName)){\n" +
				"\tdropDB(dbName)\n" +
				"}\n" +
				"db = database(dbName,VALUE,1..101)\n" +
				"t = table(1..3 as id,take(`a`b`c`d,3) as sym,decimal64(take(9.999999999 9999999999.3434 0.0000001,3),8) as decimal64)\n" +
				"pt = db.createPartitionedTable(t,`pt,`id).append!(t)";
		DBConnection DBconn = new DBConnection();
		DBconn.connect(HOST,PORT,"admin","123456");
		DBconn.run(script);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_UPDATABLE,ResultSet.HOLD_CURSORS_OVER_COMMIT);
		stmt.execute("pt=loadTable(\"dfs://test_resultSet_decimal\", 'pt')");
		rs = stmt.executeQuery("select * from pt");
		rs.next();
		TestCase.assertEquals("class java.math.BigDecimal", rs.getObject("decimal64").getClass().toString());
		TestCase.assertEquals("10.00000000", rs.getObject("decimal64").toString());
		rs.next();
		TestCase.assertEquals("9999999999.34339968", rs.getObject("decimal64").toString());
		rs.next();
		TestCase.assertEquals("1.0E-7", rs.getObject("decimal64").toString());
	}
	@Test
	public void Test_ResultSet_getBigDecimal_decimal64() throws Exception {
		String script = "dbName = \"dfs://test_resultSet_decimal\"\n" +
				"if(existsDatabase(dbName)){\n" +
				"\tdropDB(dbName)\n" +
				"}\n" +
				"db = database(dbName,VALUE,1..101)\n" +
				"t = table(1..3 as id,take(`a`b`c`d,3) as sym,decimal64(take(9.999999999 9999999999.3434 0.0000001,3),8) as decimal64)\n" +
				"pt = db.createPartitionedTable(t,`pt,`id).append!(t)";
		DBConnection DBconn = new DBConnection();
		DBconn.connect(HOST,PORT,"admin","123456");
		DBconn.run(script);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_UPDATABLE,ResultSet.HOLD_CURSORS_OVER_COMMIT);
		stmt.execute("pt=loadTable(\"dfs://test_resultSet_decimal\", 'pt')");
		rs = stmt.executeQuery("select * from pt");
		rs.next();
		TestCase.assertEquals("class java.math.BigDecimal", rs.getBigDecimal("decimal64").getClass().toString());
		TestCase.assertEquals("10.00000000", rs.getBigDecimal("decimal64").toString());
		rs.next();
		TestCase.assertEquals("9999999999.34339968", rs.getBigDecimal("decimal64").toString());
		rs.next();
		TestCase.assertEquals("1.0E-7", rs.getBigDecimal("decimal64").toString());
	}
	@Test
	public void Test_ResultSet_getObject_decimal128() throws Exception {
		String script = "dbName = \"dfs://test_resultSet_decimal\"\n" +
				"if(existsDatabase(dbName)){\n" +
				"\tdropDB(dbName)\n" +
				"}\n" +
				"db = database(dbName,VALUE,1..101)\n" +
				"t = table(1..3 as id,take(`a`b`c`d,3) as sym,decimal128(take(9.999999999 9999999999.3434 0.0000001,3),8) as decimal128)\n" +
				"pt = db.createPartitionedTable(t,`pt,`id).append!(t)";
		DBConnection DBconn = new DBConnection();
		DBconn.connect(HOST,PORT,"admin","123456");
		DBconn.run(script);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_UPDATABLE,ResultSet.HOLD_CURSORS_OVER_COMMIT);
		stmt.execute("pt=loadTable(\"dfs://test_resultSet_decimal\", 'pt')");
		rs = stmt.executeQuery("select * from pt");
		rs.next();
		TestCase.assertEquals("class java.math.BigDecimal", rs.getObject("decimal128").getClass().toString());
		TestCase.assertEquals("10.00000000", rs.getObject("decimal128").toString());
		rs.next();
		TestCase.assertEquals("9999999999.34339968", rs.getObject("decimal128").toString());
		rs.next();
		TestCase.assertEquals("1.0E-7", rs.getObject("decimal128").toString());
	}
	@Test
	public void Test_ResultSet_getBigDecimal_decimal128() throws Exception {
		String script = "dbName = \"dfs://test_resultSet_decimal\"\n" +
				"if(existsDatabase(dbName)){\n" +
				"\tdropDB(dbName)\n" +
				"}\n" +
				"db = database(dbName,VALUE,1..101)\n" +
				"t = table(1..3 as id,take(`a`b`c`d,3) as sym,decimal128(take(9.999999999 9999999999.3434 0.0000001,3),8) as decimal128)\n" +
				"pt = db.createPartitionedTable(t,`pt,`id).append!(t)";
		DBConnection DBconn = new DBConnection();
		DBconn.connect(HOST,PORT,"admin","123456");
		DBconn.run(script);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_UPDATABLE,ResultSet.HOLD_CURSORS_OVER_COMMIT);
		stmt.execute("pt=loadTable(\"dfs://test_resultSet_decimal\", 'pt')");
		rs = stmt.executeQuery("select * from pt");
		rs.next();
		TestCase.assertEquals("class java.math.BigDecimal", rs.getBigDecimal("decimal128").getClass().toString());
		TestCase.assertEquals("10.00000000", rs.getBigDecimal("decimal128").toString());
		rs.next();
		TestCase.assertEquals("9999999999.34339968", rs.getBigDecimal("decimal128").toString());
		rs.next();
		TestCase.assertEquals("1.0E-7", rs.getBigDecimal("decimal128").toString());
	}
	@Test
	public void Test_ResultSet_getBigDecimal_columnIndex() throws Exception {
		String script = "dbName = \"dfs://test_resultSet_decimal\"\n" +
				"if(existsDatabase(dbName)){\n" +
				"\tdropDB(dbName)\n" +
				"}\n" +
				"db = database(dbName,VALUE,1..101)\n" +
				"t = table(1..3 as id,take(`a`b`c`d,3) as sym,decimal128(take(9.999999999 9999999999.3434 0.0000001,3),8) as decimal128)\n" +
				"pt = db.createPartitionedTable(t,`pt,`id).append!(t)";
		DBConnection DBconn = new DBConnection();
		DBconn.connect(HOST,PORT,"admin","123456");
		DBconn.run(script);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_UPDATABLE,ResultSet.HOLD_CURSORS_OVER_COMMIT);
		stmt.execute("pt=loadTable(\"dfs://test_resultSet_decimal\", 'pt')");
		rs = stmt.executeQuery("select * from pt");
		rs.next();
		TestCase.assertEquals("class java.math.BigDecimal", rs.getBigDecimal(3).getClass().toString());
		TestCase.assertEquals("10.00000000", rs.getBigDecimal(3).toString());
		rs.next();
		TestCase.assertEquals("9999999999.34339968", rs.getBigDecimal(3).toString());
		rs.next();
		TestCase.assertEquals("1.0E-7", rs.getBigDecimal(3).toString());
	}

	@Test
	public void Test_ResultSet_getBigDecimal_not_support() throws Exception {
		String script = "dbName = \"dfs://test_resultSet_decimal\"\n" +
				"if(existsDatabase(dbName)){\n" +
				"\tdropDB(dbName)\n" +
				"}\n" +
				"db = database(dbName,VALUE,1..101)\n" +
				"t = table(1..3 as id,take(`a`b`c`d,3) as sym,decimal128(take(9.999999999 9999999999.3434 0.0000001,3),8) as decimal128)\n" +
				"pt = db.createPartitionedTable(t,`pt,`id).append!(t)";
		DBConnection DBconn = new DBConnection();
		DBconn.connect(HOST,PORT,"admin","123456");
		DBconn.run(script);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
				ResultSet.CONCUR_UPDATABLE,ResultSet.HOLD_CURSORS_OVER_COMMIT);
		stmt.execute("pt=loadTable(\"dfs://test_resultSet_decimal\", 'pt')");
		rs = stmt.executeQuery("select * from pt");
		rs.next();
		String re = null;
		try{
			TestCase.assertEquals("9.99999999", rs.getBigDecimal("decimal128",8).toString());
		}catch(Exception e){
			re = e.getMessage();
		}
		TestCase.assertEquals("The method 'getBigDecimal(String columnLabel, int scale)' is not supported.",re);
		try{
			TestCase.assertEquals("9999999999.34339968", rs.getBigDecimal(3,8).toString());
		}catch(Exception e){
			re = e.getMessage();
		}
		TestCase.assertEquals("The method 'getBigDecimal(int columnIndex, int scale)' is not supported.",re);
	}
	@Test
	public void Test_getFetchDirection() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "t= table(1..4 as id);\n" +"select * from t";
		rs = stmt.executeQuery(script);
		rs.setFetchDirection(1);
		TestCase.assertEquals(1000, rs.getFetchDirection());
	}
	@Test
	public void Test_getFetchSize() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "t= table(1..4 as id);\n" +"select * from t";
		rs = stmt.executeQuery(script);
		rs.setFetchSize(1);
		TestCase.assertEquals(0, rs.getFetchSize());
	}
	@Test
	public void Test_getType() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "t= table(1..4 as id);\n" +"select * from t";
		rs = stmt.executeQuery(script);
		TestCase.assertEquals(1005, rs.getType());
	}
	@Test
	public void Test_getConcurrency() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "t= table(1..4 as id);\n" +"select * from t";
		rs = stmt.executeQuery(script);
		TestCase.assertEquals(1007, rs.getConcurrency());
	}
	@Test
	public void Test_rowUpdated() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "t= table(1..4 as id);\n" +"select * from t";
		rs = stmt.executeQuery(script);
		TestCase.assertEquals(false, rs.rowUpdated());
	}
	@Test
	public void Test_rowInserted() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "t= table(1..4 as id);\n" +"select * from t";
		rs = stmt.executeQuery(script);
		TestCase.assertEquals(false, rs.rowInserted());
	}
	@Test
	public void Test_rowDeleted() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "t= table(1..4 as id);\n" +"select * from t";
		rs = stmt.executeQuery(script);
		TestCase.assertEquals(false, rs.rowDeleted());
	}
	@Test
	public void Test_updateNull() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "t= table(1..4 as id);\n" +"select * from t";
		rs = stmt.executeQuery(script);
		rs.updateNull(0);//方法未实现
		rs.next();
		int a = rs.getInt("id");
		TestCase.assertEquals(1, rs.getInt("id"));
	}
	@Ignore
	public void Test_updateBoolean() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "t= table(1..4 as id);\n" +"select * from t";
		rs = stmt.executeQuery(script);
		rs.updateBoolean(0,true);//方法未实现
		rs.next();
		int a = rs.getInt("id");
		TestCase.assertEquals(true, rs.getInt("id"));
	}
	@Ignore
	public void Test_insertRow() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String script = "t= table(1..4 as id);\n" +"select * from t";
		rs = stmt.executeQuery(script);
		rs.insertRow();
		rs.next();
		int a = rs.getInt("id");
		TestCase.assertEquals(true, rs.getInt("id"));
	}
	@Test
	public void Test_ResultSet_getObject_mouth() throws Exception {
		CreateDfsTable(HOST,PORT);
		List<YearMonth> res = new ArrayList<>();
		java.time.YearMonth YearMonth1 = java.time.YearMonth.of(1969, 1 );
		java.time.YearMonth YearMonth2 = java.time.YearMonth.of(1970, 1 );
		java.time.YearMonth YearMonth3 = java.time.YearMonth.of(2022, 10);
		java.time.YearMonth YearMonth4 = null;
		res.add(YearMonth1);
		res.add(YearMonth2);
		res.add(YearMonth3);
		res.add(YearMonth4);
		System.out.println(res);

		List<LocalDate> res1 = new ArrayList<>();
		LocalDate localDate1 = java.time.LocalDate.of(1969, 1, 1);
		LocalDate localDate2 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate3 = java.time.LocalDate.of(2022, 10, 1);
		LocalDate localDate4 = null;
		res1.add(localDate1);
		res1.add(localDate2);
		res1.add(localDate3);
		res1.add(localDate4);
		System.out.println(res1);

		List<LocalTime> res2 = new ArrayList<>();
		LocalTime LocalTime1 = java.time.LocalTime.of(0, 0, 0);
		LocalTime LocalTime2 = java.time.LocalTime.of(0, 0, 0);
		LocalTime LocalTime3 = java.time.LocalTime.of(0, 0, 0);
		LocalTime LocalTime4 = null;
		res2.add(LocalTime1);
		res2.add(LocalTime2);
		res2.add(LocalTime3);
		res2.add(LocalTime4);
		System.out.println(res2);

		List<LocalDateTime> res3 = new ArrayList<>();
		LocalDateTime LocalDateTime1 = java.time.LocalDateTime.of(1969, 1, 1,0, 0, 0);
		LocalDateTime LocalDateTime2 = java.time.LocalDateTime.of(1970, 1, 1,0, 0, 0);
		LocalDateTime LocalDateTime3 = java.time.LocalDateTime.of(2022, 10, 1,0, 0, 0);
		LocalDateTime LocalDateTime4 = null;
		res3.add(LocalDateTime1);
		res3.add(LocalDateTime2);
		res3.add(LocalDateTime3);
		res3.add(LocalDateTime4);
		System.out.println(res3);

		List<java.util.Date> res4 = new ArrayList<>();
		List<java.util.Date> res6 = new ArrayList<>();

		java.util.Date Date1 = Date.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date2 = Date.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date3 = Date.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date4 = null;
		res4.add(Date1);
		res4.add(Date2);
		res4.add(Date3);
		res4.add(Date4);
		System.out.println(res4);

		List<Timestamp> res5 = new ArrayList<>();
		Timestamp Timestamp1 = Timestamp.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp2 = Timestamp.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp3 = Timestamp.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp4 = null;
		res5.add(Timestamp1);
		res5.add(Timestamp2);
		res5.add(Timestamp3);
		res5.add(Timestamp4);
		System.out.println(res5);

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select cmonth from pt order by id");
		int i=0;
		while (rs.next()) {
			TestCase.assertEquals((res.get(i)), rs.getObject(1, YearMonth.class));
			TestCase.assertEquals((res1.get(i)), rs.getObject(1, LocalDate.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject(1, LocalTime.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject(1, LocalDateTime.class));
			//TestCase.assertEquals((res4.get(i)), rs.getObject(1, java.util.Date.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject(1, Timestamp.class));

			Object value= rs.getObject(1, java.time.LocalDate.class);
			System.out.println("--------");
			System.out.println(value);
			Object value2= rs.getObject(1, java.time.LocalTime.class);
			System.out.println(value2);
			Object value3= rs.getObject(1, java.time.LocalDateTime.class);
			System.out.println(value3);
			Object value4= rs.getObject(1, java.util.Date.class);
			System.out.println(value4);
			res6.add((java.util.Date) value4);
			Object value5= rs.getObject(1, Timestamp.class);
			System.out.println(value5);
			i++;
		}
		System.out.println(res6);
		TestCase.assertEquals(res6.toString(), res4.toString());
	}
	@Test
	public void Test_ResultSet_getObject_date() throws Exception {
		CreateDfsTable(HOST,PORT);
		List<YearMonth> res = new ArrayList<>();
		java.time.YearMonth YearMonth1 = java.time.YearMonth.of(1969, 8 );
		java.time.YearMonth YearMonth2 = java.time.YearMonth.of(1970, 1 );
		java.time.YearMonth YearMonth3 = java.time.YearMonth.of(2022, 9);
		java.time.YearMonth YearMonth4 = null;
		res.add(YearMonth1);
		res.add(YearMonth2);
		res.add(YearMonth3);
		res.add(YearMonth4);
		System.out.println(res);

		List<LocalDate> res1 = new ArrayList<>();
		LocalDate localDate1 = java.time.LocalDate.of(1969, 8, 16);
		LocalDate localDate2 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate3 = java.time.LocalDate.of(2022, 9, 30);
		LocalDate localDate4 = null;
		res1.add(localDate1);
		res1.add(localDate2);
		res1.add(localDate3);
		res1.add(localDate4);
		System.out.println(res1);

		List<LocalTime> res2 = new ArrayList<>();
		LocalTime LocalTime1 = java.time.LocalTime.of(0, 0, 0);
		LocalTime LocalTime2 = java.time.LocalTime.of(0, 0, 0);
		LocalTime LocalTime3 = java.time.LocalTime.of(0, 0, 0);
		LocalTime LocalTime4 = null;
		res2.add(LocalTime1);
		res2.add(LocalTime2);
		res2.add(LocalTime3);
		res2.add(LocalTime4);
		System.out.println(res2);

		List<LocalDateTime> res3 = new ArrayList<>();
		LocalDateTime LocalDateTime1 = java.time.LocalDateTime.of(1969, 8, 16,0, 0, 0);
		LocalDateTime LocalDateTime2 = java.time.LocalDateTime.of(1970, 1, 1,0, 0, 0);
		LocalDateTime LocalDateTime3 = java.time.LocalDateTime.of(2022, 9, 30,0, 0, 0);
		LocalDateTime LocalDateTime4 = null;
		res3.add(LocalDateTime1);
		res3.add(LocalDateTime2);
		res3.add(LocalDateTime3);
		res3.add(LocalDateTime4);
		System.out.println(res3);

		List<java.util.Date> res4 = new ArrayList<>();
		List<java.util.Date> res6 = new ArrayList<>();

		java.util.Date Date1 = Date.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date2 = Date.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date3 = Date.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date4 = null;
		res4.add(Date1);
		res4.add(Date2);
		res4.add(Date3);
		res4.add(Date4);
		System.out.println(res4);

		List<Timestamp> res5 = new ArrayList<>();
		Timestamp Timestamp1 = Timestamp.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp2 = Timestamp.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp3 = Timestamp.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp4 = null;
		res5.add(Timestamp1);
		res5.add(Timestamp2);
		res5.add(Timestamp3);
		res5.add(Timestamp4);
		System.out.println(res5);

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select cdate from pt order by id");
		int i=0;
		while (rs.next()) {
			TestCase.assertEquals((res.get(i)), rs.getObject(1, YearMonth.class));
			TestCase.assertEquals((res1.get(i)), rs.getObject(1, LocalDate.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject(1, LocalTime.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject(1, LocalDateTime.class));
			//TestCase.assertEquals((res4.get(i)), rs.getObject(1, java.util.Date.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject(1, Timestamp.class));
			Object value4= rs.getObject(1, java.util.Date.class);
			res6.add((java.util.Date) value4);
			i++;
		}
		System.out.println(res6);
		TestCase.assertEquals(res6.toString(), res4.toString());
	}
	@Test
	public void Test_ResultSet_getObject_time() throws Exception {
		CreateDfsTable(HOST,PORT);
		List<YearMonth> res = new ArrayList<>();
		java.time.YearMonth YearMonth1 = java.time.YearMonth.of(1970, 1 );
		java.time.YearMonth YearMonth2 = java.time.YearMonth.of(1970, 1 );
		java.time.YearMonth YearMonth3 = java.time.YearMonth.of(1970, 1);
		java.time.YearMonth YearMonth4 = null;
		res.add(YearMonth1);
		res.add(YearMonth2);
		res.add(YearMonth3);
		res.add(YearMonth4);
		System.out.println(res);

		List<LocalDate> res1 = new ArrayList<>();
		LocalDate localDate1 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate2 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate3 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate4 = null;
		res1.add(localDate1);
		res1.add(localDate2);
		res1.add(localDate3);
		res1.add(localDate4);
		System.out.println(res1);

		List<LocalTime> res2 = new ArrayList<>();
		LocalTime LocalTime1 = java.time.LocalTime.of(0, 0, 0,1000000);
		LocalTime LocalTime2 = java.time.LocalTime.of(0, 0, 0,1000000);
		LocalTime LocalTime3 = java.time.LocalTime.of(23, 59, 59,999000000);
		LocalTime LocalTime4 = null;
		res2.add(LocalTime1);
		res2.add(LocalTime2);
		res2.add(LocalTime3);
		res2.add(LocalTime4);
		System.out.println(res2);

		List<LocalDateTime> res3 = new ArrayList<>();
		LocalDateTime LocalDateTime1 = java.time.LocalDateTime.of(1970, 1, 1,0, 0, 0,1000000);
		LocalDateTime LocalDateTime2 = java.time.LocalDateTime.of(1970, 1, 1,0, 0, 0,1000000);
		LocalDateTime LocalDateTime3 = java.time.LocalDateTime.of(1970, 1, 1,23, 59, 59,999000000);
		LocalDateTime LocalDateTime4 = null;
		res3.add(LocalDateTime1);
		res3.add(LocalDateTime2);
		res3.add(LocalDateTime3);
		res3.add(LocalDateTime4);
		System.out.println(res3);

		List<java.util.Date> res4 = new ArrayList<>();
		List<java.util.Date> res6 = new ArrayList<>();

		java.util.Date Date1 = Date.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date2 = Date.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date3 = Date.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date4 = null;
		res4.add(Date1);
		res4.add(Date2);
		res4.add(Date3);
		res4.add(Date4);
		System.out.println(res4);

		List<Timestamp> res5 = new ArrayList<>();
		Timestamp Timestamp1 = Timestamp.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp2 = Timestamp.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp3 = Timestamp.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp4 = null;
		res5.add(Timestamp1);
		res5.add(Timestamp2);
		res5.add(Timestamp3);
		res5.add(Timestamp4);
		System.out.println(res5);

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select ctime from pt order by id");
		int i=0;
		while (rs.next()) {
			TestCase.assertEquals((res.get(i)), rs.getObject(1, YearMonth.class));
			TestCase.assertEquals((res1.get(i)), rs.getObject(1, LocalDate.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject(1, LocalTime.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject(1, LocalDateTime.class));
			//TestCase.assertEquals((res4.get(i)), rs.getObject(1, java.util.Date.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject(1, Timestamp.class));
			Object value= rs.getObject(1, java.time.LocalDate.class);
			System.out.println("--------");
			System.out.println(value);
			Object value2= rs.getObject(1, java.time.LocalTime.class);
			System.out.println(value2);
			Object value3= rs.getObject(1, java.time.LocalDateTime.class);
			System.out.println(value3);
			Object value4= rs.getObject(1, java.util.Date.class);
			System.out.println(value4);
			res6.add((java.util.Date) value4);
			Object value5= rs.getObject(1, Timestamp.class);
			System.out.println(value5);
			i++;
		}
		System.out.println(res6);
		TestCase.assertEquals(res6.toString(), res4.toString());
	}
	@Test
	public void Test_ResultSet_getObject_minute() throws Exception {
		CreateDfsTable(HOST,PORT);
		List<YearMonth> res = new ArrayList<>();
		java.time.YearMonth YearMonth1 = java.time.YearMonth.of(1970, 1 );
		java.time.YearMonth YearMonth2 = java.time.YearMonth.of(1970, 1 );
		java.time.YearMonth YearMonth3 = java.time.YearMonth.of(1970, 1);
		java.time.YearMonth YearMonth4 = null;
		res.add(YearMonth1);
		res.add(YearMonth2);
		res.add(YearMonth3);
		res.add(YearMonth4);
		System.out.println(res);

		List<LocalDate> res1 = new ArrayList<>();
		LocalDate localDate1 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate2 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate3 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate4 = null;
		res1.add(localDate1);
		res1.add(localDate2);
		res1.add(localDate3);
		res1.add(localDate4);
		System.out.println(res1);

		List<LocalTime> res2 = new ArrayList<>();
		LocalTime LocalTime1 = java.time.LocalTime.of(0, 1, 0,0);
		LocalTime LocalTime2 = java.time.LocalTime.of(0, 1, 0,0);
		LocalTime LocalTime3 = java.time.LocalTime.of(23, 59, 0,0);
		LocalTime LocalTime4 = null;
		res2.add(LocalTime1);
		res2.add(LocalTime2);
		res2.add(LocalTime3);
		res2.add(LocalTime4);
		System.out.println(res2);

		List<LocalDateTime> res3 = new ArrayList<>();
		LocalDateTime LocalDateTime1 = java.time.LocalDateTime.of(1970, 1, 1,0, 1, 0,0);
		LocalDateTime LocalDateTime2 = java.time.LocalDateTime.of(1970, 1, 1,0, 1, 0,0);
		LocalDateTime LocalDateTime3 = java.time.LocalDateTime.of(1970, 1, 1,23, 59, 0,0);
		LocalDateTime LocalDateTime4 = null;
		res3.add(LocalDateTime1);
		res3.add(LocalDateTime2);
		res3.add(LocalDateTime3);
		res3.add(LocalDateTime4);
		System.out.println(res3);

		List<java.util.Date> res4 = new ArrayList<>();
		List<java.util.Date> res6 = new ArrayList<>();

		java.util.Date Date1 = Date.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date2 = Date.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date3 = Date.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date4 = null;
		res4.add(Date1);
		res4.add(Date2);
		res4.add(Date3);
		res4.add(Date4);
		System.out.println(res4);

		List<Timestamp> res5 = new ArrayList<>();
		Timestamp Timestamp1 = Timestamp.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp2 = Timestamp.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp3 = Timestamp.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp4 = null;
		res5.add(Timestamp1);
		res5.add(Timestamp2);
		res5.add(Timestamp3);
		res5.add(Timestamp4);
		System.out.println(res5);

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select cminute from pt order by id");
		int i=0;
		while (rs.next()) {
			TestCase.assertEquals((res.get(i)), rs.getObject(1, YearMonth.class));
			TestCase.assertEquals((res1.get(i)), rs.getObject(1, LocalDate.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject(1, LocalTime.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject(1, LocalDateTime.class));
			//TestCase.assertEquals((res4.get(i)), rs.getObject(1, java.util.Date.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject(1, Timestamp.class));

			Object value= rs.getObject(1, java.time.LocalDate.class);
			System.out.println("--------");
			System.out.println(value);
			Object value2= rs.getObject(1, java.time.LocalTime.class);
			System.out.println(value2);
			Object value3= rs.getObject(1, java.time.LocalDateTime.class);
			System.out.println(value3);
			Object value4= rs.getObject(1, java.util.Date.class);
			System.out.println(value4);
			res6.add((java.util.Date) value4);
			Object value5= rs.getObject(1, Timestamp.class);
			System.out.println(value5);
			i++;
		}
		System.out.println(res6);
		TestCase.assertEquals(res6.toString(), res4.toString());
	}
	@Test
	public void Test_ResultSet_getObject_second() throws Exception {
		CreateDfsTable(HOST,PORT);
		List<YearMonth> res = new ArrayList<>();
		java.time.YearMonth YearMonth1 = java.time.YearMonth.of(1970, 1 );
		java.time.YearMonth YearMonth2 = java.time.YearMonth.of(1970, 1 );
		java.time.YearMonth YearMonth3 = java.time.YearMonth.of(1970, 1);
		java.time.YearMonth YearMonth4 = null;
		res.add(YearMonth1);
		res.add(YearMonth2);
		res.add(YearMonth3);
		res.add(YearMonth4);
		System.out.println(res);

		List<LocalDate> res1 = new ArrayList<>();
		LocalDate localDate1 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate2 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate3 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate4 = null;
		res1.add(localDate1);
		res1.add(localDate2);
		res1.add(localDate3);
		res1.add(localDate4);
		System.out.println(res1);

		List<LocalTime> res2 = new ArrayList<>();
		LocalTime LocalTime1 = java.time.LocalTime.of(0, 0, 1,0);
		LocalTime LocalTime2 = java.time.LocalTime.of(12, 0, 1,0);
		LocalTime LocalTime3 = java.time.LocalTime.of(23, 59, 59,0);
		LocalTime LocalTime4 = null;
		res2.add(LocalTime1);
		res2.add(LocalTime2);
		res2.add(LocalTime3);
		res2.add(LocalTime4);
		System.out.println(res2);

		List<LocalDateTime> res3 = new ArrayList<>();
		LocalDateTime LocalDateTime1 = java.time.LocalDateTime.of(1970, 1, 1,0, 0, 1,0);
		LocalDateTime LocalDateTime2 = java.time.LocalDateTime.of(1970, 1, 1,12, 0, 1,0);
		LocalDateTime LocalDateTime3 = java.time.LocalDateTime.of(1970, 1, 1,23, 59, 59,0);
		LocalDateTime LocalDateTime4 = null;
		res3.add(LocalDateTime1);
		res3.add(LocalDateTime2);
		res3.add(LocalDateTime3);
		res3.add(LocalDateTime4);
		System.out.println(res3);

		List<java.util.Date> res4 = new ArrayList<>();
		List<java.util.Date> res6 = new ArrayList<>();

		java.util.Date Date1 = Date.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date2 = Date.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date3 = Date.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date4 = null;
		res4.add(Date1);
		res4.add(Date2);
		res4.add(Date3);
		res4.add(Date4);
		System.out.println(res4);

		List<Timestamp> res5 = new ArrayList<>();
		Timestamp Timestamp1 = Timestamp.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp2 = Timestamp.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp3 = Timestamp.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp4 = null;
		res5.add(Timestamp1);
		res5.add(Timestamp2);
		res5.add(Timestamp3);
		res5.add(Timestamp4);
		System.out.println(res5);

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select csecond from pt order by id");
		int i=0;
		while (rs.next()) {
			TestCase.assertEquals((res.get(i)), rs.getObject(1, YearMonth.class));
			TestCase.assertEquals((res1.get(i)), rs.getObject(1, LocalDate.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject(1, LocalTime.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject(1, LocalDateTime.class));
			//TestCase.assertEquals((res4.get(i)), rs.getObject(1, java.util.Date.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject(1, Timestamp.class));

			Object value= rs.getObject(1, java.time.LocalDate.class);
			System.out.println("--------");
			System.out.println(value);
			Object value2= rs.getObject(1, java.time.LocalTime.class);
			System.out.println(value2);
			Object value3= rs.getObject(1, java.time.LocalDateTime.class);
			System.out.println(value3);
			Object value4= rs.getObject(1, java.util.Date.class);
			System.out.println(value4);
			res6.add((java.util.Date) value4);
			Object value5= rs.getObject(1, Timestamp.class);
			System.out.println(value5);
			i++;
		}
		System.out.println(res6);
		TestCase.assertEquals(res6.toString(), res4.toString());
	}
	@Test
	public void Test_ResultSet_getObject_datetime() throws Exception {
		CreateDfsTable(HOST,PORT);
		List<YearMonth> res = new ArrayList<>();
		java.time.YearMonth YearMonth1 = java.time.YearMonth.of(1969, 1 );
		java.time.YearMonth YearMonth2 = java.time.YearMonth.of(1970, 1 );
		java.time.YearMonth YearMonth3 = java.time.YearMonth.of(2022, 9);
		java.time.YearMonth YearMonth4 = null;
		res.add(YearMonth1);
		res.add(YearMonth2);
		res.add(YearMonth3);
		res.add(YearMonth4);
		System.out.println(res);

		List<LocalDate> res1 = new ArrayList<>();
		LocalDate localDate1 = java.time.LocalDate.of(1969, 1, 1);
		LocalDate localDate2 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate3 = java.time.LocalDate.of(2022, 9, 30);
		LocalDate localDate4 = null;
		res1.add(localDate1);
		res1.add(localDate2);
		res1.add(localDate3);
		res1.add(localDate4);
		System.out.println(res1);

		List<LocalTime> res2 = new ArrayList<>();
		LocalTime LocalTime1 = java.time.LocalTime.of(0, 0, 1,0);
		LocalTime LocalTime2 = java.time.LocalTime.of(0, 0, 1,0);
		LocalTime LocalTime3 = java.time.LocalTime.of(23, 59, 59,0);
		LocalTime LocalTime4 = null;
		res2.add(LocalTime1);
		res2.add(LocalTime2);
		res2.add(LocalTime3);
		res2.add(LocalTime4);
		System.out.println(res2);

		List<LocalDateTime> res3 = new ArrayList<>();
		LocalDateTime LocalDateTime1 = java.time.LocalDateTime.of(1969, 1, 1,0, 0, 1,0);
		LocalDateTime LocalDateTime2 = java.time.LocalDateTime.of(1970, 1, 1,0, 0, 1,0);
		LocalDateTime LocalDateTime3 = java.time.LocalDateTime.of(2022, 9, 30,23, 59, 59,0);
		LocalDateTime LocalDateTime4 = null;
		res3.add(LocalDateTime1);
		res3.add(LocalDateTime2);
		res3.add(LocalDateTime3);
		res3.add(LocalDateTime4);
		System.out.println(res3);

		List<java.util.Date> res4 = new ArrayList<>();
		List<java.util.Date> res6 = new ArrayList<>();

		java.util.Date Date1 = Date.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date2 = Date.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date3 = Date.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date4 = null;
		res4.add(Date1);
		res4.add(Date2);
		res4.add(Date3);
		res4.add(Date4);
		System.out.println(res4);

		List<Timestamp> res5 = new ArrayList<>();
		Timestamp Timestamp1 = Timestamp.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp2 = Timestamp.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp3 = Timestamp.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp4 = null;
		res5.add(Timestamp1);
		res5.add(Timestamp2);
		res5.add(Timestamp3);
		res5.add(Timestamp4);
		System.out.println(res5);

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select cdatetime from pt order by id");
		int i=0;
		while (rs.next()) {
			TestCase.assertEquals((res.get(i)), rs.getObject(1, YearMonth.class));
			TestCase.assertEquals((res1.get(i)), rs.getObject(1, LocalDate.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject(1, LocalTime.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject(1, LocalDateTime.class));
			//TestCase.assertEquals((res4.get(i)), rs.getObject(1, java.util.Date.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject(1, Timestamp.class));

			Object value= rs.getObject(1, java.time.LocalDate.class);
			System.out.println("--------");
			System.out.println(value);
			Object value2= rs.getObject(1, java.time.LocalTime.class);
			System.out.println(value2);
			Object value3= rs.getObject(1, java.time.LocalDateTime.class);
			System.out.println(value3);
			Object value4= rs.getObject(1, java.util.Date.class);
			System.out.println(value4);
			res6.add((java.util.Date) value4);
			Object value5= rs.getObject(1, Timestamp.class);
			System.out.println(value5);
			i++;
		}
		System.out.println(res6);
		TestCase.assertEquals(res6.toString(), res4.toString());
	}
	@Test
	public void Test_ResultSet_getObject_ctimestamp() throws Exception {
		CreateDfsTable(HOST,PORT);
		List<YearMonth> res = new ArrayList<>();
		java.time.YearMonth YearMonth1 = java.time.YearMonth.of(1969, 9 );
		java.time.YearMonth YearMonth2 = java.time.YearMonth.of(1970, 9 );
		java.time.YearMonth YearMonth3 = java.time.YearMonth.of(2022, 9);
		java.time.YearMonth YearMonth4 = null;
		res.add(YearMonth1);
		res.add(YearMonth2);
		res.add(YearMonth3);
		res.add(YearMonth4);
		System.out.println(res);

		List<LocalDate> res1 = new ArrayList<>();
		LocalDate localDate1 = java.time.LocalDate.of(1969, 9, 30);
		LocalDate localDate2 = java.time.LocalDate.of(1970, 9, 30);
		LocalDate localDate3 = java.time.LocalDate.of(2022, 9, 30);
		LocalDate localDate4 = null;
		res1.add(localDate1);
		res1.add(localDate2);
		res1.add(localDate3);
		res1.add(localDate4);
		System.out.println(res1);

		List<LocalTime> res2 = new ArrayList<>();
		LocalTime LocalTime1 = java.time.LocalTime.of(0, 0, 0,1000000);
		LocalTime LocalTime2 = java.time.LocalTime.of(0, 0, 0,1000000);
		LocalTime LocalTime3 = java.time.LocalTime.of(23, 59, 59,999000000);
		LocalTime LocalTime4 = null;
		res2.add(LocalTime1);
		res2.add(LocalTime2);
		res2.add(LocalTime3);
		res2.add(LocalTime4);
		System.out.println(res2);

		List<LocalDateTime> res3 = new ArrayList<>();
		LocalDateTime LocalDateTime1 = java.time.LocalDateTime.of(1969, 9, 30,0, 0, 0,1000000);
		LocalDateTime LocalDateTime2 = java.time.LocalDateTime.of(1970, 9, 30,0, 0, 0,1000000);
		LocalDateTime LocalDateTime3 = java.time.LocalDateTime.of(2022, 9, 30, 23, 59, 59,999000000);
		LocalDateTime LocalDateTime4 = null;
		res3.add(LocalDateTime1);
		res3.add(LocalDateTime2);
		res3.add(LocalDateTime3);
		res3.add(LocalDateTime4);
		System.out.println(res3);

		List<java.util.Date> res4 = new ArrayList<>();
		List<java.util.Date> res6 = new ArrayList<>();

		java.util.Date Date1 = Date.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date2 = Date.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date3 = Date.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date4 = null;
		res4.add(Date1);
		res4.add(Date2);
		res4.add(Date3);
		res4.add(Date4);
		System.out.println(res4);

		List<Timestamp> res5 = new ArrayList<>();
		Timestamp Timestamp1 = Timestamp.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp2 = Timestamp.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp3 = Timestamp.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp4 = null;
		res5.add(Timestamp1);
		res5.add(Timestamp2);
		res5.add(Timestamp3);
		res5.add(Timestamp4);
		System.out.println(res5);

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select ctimestamp from pt order by id");
		int i=0;
		while (rs.next()) {
			TestCase.assertEquals((res.get(i)), rs.getObject(1, YearMonth.class));
			TestCase.assertEquals((res1.get(i)), rs.getObject(1, LocalDate.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject(1, LocalTime.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject(1, LocalDateTime.class));
			//TestCase.assertEquals((res4.get(i)), rs.getObject(1, java.util.Date.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject(1, Timestamp.class));

			Object value= rs.getObject(1, java.time.LocalDate.class);
			System.out.println("--------");
			System.out.println(value);
			Object value2= rs.getObject(1, java.time.LocalTime.class);
			System.out.println(value2);
			Object value3= rs.getObject(1, java.time.LocalDateTime.class);
			System.out.println(value3);
			Object value4= rs.getObject(1, java.util.Date.class);
			System.out.println(value4);
			res6.add((java.util.Date) value4);
			Object value5= rs.getObject(1, Timestamp.class);
			System.out.println(value5);
			i++;
		}
		System.out.println(res6);
		TestCase.assertEquals(res6.toString(), res4.toString());
	}
	@Test
	public void Test_ResultSet_getObject_cnanotime() throws Exception {
		CreateDfsTable(HOST,PORT);
		List<YearMonth> res = new ArrayList<>();
		java.time.YearMonth YearMonth1 = java.time.YearMonth.of(1970, 1 );
		java.time.YearMonth YearMonth2 = java.time.YearMonth.of(1970, 1 );
		java.time.YearMonth YearMonth3 = java.time.YearMonth.of(1970, 1);
		java.time.YearMonth YearMonth4 = null;
		res.add(YearMonth1);
		res.add(YearMonth2);
		res.add(YearMonth3);
		res.add(YearMonth4);
		System.out.println(res);

		List<LocalDate> res1 = new ArrayList<>();
		LocalDate localDate1 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate2 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate3 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate4 = null;
		res1.add(localDate1);
		res1.add(localDate2);
		res1.add(localDate3);
		res1.add(localDate4);
		System.out.println(res1);

		List<LocalTime> res2 = new ArrayList<>();
		LocalTime LocalTime1 = java.time.LocalTime.of(23, 59, 58,1);
		LocalTime LocalTime2 = java.time.LocalTime.of(0, 0, 0,1);
		LocalTime LocalTime3 = java.time.LocalTime.of(23, 59, 58,7016);
		LocalTime LocalTime4 = null;
		res2.add(LocalTime1);
		res2.add(LocalTime2);
		res2.add(LocalTime3);
		res2.add(LocalTime4);
		System.out.println(res2);

		List<LocalDateTime> res3 = new ArrayList<>();
		LocalDateTime LocalDateTime1 = java.time.LocalDateTime.of(1970, 1, 1,23, 59, 58,1);
		LocalDateTime LocalDateTime2 = java.time.LocalDateTime.of(1970, 1, 1,0, 0, 0,1);
		LocalDateTime LocalDateTime3 = java.time.LocalDateTime.of(1970, 1, 1, 23, 59, 58,7016);
		LocalDateTime LocalDateTime4 = null;
		res3.add(LocalDateTime1);
		res3.add(LocalDateTime2);
		res3.add(LocalDateTime3);
		res3.add(LocalDateTime4);
		System.out.println(res3);

		List<java.util.Date> res4 = new ArrayList<>();
		List<java.util.Date> res6 = new ArrayList<>();

		java.util.Date Date1 = Date.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date2 = Date.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date3 = Date.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date4 = null;
		res4.add(Date1);
		res4.add(Date2);
		res4.add(Date3);
		res4.add(Date4);
		System.out.println(res4);

		List<Timestamp> res5 = new ArrayList<>();
		Timestamp Timestamp1 = Timestamp.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp2 = Timestamp.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp3 = Timestamp.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp4 = null;
		res5.add(Timestamp1);
		res5.add(Timestamp2);
		res5.add(Timestamp3);
		res5.add(Timestamp4);
		System.out.println(res5);

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select cnanotime from pt order by id");
		int i=0;
		while (rs.next()) {
			TestCase.assertEquals((res.get(i)), rs.getObject(1, YearMonth.class));
			TestCase.assertEquals((res1.get(i)), rs.getObject(1, LocalDate.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject(1, LocalTime.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject(1, LocalDateTime.class));
			//TestCase.assertEquals((res4.get(i)), rs.getObject(1, java.util.Date.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject(1, Timestamp.class));

			Object value= rs.getObject(1, java.time.LocalDate.class);
			System.out.println("--------");
			System.out.println(value);
			Object value2= rs.getObject(1, java.time.LocalTime.class);
			System.out.println(value2);
			Object value3= rs.getObject(1, java.time.LocalDateTime.class);
			System.out.println(value3);
			Object value4= rs.getObject(1, java.util.Date.class);
			System.out.println(value4);
			res6.add((java.util.Date) value4);
			Object value5= rs.getObject(1, Timestamp.class);
			System.out.println(value5);
			i++;
		}
		System.out.println(res6);
		TestCase.assertEquals(res6.toString(), res4.toString());
	}
	@Test
	public void Test_ResultSet_getObject_cnanotimestamp() throws Exception {
		CreateDfsTable(HOST,PORT);
		List<YearMonth> res = new ArrayList<>();
		java.time.YearMonth YearMonth1 = java.time.YearMonth.of(1969, 9);
		java.time.YearMonth YearMonth2 = java.time.YearMonth.of(1970, 1);
		java.time.YearMonth YearMonth3 = java.time.YearMonth.of(2022, 9);
		java.time.YearMonth YearMonth4 = null;
		res.add(YearMonth1);
		res.add(YearMonth2);
		res.add(YearMonth3);
		res.add(YearMonth4);
		System.out.println(res);

		List<LocalDate> res1 = new ArrayList<>();
		LocalDate localDate1 = java.time.LocalDate.of(1969, 9, 30);
		LocalDate localDate2 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate3 = java.time.LocalDate.of(2022, 9, 30);
		LocalDate localDate4 = null;
		res1.add(localDate1);
		res1.add(localDate2);
		res1.add(localDate3);
		res1.add(localDate4);
		System.out.println(res1);

		List<LocalTime> res2 = new ArrayList<>();
		LocalTime LocalTime1 = java.time.LocalTime.of(23, 59, 58,1);
		LocalTime LocalTime2 = java.time.LocalTime.of(23, 59, 58,1);
		LocalTime LocalTime3 = java.time.LocalTime.of(23, 59, 58,1112);
		LocalTime LocalTime4 = null;
		res2.add(LocalTime1);
		res2.add(LocalTime2);
		res2.add(LocalTime3);
		res2.add(LocalTime4);
		System.out.println(res2);

		List<LocalDateTime> res3 = new ArrayList<>();
		LocalDateTime LocalDateTime1 = java.time.LocalDateTime.of(1969, 9, 30,23, 59, 58,1);
		LocalDateTime LocalDateTime2 = java.time.LocalDateTime.of(1970, 1, 1,23, 59, 58,1);
		LocalDateTime LocalDateTime3 = java.time.LocalDateTime.of(2022, 9, 30, 23, 59, 58,1112);
		LocalDateTime LocalDateTime4 = null;
		res3.add(LocalDateTime1);
		res3.add(LocalDateTime2);
		res3.add(LocalDateTime3);
		res3.add(LocalDateTime4);
		System.out.println(res3);

		List<java.util.Date> res4 = new ArrayList<>();
		List<java.util.Date> res6 = new ArrayList<>();

		java.util.Date Date1 = Date.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date2 = Date.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date3 = Date.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date4 = null;
		res4.add(Date1);
		res4.add(Date2);
		res4.add(Date3);
		res4.add(Date4);
		System.out.println(res4);

		List<Timestamp> res5 = new ArrayList<>();
		Timestamp Timestamp1 = Timestamp.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp2 = Timestamp.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp3 = Timestamp.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp4 = null;
		res5.add(Timestamp1);
		res5.add(Timestamp2);
		res5.add(Timestamp3);
		res5.add(Timestamp4);
		System.out.println(res5);

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select cnanotimestamp from pt order by id");
		int i=0;
		while (rs.next()) {
			TestCase.assertEquals((res.get(i)), rs.getObject(1, YearMonth.class));
			TestCase.assertEquals((res1.get(i)), rs.getObject(1, LocalDate.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject(1, LocalTime.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject(1, LocalDateTime.class));
			//TestCase.assertEquals((res4.get(i)), rs.getObject(1, java.util.Date.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject(1, Timestamp.class));

			Object value= rs.getObject(1, java.time.LocalDate.class);
			System.out.println("--------");
			System.out.println(value);
			Object value2= rs.getObject(1, java.time.LocalTime.class);
			System.out.println(value2);
			Object value3= rs.getObject(1, java.time.LocalDateTime.class);
			System.out.println(value3);
			Object value4= rs.getObject(1, java.util.Date.class);
			System.out.println(value4);
			res6.add((java.util.Date) value4);
			Object value5= rs.getObject(1, Timestamp.class);
			System.out.println(value5);
			i++;
		}
		System.out.println(res6);
		TestCase.assertEquals(res6.toString(), res4.toString());
	}
	@Test
	public void Test_ResultSet_getObject_cdatehour() throws Exception {
		CreateDfsTable(HOST,PORT);
		List<YearMonth> res = new ArrayList<>();
		java.time.YearMonth YearMonth1 = java.time.YearMonth.of(1969, 1);
		java.time.YearMonth YearMonth2 = java.time.YearMonth.of(1970, 1);
		java.time.YearMonth YearMonth3 = java.time.YearMonth.of(2024, 1);
		java.time.YearMonth YearMonth4 = null;
		res.add(YearMonth1);
		res.add(YearMonth2);
		res.add(YearMonth3);
		res.add(YearMonth4);
		System.out.println(res);

		List<LocalDate> res1 = new ArrayList<>();
		LocalDate localDate1 = java.time.LocalDate.of(1969, 1, 1);
		LocalDate localDate2 = java.time.LocalDate.of(1970, 1, 1);
		LocalDate localDate3 = java.time.LocalDate.of(2024, 1, 1);
		LocalDate localDate4 = null;
		res1.add(localDate1);
		res1.add(localDate2);
		res1.add(localDate3);
		res1.add(localDate4);
		System.out.println(res1);

		List<LocalTime> res2 = new ArrayList<>();
		LocalTime LocalTime1 = java.time.LocalTime.of(1, 0, 0);
		LocalTime LocalTime2 = java.time.LocalTime.of(1, 0, 0);
		LocalTime LocalTime3 = java.time.LocalTime.of(1, 0, 0);
		LocalTime LocalTime4 = null;
		res2.add(LocalTime1);
		res2.add(LocalTime2);
		res2.add(LocalTime3);
		res2.add(LocalTime4);
		System.out.println(res2);

		List<LocalDateTime> res3 = new ArrayList<>();
		LocalDateTime LocalDateTime1 = java.time.LocalDateTime.of(1969, 1, 1,1, 0, 0);
		LocalDateTime LocalDateTime2 = java.time.LocalDateTime.of(1970, 1, 1,1, 0, 0);
		LocalDateTime LocalDateTime3 = java.time.LocalDateTime.of(2024, 1, 1,1, 0, 0);
		LocalDateTime LocalDateTime4 = null;
		res3.add(LocalDateTime1);
		res3.add(LocalDateTime2);
		res3.add(LocalDateTime3);
		res3.add(LocalDateTime4);
		System.out.println(res3);

		List<java.util.Date> res4 = new ArrayList<>();
		List<java.util.Date> res6 = new ArrayList<>();

		java.util.Date Date1 = Date.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date2 = Date.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date3 = Date.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		java.util.Date Date4 = null;
		res4.add(Date1);
		res4.add(Date2);
		res4.add(Date3);
		res4.add(Date4);
		System.out.println(res4);

		List<Timestamp> res5 = new ArrayList<>();
		Timestamp Timestamp1 = Timestamp.from(LocalDateTime1.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp2 = Timestamp.from(LocalDateTime2.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp3 = Timestamp.from(LocalDateTime3.atZone(ZoneId.systemDefault()).toInstant());
		Timestamp Timestamp4 = null;
		res5.add(Timestamp1);
		res5.add(Timestamp2);
		res5.add(Timestamp3);
		res5.add(Timestamp4);
		System.out.println(res5);

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select cdatehour from pt order by id");
		int i=0;
		while (rs.next()) {
			TestCase.assertEquals((res.get(i)), rs.getObject(1, YearMonth.class));
			TestCase.assertEquals((res1.get(i)), rs.getObject(1, LocalDate.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject(1, LocalTime.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject(1, LocalDateTime.class));
			//TestCase.assertEquals((res4.get(i)), rs.getObject(1, java.util.Date.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject(1, Timestamp.class));

			Object value= rs.getObject(1, java.time.LocalDate.class);
			System.out.println("--------");
			System.out.println(value);
			Object value2= rs.getObject(1, java.time.LocalTime.class);
			System.out.println(value2);
			Object value3= rs.getObject(1, java.time.LocalDateTime.class);
			System.out.println(value3);
			Object value4= rs.getObject(1, java.util.Date.class);
			System.out.println(value4);
			res6.add((java.util.Date) value4);
			Object value5= rs.getObject(1, Timestamp.class);
			System.out.println(value5);
			i++;
		}
		System.out.println(res6);
		TestCase.assertEquals(res6.toString(), res4.toString());
	}
	@Test
	public void Test_ResultSet_getObject_Boolean() throws Exception {
		CreateDfsTable(HOST,PORT);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select * from pt order by id");
		int i=0;
		List<Boolean> res1 = new ArrayList<>();
		res1.add(true);
		res1.add(null);
		res1.add(false);
		res1.add(false);

		List<Boolean> res2 = new ArrayList<>();
		res2.add(true);
		res2.add(true);
		res2.add(true);
		res2.add(null);

		List<Boolean> res3 = new ArrayList<>();
		res3.add(true);
		res3.add(true);
		res3.add(false);
		res3.add(null);

		List<Boolean> res4 = new ArrayList<>();
		res4.add(true);
		res4.add(true);
		res4.add(false);
		res4.add(null);

		List<Boolean> res5 = new ArrayList<>();
		res5.add(true);
		res5.add(true);
		res5.add(false);
		res5.add(null);

		List<Boolean> res6 = new ArrayList<>();
		res6.add(true);
		res6.add(false);
		res6.add(true);
		res6.add(null);

		List<Boolean> res7 = new ArrayList<>();
		res7.add(true);
		res7.add(false);
		res7.add(true);
		res7.add(null);

		List<Boolean> res8 = new ArrayList<>();
		res8.add(true);
		res8.add(false);
		res8.add(true);
		res8.add(null);
		while (rs.next()) {
			TestCase.assertEquals((res1.get(i)), rs.getObject("cbool", java.lang.Boolean.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject("cchar", java.lang.Boolean.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject("cshort", java.lang.Boolean.class));
			TestCase.assertEquals((res4.get(i)), rs.getObject("cint", java.lang.Boolean.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject("clong", java.lang.Boolean.class));
			TestCase.assertEquals((res6.get(i)), rs.getObject("cfloat", java.lang.Boolean.class));
			TestCase.assertEquals((res7.get(i)), rs.getObject("cdouble", java.lang.Boolean.class));
			TestCase.assertEquals((res8.get(i)), rs.getObject("cstring", java.lang.Boolean.class));
			try{
				Object value9 = rs.getObject("cdecimal32", java.lang.Boolean.class);
			}catch(Exception E){
				TestCase.assertEquals("java.io.IOException: com.xxdb.data.BasicDecimal32 can not cast  java.lang.Boolean",E.getMessage());
			}
			i++;
		}
	}
	@Test
	public void Test_ResultSet_getObject_Integer() throws Exception {
		CreateDfsTable(HOST,PORT);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select * from pt order by id");
		int i=0;
		List<Boolean> res1 = new ArrayList<>();
		res1.add(true);
		res1.add(null);
		res1.add(false);
		res1.add(false);

		List<Byte> res2 = new ArrayList<>();
		res2.add(new Byte("97"));
		res2.add(new Byte("48"));
		res2.add(new Byte("122"));
		res2.add(null);

		List<Integer> res3 = new ArrayList<>();
		res3.add(-1);
		res3.add(200);
		res3.add(0);
		res3.add(null);

		List<Integer> res4 = new ArrayList<>();
		res4.add(-1);
		res4.add(1000);
		res4.add(0);
		res4.add(null);

		List<Long> res5 = new ArrayList<>();
		res5.add(200L);
		res5.add(2000L);
		res5.add(0L);
		res5.add(null);

		List<Float> res6 = new ArrayList<>();
		res6.add(300.0F);
		res6.add(0.0F);
		res6.add((float) -2.0);
		res6.add(null);

		List<Double> res7 = new ArrayList<>();
		res7.add(230.0);
		res7.add(0.0);
		res7.add(-230.0);
		res7.add(null);

		List<Integer> res8 = new ArrayList<>();
		res8.add(123);
		res8.add(0);
		res8.add(-123);
		res8.add(null);
		while (rs.next()) {
			TestCase.assertEquals((res1.get(i)), rs.getObject("cbool", java.lang.Integer.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject("cchar", java.lang.Integer.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject("cshort", java.lang.Integer.class));
			TestCase.assertEquals((res4.get(i)), rs.getObject("cint", java.lang.Integer.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject("clong", java.lang.Integer.class));
			TestCase.assertEquals((res6.get(i)), rs.getObject("cfloat", java.lang.Integer.class));
			TestCase.assertEquals((res7.get(i)), rs.getObject("cdouble", java.lang.Integer.class));
			TestCase.assertEquals((res8.get(i)), rs.getObject("cstring", java.lang.Integer.class));
			try{
				Object value9 = rs.getObject("cdecimal32", java.lang.Integer.class);
			}catch(Exception E){
				TestCase.assertEquals("java.io.IOException: com.xxdb.data.BasicDecimal32 can not cast  java.lang.Integer",E.getMessage());
			}
			i++;
		}
	}
	@Test
	public void Test_ResultSet_getObject_Byte() throws Exception {
		CreateDfsTable(HOST,PORT);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select * from pt order by id");
		int i=0;
		List<Boolean> res1 = new ArrayList<>();
		res1.add(true);
		res1.add(null);
		res1.add(false);
		res1.add(false);

		List<Byte> res2 = new ArrayList<>();
		res2.add(new Byte("97"));
		res2.add(new Byte("48"));
		res2.add(new Byte("122"));
		res2.add(null);

		List<Integer> res3 = new ArrayList<>();
		res3.add(-1);
		res3.add(200);
		res3.add(0);
		res3.add(null);

		List<Integer> res4 = new ArrayList<>();
		res4.add(-1);
		res4.add(1000);
		res4.add(0);
		res4.add(null);

		List<Long> res5 = new ArrayList<>();
		res5.add(200L);
		res5.add(2000L);
		res5.add(0L);
		res5.add(null);

		List<Float> res6 = new ArrayList<>();
		res6.add(300.0F);
		res6.add(0.0F);
		res6.add((float) -2.0);
		res6.add(null);

		List<Double> res7 = new ArrayList<>();
		res7.add(230.0);
		res7.add(0.0);
		res7.add(-230.0);
		res7.add(null);

		List<Byte> res8 = new ArrayList<>();
		res8.add((byte) 123);
		res8.add((byte) 0);
		res8.add((byte) -123);
		res8.add(null);
		while (rs.next()) {
			TestCase.assertEquals((res1.get(i)), rs.getObject("cbool", java.lang.Byte.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject("cchar", java.lang.Byte.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject("cshort", java.lang.Byte.class));
			TestCase.assertEquals((res4.get(i)), rs.getObject("cint", java.lang.Byte.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject("clong", java.lang.Byte.class));
			TestCase.assertEquals((res6.get(i)), rs.getObject("cfloat", java.lang.Byte.class));
			TestCase.assertEquals((res7.get(i)), rs.getObject("cdouble", java.lang.Byte.class));
			TestCase.assertEquals((res8.get(i)), rs.getObject("cstring", java.lang.Byte.class));
			try{
				Object value9 = rs.getObject("cdecimal32", java.lang.Byte.class);
			}catch(Exception E){
				TestCase.assertEquals("java.io.IOException: com.xxdb.data.BasicDecimal32 can not cast  java.lang.Byte",E.getMessage());
			}
			i++;
		}
	}
	@Test
	public void Test_ResultSet_getObject_Character() throws Exception {
		CreateDfsTable(HOST,PORT);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select * from pt order by id");
		int i=0;
		List<Boolean> res1 = new ArrayList<>();
		res1.add(true);
		res1.add(null);
		res1.add(false);
		res1.add(false);

		List<Byte> res2 = new ArrayList<>();
		res2.add(new Byte("97"));
		res2.add(new Byte("48"));
		res2.add(new Byte("122"));
		res2.add(null);

		List<Integer> res3 = new ArrayList<>();
		res3.add(-1);
		res3.add(200);
		res3.add(0);
		res3.add(null);

		List<Integer> res4 = new ArrayList<>();
		res4.add(-1);
		res4.add(1000);
		res4.add(0);
		res4.add(null);

		List<Long> res5 = new ArrayList<>();
		res5.add(200L);
		res5.add(2000L);
		res5.add(0L);
		res5.add(null);

		List<Float> res6 = new ArrayList<>();
		res6.add(300.0F);
		res6.add(0.0F);
		res6.add((float) -2.0);
		res6.add(null);

		List<Double> res7 = new ArrayList<>();
		res7.add(230.0);
		res7.add(0.0);
		res7.add(-230.0);
		res7.add(null);

		List<Byte> res8 = new ArrayList<>();
		res8.add((byte) 123);
		res8.add((byte) 0);
		res8.add((byte) -123);
		res8.add(null);
		while (rs.next()) {
			TestCase.assertEquals((res1.get(i)), rs.getObject("cbool", java.lang.Character.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject("cchar", java.lang.Character.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject("cshort", java.lang.Character.class));
			TestCase.assertEquals((res4.get(i)), rs.getObject("cint", java.lang.Character.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject("clong", java.lang.Character.class));
			TestCase.assertEquals((res6.get(i)), rs.getObject("cfloat", java.lang.Character.class));
			TestCase.assertEquals((res7.get(i)), rs.getObject("cdouble", java.lang.Character.class));
			try{
				Object value = rs.getObject("cstring", java.lang.Character.class);
			}catch(Exception E){
				TestCase.assertEquals("java.io.IOException: com.xxdb.data.BasicString can not cast  java.lang.Character",E.getMessage());
			}
			try{
				Object value = rs.getObject("cdecimal32", java.lang.Character.class);
			}catch(Exception E){
				TestCase.assertEquals("java.io.IOException: com.xxdb.data.BasicDecimal32 can not cast  java.lang.Character",E.getMessage());
			}
			i++;
		}
	}
	@Test
	public void Test_ResultSet_getObject_Short() throws Exception {
		CreateDfsTable(HOST,PORT);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select * from pt order by id");
		int i=0;
		List<Boolean> res1 = new ArrayList<>();
		res1.add(true);
		res1.add(null);
		res1.add(false);
		res1.add(false);

		List<Byte> res2 = new ArrayList<>();
		res2.add(new Byte("97"));
		res2.add(new Byte("48"));
		res2.add(new Byte("122"));
		res2.add(null);

		List<Integer> res3 = new ArrayList<>();
		res3.add(-1);
		res3.add(200);
		res3.add(0);
		res3.add(null);

		List<Integer> res4 = new ArrayList<>();
		res4.add(-1);
		res4.add(1000);
		res4.add(0);
		res4.add(null);

		List<Long> res5 = new ArrayList<>();
		res5.add(200L);
		res5.add(2000L);
		res5.add(0L);
		res5.add(null);

		List<Float> res6 = new ArrayList<>();
		res6.add(300.0F);
		res6.add(0.0F);
		res6.add((float) -2.0);
		res6.add(null);

		List<Double> res7 = new ArrayList<>();
		res7.add(230.0);
		res7.add(0.0);
		res7.add(-230.0);
		res7.add(null);

		List<Short> res8 = new ArrayList<>();
		res8.add((short) 123);
		res8.add((short) 0);
		res8.add((short) -123);
		res8.add(null);
		while (rs.next()) {
			TestCase.assertEquals((res1.get(i)), rs.getObject("cbool", java.lang.Short.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject("cchar", java.lang.Short.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject("cshort", java.lang.Short.class));
			TestCase.assertEquals((res4.get(i)), rs.getObject("cint", java.lang.Short.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject("clong", java.lang.Short.class));
			TestCase.assertEquals((res6.get(i)), rs.getObject("cfloat", java.lang.Short.class));
			TestCase.assertEquals((res7.get(i)), rs.getObject("cdouble", java.lang.Short.class));
			TestCase.assertEquals((res8.get(i)), rs.getObject("cstring", java.lang.Short.class));

			try{
				Object value = rs.getObject("cdecimal32", java.lang.Short.class);
			}catch(Exception E){
				TestCase.assertEquals("java.io.IOException: com.xxdb.data.BasicDecimal32 can not cast  java.lang.Short",E.getMessage());
			}
			i++;
		}
	}
	@Test
	public void Test_ResultSet_getObject_Long() throws Exception {
		CreateDfsTable(HOST,PORT);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select * from pt order by id");
		int i=0;
		List<Boolean> res1 = new ArrayList<>();
		res1.add(true);
		res1.add(null);
		res1.add(false);
		res1.add(false);

		List<Byte> res2 = new ArrayList<>();
		res2.add(new Byte("97"));
		res2.add(new Byte("48"));
		res2.add(new Byte("122"));
		res2.add(null);

		List<Integer> res3 = new ArrayList<>();
		res3.add(-1);
		res3.add(200);
		res3.add(0);
		res3.add(null);

		List<Integer> res4 = new ArrayList<>();
		res4.add(-1);
		res4.add(1000);
		res4.add(0);
		res4.add(null);

		List<Long> res5 = new ArrayList<>();
		res5.add(200L);
		res5.add(2000L);
		res5.add(0L);
		res5.add(null);

		List<Float> res6 = new ArrayList<>();
		res6.add(300.0F);
		res6.add(0.0F);
		res6.add((float) -2.0);
		res6.add(null);

		List<Double> res7 = new ArrayList<>();
		res7.add(230.0);
		res7.add(0.0);
		res7.add(-230.0);
		res7.add(null);

		List<Long> res8 = new ArrayList<>();
		res8.add((long) 123);
		res8.add((long) 0);
		res8.add((long) -123);
		res8.add(null);
		while (rs.next()) {
			TestCase.assertEquals((res1.get(i)), rs.getObject("cbool", java.lang.Long.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject("cchar", java.lang.Long.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject("cshort", java.lang.Long.class));
			TestCase.assertEquals((res4.get(i)), rs.getObject("cint", java.lang.Long.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject("clong", java.lang.Long.class));
			TestCase.assertEquals((res6.get(i)), rs.getObject("cfloat", java.lang.Long.class));
			TestCase.assertEquals((res7.get(i)), rs.getObject("cdouble", java.lang.Long.class));
			TestCase.assertEquals((res8.get(i)), rs.getObject("cstring", java.lang.Long.class));
			try{
				Object value = rs.getObject("cdecimal32", java.lang.Long.class);
			}catch(Exception E){
				TestCase.assertEquals("java.io.IOException: com.xxdb.data.BasicDecimal32 can not cast  java.lang.Long",E.getMessage());
			}
			i++;
		}
	}
	@Test
	public void Test_ResultSet_getObject_Float() throws Exception {
		CreateDfsTable(HOST,PORT);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select * from pt order by id");
		int i=0;
		List<Boolean> res1 = new ArrayList<>();
		res1.add(true);
		res1.add(null);
		res1.add(false);
		res1.add(false);

		List<Byte> res2 = new ArrayList<>();
		res2.add(new Byte("97"));
		res2.add(new Byte("48"));
		res2.add(new Byte("122"));
		res2.add(null);

		List<Integer> res3 = new ArrayList<>();
		res3.add(-1);
		res3.add(200);
		res3.add(0);
		res3.add(null);

		List<Integer> res4 = new ArrayList<>();
		res4.add(-1);
		res4.add(1000);
		res4.add(0);
		res4.add(null);

		List<Long> res5 = new ArrayList<>();
		res5.add(200L);
		res5.add(2000L);
		res5.add(0L);
		res5.add(null);

		List<Float> res6 = new ArrayList<>();
		res6.add(300.0F);
		res6.add(0.0F);
		res6.add((float) -2.0);
		res6.add(null);

		List<Double> res7 = new ArrayList<>();
		res7.add(230.0);
		res7.add(0.0);
		res7.add(-230.0);
		res7.add(null);

		List<Float> res8 = new ArrayList<>();
		res8.add((float) 123);
		res8.add((float) 0);
		res8.add((float) -123);
		res8.add(null);
		while (rs.next()) {
			TestCase.assertEquals((res1.get(i)), rs.getObject("cbool", java.lang.Float.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject("cchar", java.lang.Float.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject("cshort", java.lang.Float.class));
			TestCase.assertEquals((res4.get(i)), rs.getObject("cint", java.lang.Float.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject("clong", java.lang.Float.class));
			TestCase.assertEquals((res6.get(i)), rs.getObject("cfloat", java.lang.Float.class));
			TestCase.assertEquals((res7.get(i)), rs.getObject("cdouble", java.lang.Float.class));
			TestCase.assertEquals((res8.get(i)), rs.getObject("cstring", java.lang.Float.class));
			try{
				Object value = rs.getObject("cdecimal32", java.lang.Float.class);
			}catch(Exception E){
				TestCase.assertEquals("java.io.IOException: com.xxdb.data.BasicDecimal32 can not cast  java.lang.Float",E.getMessage());
			}
			i++;
		}
	}
	@Test
	public void Test_ResultSet_getObject_Double() throws Exception {
		CreateDfsTable(HOST,PORT);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select * from pt order by id");
		int i=0;
		List<Boolean> res1 = new ArrayList<>();
		res1.add(true);
		res1.add(null);
		res1.add(false);
		res1.add(false);

		List<Byte> res2 = new ArrayList<>();
		res2.add(new Byte("97"));
		res2.add(new Byte("48"));
		res2.add(new Byte("122"));
		res2.add(null);

		List<Integer> res3 = new ArrayList<>();
		res3.add(-1);
		res3.add(200);
		res3.add(0);
		res3.add(null);

		List<Integer> res4 = new ArrayList<>();
		res4.add(-1);
		res4.add(1000);
		res4.add(0);
		res4.add(null);

		List<Long> res5 = new ArrayList<>();
		res5.add(200L);
		res5.add(2000L);
		res5.add(0L);
		res5.add(null);

		List<Float> res6 = new ArrayList<>();
		res6.add(300.0F);
		res6.add(0.0F);
		res6.add((float) -2.0);
		res6.add(null);

		List<Double> res7 = new ArrayList<>();
		res7.add(230.0);
		res7.add(0.0);
		res7.add(-230.0);
		res7.add(null);

		List<Double> res8 = new ArrayList<>();
		res8.add((double) 123);
		res8.add((double) 0);
		res8.add((double) -123);
		res8.add(null);
		while (rs.next()) {
			TestCase.assertEquals((res1.get(i)), rs.getObject("cbool", java.lang.Double.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject("cchar", java.lang.Double.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject("cshort", java.lang.Double.class));
			TestCase.assertEquals((res4.get(i)), rs.getObject("cint", java.lang.Double.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject("clong", java.lang.Double.class));
			TestCase.assertEquals((res6.get(i)), rs.getObject("cfloat", java.lang.Double.class));
			TestCase.assertEquals((res7.get(i)), rs.getObject("cdouble", java.lang.Double.class));
			TestCase.assertEquals((res8.get(i)), rs.getObject("cstring", java.lang.Double.class));
			try{
				Object value = rs.getObject("cdecimal32", java.lang.Double.class);
			}catch(Exception E){
				TestCase.assertEquals("java.io.IOException: com.xxdb.data.BasicDecimal32 can not cast java.lang.Double",E.getMessage());
			}
			i++;
		}
	}
	@Test
	public void Test_ResultSet_getObject_String() throws Exception {
		CreateDfsTable(HOST,PORT);
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("pt=loadTable(\"dfs://testResult\", 'pt')");
		rs = stmt.executeQuery("select * from pt order by id");
		int i=0;
		List<String> res1 = new ArrayList<>();
		res1.add("true");
		res1.add(null);
		res1.add("false");
		res1.add("false");

		List<String> res2 = new ArrayList<>();
		res2.add("'a'");
		res2.add("'0'");
		res2.add("'z'");
		res2.add(null);

		List<String> res3 = new ArrayList<>();
		res3.add("-1");
		res3.add("200");
		res3.add("0");
		res3.add(null);

		List<String> res4 = new ArrayList<>();
		res4.add("-1");
		res4.add("1000");
		res4.add("0");
		res4.add(null);

		List<String> res5 = new ArrayList<>();
		res5.add("200");
		res5.add("2000");
		res5.add("0");
		res5.add(null);

		List<String> res6 = new ArrayList<>();
		res6.add("300");
		res6.add("0");
		res6.add("-2");
		res6.add(null);

		List<String> res7 = new ArrayList<>();
		res7.add("230");
		res7.add("0");
		res7.add("-230");
		res7.add(null);

		List<String> res8 = new ArrayList<>();
		res8.add("123");
		res8.add("0");
		res8.add("-123");
		res8.add(null);
		while (rs.next()) {
			TestCase.assertEquals((res1.get(i)), rs.getObject("cbool", java.lang.String.class));
			TestCase.assertEquals((res2.get(i)), rs.getObject("cchar", java.lang.String.class));
			TestCase.assertEquals((res3.get(i)), rs.getObject("cshort", java.lang.String.class));
			TestCase.assertEquals((res4.get(i)), rs.getObject("cint", java.lang.String.class));
			TestCase.assertEquals((res5.get(i)), rs.getObject("clong", java.lang.String.class));
			TestCase.assertEquals((res6.get(i)), rs.getObject("cfloat", java.lang.String.class));
			TestCase.assertEquals((res7.get(i)), rs.getObject("cdouble", java.lang.String.class));
			TestCase.assertEquals((res8.get(i)), rs.getObject("cstring", java.lang.String.class));
			i++;
		}
	}

	@Test
	public void Test_ResultSet_getInt() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("intv = int(-2147483648 2147483647 0 -2147483647 -100 100 NULL);\nshortv = short(-32768  32767 0 -32767 -100 100 NULL);\n tt= table(intv,shortv);");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt ");
		rs.next();
		assertEquals(0, rs.getInt(1));
		assertEquals(0, rs.getInt(2));
		rs.next();
		assertEquals(2147483647, rs.getInt(1));
		assertEquals(32767, rs.getInt(2));
		rs.next();
		assertEquals(0, rs.getInt(1));
		assertEquals(0, rs.getInt(2));
		rs.next();
		assertEquals(-2147483647, rs.getInt(1));
		assertEquals(-32767, rs.getInt(2));
		rs.next();
		assertEquals(-100, rs.getInt(1));
		assertEquals(-100, rs.getInt(2));
		rs.next();
		assertEquals(100, rs.getInt(1));
		assertEquals(100, rs.getInt(2));
		rs.next();
		assertEquals(0, rs.getInt(1));
		assertEquals(0, rs.getInt(2));
	}
	@Test
	public void Test_ResultSet_getLong() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("intv = int(-2147483648 2147483647 0 -2147483647 -100 100 NULL);\nshortv = short(-32768  32767 0 -32767 -100 100 NULL);\n longv = long(-9223372036854775808  9223372036854775807 0 -9223372036854775807 -100 100 NULL);\n tt= table(intv,shortv,longv);");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt ");
		rs.next();
		assertEquals(0, rs.getLong(1));
		assertEquals(0, rs.getLong(2));
		assertEquals(0, rs.getLong(3));
		rs.next();
		assertEquals(2147483647, rs.getLong(1));
		assertEquals(32767, rs.getLong(2));
		assertEquals(9223372036854775807l, rs.getLong(3));
		rs.next();
		assertEquals(0, rs.getLong(1));
		assertEquals(0, rs.getLong(2));
		assertEquals(0, rs.getLong(3));
		rs.next();
		assertEquals(-2147483647, rs.getLong(1));
		assertEquals(-32767, rs.getLong(2));
		assertEquals(-9223372036854775807l, rs.getLong(3));
		rs.next();
		assertEquals(-100, rs.getLong(1));
		assertEquals(-100, rs.getLong(2));
		assertEquals(-100, rs.getLong(3));
		rs.next();
		assertEquals(100, rs.getLong(1));
		assertEquals(100, rs.getLong(2));
		assertEquals(100, rs.getLong(3));
		rs.next();
		assertEquals(0, rs.getLong(1));
		assertEquals(0, rs.getLong(2));
		assertEquals(0, rs.getLong(3));
	}
	@Test
	public void Test_ResultSet_getDouble() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("floatv = float(-2.14748364 21474.8364 0 -21474.83 100 NULL);\ndoublev = double(-2.14748364 21474.8364 0 -21474.83 100 NULL);\n tt= table(floatv,doublev);");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt ");
		rs.next();
		assertEquals(-2.14748364, rs.getDouble(1),4);
		assertEquals(-2.14748364, rs.getDouble(2),4);
		rs.next();
		assertEquals(21474.8364, rs.getDouble(1),4);
		assertEquals(21474.8364, rs.getDouble(2),4);
		rs.next();
		assertEquals(0, rs.getDouble(1),4);
		assertEquals(0, rs.getDouble(2),4);
		rs.next();
		assertEquals(-21474.83, rs.getDouble(1),4);
		assertEquals(-21474.83, rs.getDouble(2),4);
		rs.next();
		assertEquals(100, rs.getDouble(1),4);
		assertEquals(100, rs.getDouble(2),4);
		rs.next();
		assertEquals(0, rs.getDouble(1),4);
		assertEquals(0, rs.getDouble(2),4);
	}
	@Test
	public void Test_ResultSet_getBigDecimal_1() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("floatv = float(-2.14748364 21474.8364 0 -21474.83 100 NULL);\ndoublev = double(-2.14748364 21474.8364 0 -21474.83 100 NULL);\n tt= table(floatv,doublev);");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt ");
		rs.next();
		assertEquals("-2.1474835872650146", rs.getBigDecimal(1).toString());
		assertEquals("-2.14748364", rs.getBigDecimal(2).toString());
		rs.next();
		assertEquals("21474.8359375", rs.getBigDecimal(1).toString());
		assertEquals("21474.8364", rs.getBigDecimal(2).toString());
		rs.next();
		assertEquals("0.0", rs.getBigDecimal(1).toString());
		assertEquals("0.0", rs.getBigDecimal(2).toString());
		rs.next();
		assertEquals("-21474.830078125", rs.getBigDecimal(1).toString());
		assertEquals("-21474.83", rs.getBigDecimal(2).toString());
		rs.next();
		assertEquals("100.0", rs.getBigDecimal(1).toString());
		assertEquals("100.0", rs.getBigDecimal(2).toString());
		rs.next();
		assertEquals(null, rs.getBigDecimal(1));
		assertEquals(null, rs.getBigDecimal(2));
	}
	@Test
	public void Test_ResultSet_getString() throws Exception {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		stmt.execute("charv = char('1' join char());\n" +
				"symbolv = symbol(\"syms100\" join string());\n" +
				"stringv = string(\"stringv100\" join string());\n" +
				"uuidv = uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee89\", \"\"]);\n" +
				"datehourv = take(datehour(-1 join NULL), 2);\n" +
				"ippaddrv = ipaddr([\"192.168.100.14\", \"\"]);\n" +
				"int128v = int128([\"e1671797c52e15f763380b45e841ec35\", \"\"]);\n" +
				"blobv = blob(\"blobv100\" join string());\n" +
				"complexv = complex(-1, -1) join NULL;\n" +
				"pointv = point(-1, -1) join NULL;\n" +
				"tt = table(charv,symbolv,stringv,uuidv,datehourv,ippaddrv,int128v,blobv,complexv,pointv);");
		ResultSet rs = (ResultSet)stmt.executeQuery("select * from tt ");
		rs.next();
		assertEquals("49", rs.getString(1));
		assertEquals("syms100", rs.getString(2));
		assertEquals("stringv100", rs.getString(3));
		assertEquals("5d212a78-cc48-e3b1-4235-b4d91473ee89", rs.getString(4));
		assertEquals("1969-12-31T23:00", rs.getString(5));
		assertEquals("192.168.100.14", rs.getString(6));
		assertEquals("e1671797c52e15f763380b45e841ec35", rs.getString(7));
		assertEquals("blobv100", rs.getString(8));
		assertEquals("-1.0-1.0i", rs.getString(9));
		assertEquals("(-1.0, -1.0)", rs.getString(10));
		rs.next();
		assertEquals(null, rs.getString(1));
		assertEquals(null, rs.getString(2));
		assertEquals(null, rs.getString(3));
		assertEquals(null, rs.getString(4));
		assertEquals(null, rs.getString(5));
		assertEquals("0.0.0.0", rs.getString(6));
		assertEquals(null, rs.getString(7));
		assertEquals(null, rs.getString(8));
		assertEquals(null, rs.getString(9));
		assertEquals("(,)", rs.getString(10));
	}

	@Test
	public void Test_ResultSet_MatrixtoTable_null() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("");
		BasicTable re = (BasicTable)rs.getResult();
		System.out.println(re.getString());
		assertEquals("col0\n" + "----\n" + "    \n" ,re.getString());
	}

	@Test
	public void Test_ResultSet_MatrixtoTable_int() throws SQLException, ClassNotFoundException {

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(INT,2,2, ,16665);m[0,1]=null\n" +
				"m\n"+"m.rename!(\"aa\"\"bb\")");
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(INT,2,2, ,,)");
		BasicTable re = (BasicTable)rs.getResult();
		BasicTable re1 = (BasicTable)rs1.getResult();
		System.out.println(re.getString());
		assertEquals("  col0  col1 \n" +
				"- ----- -----\n" +
				"0 16665      \n" +
				"1 16665 16665\n",re.getString());
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_bool() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(BOOL,2,2,,true)\n m[0,1]=null\n m");
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(BOOL,2,2, ,,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		BasicTable re = (BasicTable)rs.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0 true     \n" +
				"1 true true\n",re.getString());
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_byte() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(CHAR,2,2,,'c');\n m[0,0]=null\n m");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(CHAR,2,2, ,,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0      'c' \n" +
				"1 'c'  'c' \n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_date() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery(" m=matrix(DATE,2,2,,2023.06.13)\n m[0,1]=null\n m;");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(DATE,2,2, ,,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0       col1      \n" +
				"- ---------- ----------\n" +
				"0 2023.06.13           \n" +
				"1 2023.06.13 2023.06.13\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_datehour() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("  m=matrix(DATEHOUR,2,2,,2012.06.13 13:30:10)\n" +
				" m[0,1]=null\n" +
				" m;");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(DATEHOUR,2,2,,,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		System.out.println(re.getString());
		assertEquals("  col0          col1         \n" +
				"- ------------- -------------\n" +
				"0 2012.06.13T13              \n" +
				"1 2012.06.13T13 2012.06.13T13\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_datetime() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery(" m=matrix(DATETIME,2,2,,2012.06.13 13:30:10)\n m[0,1]=null\n m;");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(DATETIME,2,2, ,,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0                col1               \n" +
				"- ------------------- -------------------\n" +
				"0 2012.06.13T13:30:10                    \n" +
				"1 2012.06.13T13:30:10 2012.06.13T13:30:10\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_minute() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery(" m=matrix(MINUTE,2,2,,2012.06.13 13:30:10)\n m[0,1]=null\n m;");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(MINUTE,2,2, ,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0   col1  \n" +
				"- ------ ------\n" +
				"0 13:30m       \n" +
				"1 13:30m 13:30m\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_time() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery(" m=matrix(TIME,2,2,,2012.06.13 13:30:10.005)\n m[0,1]=null\n m");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(TIME,2,2, ,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0         col1        \n" +
				"- ------------ ------------\n" +
				"0 13:30:10.005             \n" +
				"1 13:30:10.005 13:30:10.005\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_timestamp() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(TIMESTAMP,2,2,,2022.06.13 13:30:10.008)\n m[0,1]=null\n m");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(TIMESTAMP,2,2, ,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0                    col1                   \n" +
				"- ----------------------- -----------------------\n" +
				"0 2022.06.13T13:30:10.008                        \n" +
				"1 2022.06.13T13:30:10.008 2022.06.13T13:30:10.008\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_nanotime() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(nanotime,2,2,,13:30:10.252525255)\n m[0,1]=null\n m");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(nanotime,2,2, ,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0               col1              \n" +
				"- ------------------ ------------------\n" +
				"0 13:30:10.252525255                   \n" +
				"1 13:30:10.252525255 13:30:10.252525255\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_nanotimestamp() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(NANOTIMESTAMP,2,2,,2030.01.02 13:30:10.252525255)\n m[0,1]=null\n m");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(NANOTIMESTAMP,2,2, ,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0                          col1                         \n" +
				"- ----------------------------- -----------------------------\n" +
				"0 2030.01.02T13:30:10.252525255                              \n" +
				"1 2030.01.02T13:30:10.252525255 2030.01.02T13:30:10.252525255\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_decimal32() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(DECIMAL32(8),2,2,,5.52348648)\n m[0,1]=null\n m");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(DECIMAL32(8),2,2, ,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0       col1      \n" +
				"- ---------- ----------\n" +
				"0 5.52348648           \n" +
				"1 5.52348648 5.52348648\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_decimal64() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(DECIMAL64(18),2,2,,5.52348648864824558)\n m[0,1]=null\n m");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(DECIMAL64(18),2,2, ,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0                 col1                \n" +
				"- -------------------- --------------------\n" +
				"0 5.523486488648245248                     \n" +
				"1 5.523486488648245248 5.523486488648245248\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_decimal128() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(DECIMAL128(38),2,2,,decimal128(\"0.12345678912345678912345678912345678912\",38))\n m[0,1]=null\n m");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(DECIMAL128(38),2,2, ,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0                                     col1                                    \n" +
				"- ---------------------------------------- ----------------------------------------\n" +
				"0 0.12345678912345678912345678912345678912                                         \n" +
				"1 0.12345678912345678912345678912345678912 0.12345678912345678912345678912345678912\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_double() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(DOUBLE,2,2,,5.565653543687667)\n m[0,1]=null\n m");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(DOUBLE,2,2, ,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0       col1      \n" +
				"- ---------- ----------\n" +
				"0 5.56565354           \n" +
				"1 5.56565354 5.56565354\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_float() throws SQLException, ClassNotFoundException {

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(FLOAT,2,2,,0.56544f)\n m[0,1]=null\n m");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(FLOAT,2,2, ,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0    col1   \n" +
				"- ------- -------\n" +
				"0 0.56544        \n" +
				"1 0.56544 0.56544\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_long() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(LONG,2,2,,35687946468)\n m[0,1]=null\n m");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(LONG,2,2, ,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0        col1       \n" +
				"- ----------- -----------\n" +
				"0 35687946468            \n" +
				"1 35687946468 35687946468\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_month() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(MONTH,2,2,, 2012.12M)\n m[0,1]=null\n m");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(MONTH,2,2, ,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0     col1    \n" +
				"- -------- --------\n" +
				"0 2012.12M         \n" +
				"1 2012.12M 2012.12M\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_second() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(SECOND,2,2,, 13:30:10)\n m[0,1]=null\n m");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(SECOND,2,2, ,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0     col1    \n" +
				"- -------- --------\n" +
				"0 13:30:10         \n" +
				"1 13:30:10 13:30:10\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_short() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet)stmt.executeQuery("m=matrix(SHORT,2,2,,4548)\n m[0,1]=null\n m");
		BasicTable re = (BasicTable)rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(SHORT,2,2, ,)");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0 4548     \n" +
				"1 4548 4548\n",re.getString());
	}
	@Test
	public void Test_ResultSet_MatrixtoTable_string() throws SQLException, ClassNotFoundException {
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=matrix(STRING,2,2,,\"dd\")\n m[0,1]=null\n m");
		BasicTable re = (BasicTable) rs.getResult();
		JDBCResultSet rs1 = (JDBCResultSet)stmt.executeQuery("matrix(STRING,2,2, ,\"\" )");
		BasicTable re1 = (BasicTable)rs1.getResult();
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0          \n" +
				"1          \n",re1.getString());
		assertEquals("  col0 col1\n" +
				"- ---- ----\n" +
				"0 dd       \n" +
				"1 dd   dd  \n",re.getString());
		System.out.println(re.getString());
	}

	@Test
	public void Test_ResultSet_ScalartoTable_bool() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("true");
		BasicTable re = (BasicTable) rs.getResult();
		assertEquals("col0\n" +
				"----\n" +
				"true\n",re.getString());;
	}
	@Test
	public void Test_ResultSet_ScalartoTable_char() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("'a'");
		BasicTable re = (BasicTable) rs.getResult();
		assertEquals("col0\n" +
				"----\n" +
				"'a' \n",re.getString());;
		System.out.println(re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_short() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("122h");
		BasicTable re = (BasicTable) rs.getResult();
		assertEquals("col0\n" +
				"----\n" +
				"122 \n",re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_int() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("5");
		BasicTable re = (BasicTable) rs.getResult();
		assertEquals("col0\n" +
				"----\n" +
				"5   \n",re.getString());

	}
	@Test
	public void Test_ResultSet_ScalartoTable_long() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("22564l");
		BasicTable re = (BasicTable) rs.getResult();
		assertEquals("col0 \n"+"-----\n"+"22564\n",re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_date() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("2013.06.13");
		BasicTable re = (BasicTable) rs.getResult();
		int length="2013.06.13".length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, "2013.06.13");
		System.out.println(expected);
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_month() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String date="2012.06M";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(date);
		BasicTable re = (BasicTable) rs.getResult();
		int length=date.length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, date);
		assertEquals(expected,re.getString());
	}

	@Test
	public void Test_ResultSet_ScalartoTable_time() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String date="13:30:10.008";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(date);
		BasicTable re = (BasicTable) rs.getResult();
		int length=date.length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, date);
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_minute() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String date="13:30m";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(date);
		BasicTable re = (BasicTable) rs.getResult();
		int length=date.length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, date);
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_second() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String date="13:30:10";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(date);
		BasicTable re = (BasicTable) rs.getResult();
		int length=date.length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, date);
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_datetime() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String date="2012.06.13T13:30:10";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(date);
		BasicTable re = (BasicTable) rs.getResult();
		int length=date.length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, date);
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_timestamp() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String date="2012.06.13T13:30:10.008";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(date);
		BasicTable re = (BasicTable) rs.getResult();
		int length=date.length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, date);
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_nanotime() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String date="13:30:10.008007006";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(date);
		BasicTable re = (BasicTable) rs.getResult();
		int length=date.length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, date);
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_nanotimestamp() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String date="2012.06.13T13:30:10.008007006";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(date);
		BasicTable re = (BasicTable) rs.getResult();
		int length=date.length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, date);
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_datehour() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String date="datehour(2012.06.13 13:30:10)";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(date);
		BasicTable re = (BasicTable) rs.getResult();
		int length="2012.06.13T13".length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, "2012.06.13T13");
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_float() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		float t= 5.12F;
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(String.valueOf(t));
		BasicTable re = (BasicTable) rs.getResult();
		int length=String.valueOf(t).length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, t);
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_double() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String temp="5.2547";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(temp);
		BasicTable re = (BasicTable) rs.getResult();
		int length=temp.length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, temp);
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_string() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String temp="'hello'";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(temp);
		BasicTable re = (BasicTable) rs.getResult();
		int length=5;
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, "hello");
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_blob() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String temp="blob(\"hello\")";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(temp);
		BasicTable re = (BasicTable) rs.getResult();
		int length=5;
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, "hello");
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_int128() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String temp="uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\")";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(temp);
		BasicTable re = (BasicTable) rs.getResult();
		int length="9d457e79-1bed-d6c2-3612-b0d31c1881f6".length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, "9d457e79-1bed-d6c2-3612-b0d31c1881f6");
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_ipaddr() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String temp="ipaddr(\"192.168.1.13\")";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(temp);
		BasicTable re = (BasicTable) rs.getResult();
		int length="192.168.1.13".length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, "192.168.1.13");
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_point() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String temp="point(117.60972, 24.118418)";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(temp);
		BasicTable re = (BasicTable) rs.getResult();
		int length="(117.60972, 24.118418)".length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, "(117.60972, 24.118418)");
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_complex() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String temp="decimal32(25,6)";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(temp);
		BasicTable re = (BasicTable) rs.getResult();
		int length="25.000000".length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, "25.000000");
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_decimal32() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String temp="decimal32(25,6)";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(temp);
		BasicTable re = (BasicTable) rs.getResult();
		int length="25.000000".length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, "25.000000");
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_decimal64() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String temp="decimal64(2,18)";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(temp);
		BasicTable re = (BasicTable) rs.getResult();
		int length="2.000000000000000000".length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, "2.000000000000000000");
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_ScalartoTable_decimal128() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		String temp="decimal128(6,10)";
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery(temp);
		BasicTable re = (BasicTable) rs.getResult();
		int length="6.0000000000".length();
		String col0 = String.format("%-" + length + "s", "col0");
		String separator = String.format("%-" + length + "s", "").replace(' ', '-');
		String expected = String.format("%s\n%s\n%s\n", col0, separator, "6.0000000000");
		assertEquals(expected,re.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_bool() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(BOOL,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(BOOL,2,,1);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0\n" +
				"----\n" +
				"    \n" +
				"true\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_char() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(CHAR,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(CHAR,2,,'A');m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0\n" +
				"----\n" +
				"    \n" +
				"'A' \n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_short() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(short,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(short,2,,23);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0\n" +
				"----\n" +
				"    \n" +
				"23  \n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_int() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(INT,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(INT,2,,1);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0\n" +
				"----\n" +
				"    \n" +
				"1   \n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_long() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(LONG,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(LONG,2,,10);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0\n" +
				"----\n" +
				"    \n" +
				"10  \n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_date() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(DATE,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(DATE,2,,2013.06.13);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0      \n" +
				"----------\n" +
				"          \n" +
				"2013.06.13\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_month() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(MONTH,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(MONTH,2,,2012.06M);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals
				("col0    \n" +
				"--------\n" +
				"        \n" +
				"2012.06M\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_time() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(time,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(time,2,,13:30:10.008);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0        \n" +
				"------------\n" +
				"            \n" +
				"13:30:10.008\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_minute() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(minute,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(minute,2,,13:30:10.008);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0  \n" +
				"------\n" +
				"      \n" +
				"13:30m\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_second() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(second,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(second,2,,13:30:10.008);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0    \n" +
				"--------\n" +
				"        \n" +
				"13:30:10\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_datetime() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(datetime,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(datetime,2,,2012.06.13T13:30:10.008007006);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0               \n" +
				"-------------------\n" +
				"                   \n" +
				"2012.06.13T13:30:10\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_timestamp() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(timestamp,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(timestamp,2,,2012.06.13T13:30:10.008007006);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0                   \n" +
				"-----------------------\n" +
				"                       \n" +
				"2012.06.13T13:30:10.008\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_nanotime() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(nanotime,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(nanotime,2,,2012.06.13T13:30:10.008007006);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0              \n" +
				"------------------\n" +
				"                  \n" +
				"13:30:10.008007006\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_nanotimestamp() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(nanotimestamp,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(nanotimestamp,2,,2012.06.13T13:30:10.008007006);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0                         \n" +
				"-----------------------------\n" +
				"                             \n" +
				"2012.06.13T13:30:10.008007006\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_datehour() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(datehour,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(datehour,2,,2012.06.13T13:30:10.008007006);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0         \n" +
				"-------------\n" +
				"             \n" +
				"2012.06.13T13\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_float() throws SQLException, ClassNotFoundException{
		float a=32.57F;
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(float,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(float,2,,"+a+");m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0       \n" +
				"-----------\n" +
				"           \n" +
				"32.56999969\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_double() throws SQLException, ClassNotFoundException{

		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(double,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(double,2,,2);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0\n" +
				"----\n" +
				"    \n" +
				"2   \n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_symbol() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(symbol,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(symbol,2,,\"ddd\");m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0\n" +
				"----\n" +
				"    \n" +
				"ddd \n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_string() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=array(string,0);m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=array(string,2,,'dasearfaa');m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0     \n" +
				"---------\n" +
				"         \n" +
				"dasearfaa\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_int128() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=int128(array(string,0));m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=int128(array(string,2,,'e1671797c52e15f763380b45e841ec32'));m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0                            \n" +
				"--------------------------------\n" +
				"                                \n" +
				"e1671797c52e15f763380b45e841ec32\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_uuid() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=uuid(array(string,0));m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=uuid(array(string,2,,'5d212a78-cc48-e3b1-4235-b4d91473ee87'));m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0                                \n" +
				"------------------------------------\n" +
				"                                    \n" +
				"5d212a78-cc48-e3b1-4235-b4d91473ee87\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_ipaddr() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("m=ipaddr(array(string,0));m");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=ipaddr(array(string,2,,'192.168.1.13'));m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0        \n" +
				"------------\n" +
				"0.0.0.0     \n" +
				"192.168.1.13\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_point() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("array(point,0)");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=point(1..2,9..10);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0       \n" +
				"-----------\n" +
				"(,)        \n" +
				"(2.0, 10.0)\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_complex() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("array(complex,0)");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=complex(1 2,9 10);m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0     \n" +
				"---------\n" +
				"         \n" +
				"2.0+10.0i\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_decimal32() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("array(DECIMAL32(4),0)");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=[decimal32(42,2),decimal32(25,2)];m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0 \n" +
				"-----\n" +
				"     \n" +
				"25.00\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_decimal64() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("array(DECIMAL64(4),0)");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=[decimal64(42,4),decimal64(25,4)];m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0   \n" +
				"-------\n" +
				"       \n" +
				"25.0000\n",re1.getString());
	}
	@Test
	public void Test_ResultSet_VectortoTable_decimal128() throws SQLException, ClassNotFoundException{
		Class.forName(JDBC_DRIVER);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
		JDBCResultSet rs = (JDBCResultSet) stmt.executeQuery("array(DECIMAL128(4),0)");
		JDBCResultSet rs1 = (JDBCResultSet) stmt.executeQuery("m=[decimal128(42,5),decimal128(25,5)];m[0]=null;m");
		BasicTable re = (BasicTable) rs.getResult();
		BasicTable re1 = (BasicTable) rs1.getResult();
		assertEquals("col0\n" +
				"----\n" ,re.getString());
		assertEquals("col0    \n" +
				"--------\n" +
				"        \n" +
				"25.00000\n",re1.getString());
	}

}

