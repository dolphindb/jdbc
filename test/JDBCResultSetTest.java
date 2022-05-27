import com.xxdb.data.BasicBoolean;
import com.xxdb.data.BasicBooleanVector;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.sql.*;
import java.time.DateTimeException;
import java.time.LocalTime;
import java.time.Month;
import java.util.Properties;
import java.util.logging.Logger;

import com.xxdb.DBConnection;

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

	@Test
	public void Test_ResultSet_dfs_getFloat() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String[] fValueStr = {"0.102", "-2.2", "301", ""};
		String[] dValueStr = {"0.102", "-2", "301", ""};
		float[] fValue = new float[4];
		fValue[0] = 0.102f;		fValue[1] = -2.2f;		fValue[2] = 301f;
		double[] dValue = new double[4];
		dValue[0] = 0.102;		dValue[1] = -2;		dValue[2] = 301;
		try {
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
			int i=0;
			while (rs.next()) {
				TestCase.assertEquals((fValue[i]), rs.getFloat("f1"),3);
				TestCase.assertEquals((fValueStr[i]), rs.getString(1));
				TestCase.assertEquals((dValue[i]), rs.getDouble("d1"),3);
				TestCase.assertEquals((dValueStr[i]), rs.getString(2));
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_dfs_getBool() throws Exception {
		DBConnection myConn= new DBConnection();
		try {
			if (!myConn.connect(HOST, PORT, "admin", "123456")) {
				throw new IOException("Failed to connect to 2xdb server");
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String[] bValueStr = {"false", "true", "true", ""};
		boolean[] bValue = new boolean[4];
		bValue[0] = false;		bValue[1] = true;		bValue[2] = true;
		try {
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
			int i=0;
			while (rs.next()) {
				TestCase.assertEquals((bValue[i]), rs.getBoolean("bol"));
				TestCase.assertEquals((bValueStr[i]), rs.getObject("bol").toString());
				TestCase.assertEquals((bValueStr[i]), rs.getObject(1).toString());
				TestCase.assertEquals((bValueStr[i]), rs.getString(1));
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_dfs_getChar() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String[] cValueStr = {"0", "2", "'-'", ""};
		char[] cValue = new char[4];
		cValue[0] = 0;		cValue[1] = 2;		cValue[2] = '-';
		try {
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
			int i=0;
			while (rs.next()) {
				TestCase.assertEquals((cValue[i]), rs.getByte("char"));
				TestCase.assertEquals((cValueStr[i]), rs.getString(1));
				TestCase.assertEquals((cValueStr[i]), rs.getObject("char").toString());
				TestCase.assertEquals((cValueStr[i]), rs.getObject(1).toString());
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_dfs_getInt() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String[] sValueStr = {"0", "-2", "301", ""};
		String[] iValueStr = {"1", "2", "3", "4"};
		String[] lValueStr = {"0", "-2", "301", ""};
		short[] sValue = new short[4];
		sValue[0] = 0;		sValue[1] = -2;		sValue[2] = 301;
		int[] iValue = new int[4];
		iValue[0] = 1;		iValue[1] = 2;		iValue[2] = 3;		iValue[3] = 4;
		long[] lValue = new long[4];
		lValue[0] = 0;		lValue[1] = -2;		lValue[2] = 301;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			String script = "\n" +
					"f=short(0 -2 301 NULL);\n" +
					"g=int(1 2 3 4);\n" +
					"h=long(0 -2 301 NULL);" +
					"t= table(f as s,g as i,h as l);\n" +
					"db =database(\"" + PATH + "/db1\");\n" +
					"saveTable(db,t,`tb)";
			stmt.execute(script);
			stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
			rs = stmt.executeQuery("select * from pt ");
			int i=0;
			while (rs.next()) {
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
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_dfs_getString() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String[] StrValue = {"0", "-2", "301", ""};
		try {
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
			int i=0;
			while (rs.next()) {

				TestCase.assertEquals((StrValue[i]), rs.getString("str"));
				TestCase.assertEquals((StrValue[i]), rs.getString(1));
				TestCase.assertEquals((StrValue[i]), rs.getObject("str").toString());
				TestCase.assertEquals((StrValue[i]), rs.getObject(1).toString());
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_dfs_getDate() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String[] DValue = {"2000-01-01", "2011-01-01", "2020-11-01", "2021-11-01"};
		String[] DValueObj = {"2000.01.01", "2011.01.01", "2020.11.01", "2021.11.01"};
		try {
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
				TestCase.assertEquals((DValueObj[i]), rs.getObject("date").toString());
				TestCase.assertEquals((DValueObj[i]), rs.getObject(1).toString());
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_dfs_getTime() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		Time[] TValue = {new Time(0, 0, 0), null, new Time(13, 41, 39), new Time(13, 41, 29)};
		String[] TValueStr = {"00:00:00.000", "", "13:41:39.989", "13:41:29.989"};
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			String script = "m=[00:00:00.000,NULL,13:41:39.989,13:41:29.989];\n" +
					"t= table(m as time);\n" +
					"db =database(\"" + PATH + "/db1\");\n" +
					"saveTable(db,t,`tb)";
			stmt.execute(script);
			stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
			rs = stmt.executeQuery("select * from pt ");
			int i=0;
			while (rs.next()) {
				TestCase.assertEquals((TValue[i]), rs.getTime("time"));
				TestCase.assertEquals((TValueStr[i]), rs.getString(1));
				TestCase.assertEquals((TValueStr[i]), rs.getObject("time").toString());
				TestCase.assertEquals((TValueStr[i]), rs.getObject(1).toString());
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_dfs_getTimeStamp() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		Timestamp[] TsValue = {new Timestamp(2012 - 1900, 5, 13, 13, 30, 10, 8000000), new Timestamp(2013 - 1900, 5, 13, 13, 30, 10, 7000000), new Timestamp(2012 - 1900, 5, 13, 13, 35, 10, 8000000), null};
		String[] TsValueStr = {"2012.06.13T13:30:10.008", "2013.06.13T13:30:10.007", "2012.06.13T13:35:10.008", ""};

		try {
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
			int i=0;
			while (rs.next()) {
				TestCase.assertEquals((TsValue[i]), rs.getTimestamp("ts"));
				TestCase.assertEquals((TsValueStr[i]), rs.getString(1));
				TestCase.assertEquals((TsValueStr[i]), rs.getObject("ts").toString());
				TestCase.assertEquals((TsValueStr[i]), rs.getObject(1).toString());
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_dfs_getMonth() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		int[] MValueY = new int[4];
		MValueY[0] = 2012;		MValueY[1] = 2012;		MValueY[2] = 2012;
		int[] MValueM = new int[4];
		MValueM[0] = 6;		MValueM[1] = 7;		MValueM[2] = 8;
		String[] MValueStr = {"2012.06M", "2012.07M", "2012.08M", ""};
		try {
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
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_dfs_getDateTime() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String[] DtValueStr = {"2012.06.13T13:30:10", "", "2013.04.13T13:30:10", "2012.06.14T13:34:50"};
		String[] DtValueDate = {"2012-06-13", "", "2013-04-13", "2012-06-14"};
		String[] DtValueTime = {"13:30:10", "", "13:30:10", "13:34:50"};
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			String script = "q=[2012.06.13 13:30:10,,2013.04.13 13:30:10,2012.06.14 13:34:50];\n" +
					"t= table(q as dt);\n" +
					"db =database(\"" + PATH + "/db1\");\n" +
					"saveTable(db,t,`tb)";
			stmt.execute(script);
			stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
			rs = stmt.executeQuery("select * from pt ");
			int i=0;
			while (rs.next()) {
					if (rs.getDate(1) != null) {
						TestCase.assertEquals(DtValueDate[i], rs.getDate(1).toLocalDate().toString());
						TestCase.assertEquals(DtValueTime[i], rs.getTime(1).toString());
					}
				TestCase.assertEquals((DtValueStr[i]), rs.getString("dt"));
				TestCase.assertEquals((DtValueStr[i]), rs.getObject("dt").toString());
				TestCase.assertEquals((DtValueStr[i]), rs.getObject(1).toString());
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_dfs_getMinute() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String[] MinValueStr = {"13:30m", "14:32m", "11:38m", ""};
		long[] MinValueL = new long[4];
		MinValueL[0] = 810;		MinValueL[1] = 872;		MinValueL[2] = 698;
		try {
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
			int i=0;
			while (rs.next()) {
				TestCase.assertEquals(((MinValueL[i])), rs.getLong(1));
				TestCase.assertEquals((MinValueStr[i]), rs.getString("min"));
				TestCase.assertEquals((MinValueStr[i]), rs.getObject("min").toString());
				TestCase.assertEquals((MinValueStr[i]), rs.getObject(1).toString());
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_dfs_getSecond() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String[] SeValueStr = {"13:30:10", "13:32:10", "13:30:13", ""};
		long[] SeValueL = new long[4];
		SeValueL[0] = 48610;		SeValueL[1] = 48730;		SeValueL[2] = 48613;
		try {
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
			int i=0;
			while (rs.next()) {
				TestCase.assertEquals(((SeValueL[i])), rs.getLong(1));
				TestCase.assertEquals((SeValueStr[i]), rs.getString("sec"));
				TestCase.assertEquals((SeValueStr[i]), rs.getObject("sec").toString());
				TestCase.assertEquals((SeValueStr[i]), rs.getObject(1).toString());
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_dfs_getNanotime() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String[] NanoSValueStr = {"2012.06.13T13:30:10.008007006", "2013.06.14T12:36:10.003007006", "2002.06.14T13:35:12.058007006", ""};
		long[] NanoSValueL = new long[4];
		NanoSValueL[0] = 1339594210008007006l;		NanoSValueL[1] = 1371213370003007006l;		NanoSValueL[2] = 1024061712058007006l;
		String[] NanoTValueStr = {"13:30:10.008007006", "13:31:12.008002006", "", "13:32:10.008207006"};
		long[] NanoTValueL = new long[4];
		NanoTValueL[0] = 48610008007006l;		NanoTValueL[1] = 48672008002006l;		NanoTValueL[3] = 48730008207006l;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			String script = "u=13:30:10.008007006 13:31:12.008002006 NULL 13:32:10.008207006\n" +
					"q=[2012.06.13 13:30:10.008007006,2013.06.14 12:36:10.003007006,2002.06.14 13:35:12.058007006,]\n" +
					"t= table(u as nanoT,q as nanoTS);\n" +
					"db =database(\"" + PATH + "/db1\");\n" +
					"saveTable(db,t,`tb)";
			stmt.execute(script);
			stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
			rs = stmt.executeQuery("select * from pt ");
			int i=0;
			while (rs.next()) {
				TestCase.assertEquals(((NanoSValueL[i])), rs.getLong(2));
				TestCase.assertEquals((NanoSValueStr[i]), rs.getString("nanoTS"));
				TestCase.assertEquals(((NanoTValueL[i])), rs.getLong(1));
				TestCase.assertEquals((NanoTValueStr[i]), rs.getString("nanoT"));
				TestCase.assertEquals(((NanoSValueStr[i])), rs.getObject(2).toString());
				TestCase.assertEquals((NanoSValueStr[i]), rs.getObject("nanoTS").toString());
				TestCase.assertEquals(((NanoTValueStr[i])), rs.getObject(1).toString());
				TestCase.assertEquals((NanoTValueStr[i]), rs.getObject("nanoT").toString());
				//System.out.println(rs.getType());
				i++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_MetaData() throws Exception {
		CreateMemoryTable(HOST, PORT);
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
			stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
			rs = stmt.executeQuery("select * from pt ");

			int[] ColumnType = {1, 2, 3, 4, 5, 15, 16, 18, 6, 8, 12, 7, 11, 14, 9, 10, 13};
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
			org.junit.Assert.assertEquals(len, 17);
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
			org.junit.Assert.assertEquals(len1, 2);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
					rs.close();
					stmt.close();
					conn.close();
			}
		}


	@Test
	public void Test_ResultSet_update() throws Exception {
		CreateMemoryTable(HOST, PORT);
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
					ResultSet.CONCUR_UPDATABLE,ResultSet.HOLD_CURSORS_OVER_COMMIT);
			stmt.execute("pt=loadTable('"+PATH+"/db1', 'tb')");
			rs = stmt.executeQuery("select a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17 from pt");
			rs.next();
			rs.updateNString("a10", "ss");
			rs.updateString("a10","wew");
			rs.updateRow();
			rs.moveToInsertRow();
			rs.moveToCurrentRow();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}


	@Test
	public void Test_ResultSet_wasNull() throws Exception {
		CreateMemoryTable(HOST, PORT);
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
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
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_Browse() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
	try {
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

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_setType() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
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
		//	TestCase.assertFalse(rs.isFirst());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_bigData() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
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
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_Multiple_lines() throws SQLException {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
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
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

	@Test
	public void Test_ResultSet_Others() throws SQLException {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
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
				TestCase.assertEquals(null, rs.getBigDecimal(1));
				TestCase.assertEquals(null, rs.getBigDecimal("str" ));
				TestCase.assertEquals(null, rs.getBigDecimal(1, 1));
				TestCase.assertEquals(null, rs.getBigDecimal("str", 1));
				i++;
			}
			rs.clearWarnings();
			TestCase.assertEquals(null, rs.getWarnings());
			//System.out.println(rs.getCursorName());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			rs.close();
			stmt.close();
			conn.close();
		}
	}

}