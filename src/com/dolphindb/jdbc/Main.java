package com.dolphindb.jdbc;

import com.xxdb.DBConnection;
import com.xxdb.data.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class Main {
	private static final String preLoadTable = null;
	
	private static final String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
	
	private static final String path_All = "/data/dballdata";
	private static final String path = "C:/DolphinDB/Data/send/0717-1";

	private static final String DB_URL = MessageFormat.format("jdbc:dolphindb://172.16.95.128:8920?databasePath={0}",
			path); // System.getProperty("user.dir").replaceAll("\\\\","/"),path_All);

	private static final String DB_URL1 = "jdbc:dolphindb://";

	private static final String DB_URL_DFS = "jdbc:dolphindb://172.16.95.128:8921?databasePath=dfs://valuedb&partitionType=VALUE&partitionScheme=2000.01M..2016.12M";

	private static final String DB_URL_DFS1 = "jdbc:dolphindb://172.16.95.128:8922?databasePath=dfs://rangedb&partitionType=RANGE&partitionScheme= 0 5 10&locations= [`rh8503, `rh8502`rh8504]";

	public static void main(String[] args) throws Exception {

//		 CreateTable(System.getProperty("user.dir")+"/data/createTable_all.java",path,"t1","172.16.95.128", "8922");
//		CreateValueTable("172.16.95.128", "8922");
		
		Object[] o1 = new Object[] { true, 'a', 122, 21, 22, 2.1f, 2.1, "Hello",
				new BasicDate(LocalDate.parse("2013-06-13")), new BasicMonth(YearMonth.parse("2016-06")),
				new BasicTime(LocalTime.parse("13:30:10.008")), new BasicMinute(LocalTime.parse("13:30:10")),
				new BasicSecond(LocalTime.parse("13:30:10")), new BasicMinute(LocalTime.parse("13:30:10")),
				new BasicTimestamp(LocalDateTime.parse("2012-06-13T13:30:10.008")),
				new BasicNanoTime(LocalTime.parse("13:30:10.008007006")),
				new BasicDate(LocalDate.parse("2013-06-13")) };

		Object[] o2 = new Object[] { true, 'A', 123, 22, 23, 2.2f, 2.2, "world",
				new BasicDate(LocalDate.parse("2013-06-14")), new BasicMonth(YearMonth.parse("2016-07")),
				new BasicTime(LocalTime.parse("13:30:10.009")), new BasicMinute(LocalTime.parse("13:31:10")),
				new BasicSecond(LocalTime.parse("13:30:11")),
				new BasicDateTime(LocalDateTime.parse("2012-06-13T13:30:11")),
				new BasicTimestamp(LocalDateTime.parse("2012-06-13T13:30:10.009")),
				new BasicNanoTime(LocalTime.parse("13:30:10.008007007")),
				new BasicDate(LocalDate.parse("2013-06-14")) };

		Object[][] o3 = new Object[][] { o1, o2 };

		int len = o3[0].length;
		int n = o3.length;

		HashMap<Integer, Object> map = new HashMap<>(len + 1);
		Object[] o4 = new Object[len];
		Vector vector;

		for (int i = 0; i < len; ++i) {
			vector = new BasicAnyVector(n);
			switch (i) {
			case 0:
				for (int j = 0; j < n; ++j) {
					vector.set(j, new BasicBoolean((boolean) o3[j][i]));
				}
				break;
			case 1:
				for (int j = 0; j < n; ++j) {
					vector.set(j, new BasicByte((byte) ((char) o3[j][i] & 0xFF)));
				}
				break;
			case 2:
				for (int j = 0; j < n; ++j) {
					vector.set(j, new BasicShort(Short.parseShort(o3[j][i].toString())));
				}
				break;
			case 3:
				for (int j = 0; j < n; ++j) {
					vector.set(j, new BasicInt((int) o3[j][i]));
				}
				break;
			case 4:
				for (int j = 0; j < n; ++j) {
					vector.set(j, new BasicLong((int) o3[j][i]));
				}
				break;
			case 5:
				for (int j = 0; j < n; ++j) {
					vector.set(j, new BasicFloat((float) o3[j][i]));
				}
				break;
			case 6:
				for (int j = 0; j < n; ++j) {
					vector.set(j, new BasicDouble((double) o3[j][i]));
				}
				break;
			case 7:
				for (int j = 0; j < n; ++j) {
					vector.set(j, new BasicString((String) o3[j][i]));
				}
				break;
			case 8:
			case 9:
			case 10:
			case 11:
			case 12:
			case 13:
			case 14:
			case 15:
			case 16:
				for (int j = 0; j < n; ++j) {
					vector.set(j, (Scalar) o3[j][i]);
				}
				break;
			}
			map.put(i + 1, vector);
			o4[i] = vector;
		}

//		TestPreparedStatement("jdbc:dolphindb://172.16.95.128:8921","t1 = loadTable(\"C:/DolphinDB/Data/send/0717\",`t1)", "select * from t1","insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", o4);

//		 TestPreparedStatement(DB_URL_DFS,"t1 = loadTable(\"C:/DolphinDB/Data/send/0717\",`t1)","select * from t1","update t1 set bool = ? where char = ?",new Object[]{false, 'a'});

//		 TestPreparedStatement(DB_URL_DFS,"t1 =loadTable(\"C:/DolphinDB/Data/send/0717\",`t1)","select * from t1","delete from t1 where char = ?",new Object[]{'a'});

//		 TestPreparedStatement(DB_URL_DFS,null,"select top 2 * from pt","insert into pt values(?, ?)",new Object[]{new YearMonth[]{YearMonth.parse("2000-01"),YearMonth.parse("2000-01")},new double[]{0.4,0.5}});

//		 TestPreparedStatement(DB_URL_DFS,null,"select top 2 * from pt","update pt set x = ? where month = ?",new Object[]{0.5, YearMonth.parse("2000-01")});

//		 TestPreparedStatement(DB_URL_DFS,null,"select top 2 * from pt","delete from pt where x = ?",new Object[]{YearMonth.parse("2000-01")});


//		 TestPreparedStatementBatch(DB_URL,"t1 = loadTable(\"C:/DolphinDB/Data/test\",`t1)","select * from t1","insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",o3);
		 
//		 TestPreparedStatementBatch1(DB_URL,o3);
//		 TestPreparedStatementBatch2(DB_URL,o3);
//		 TestPreparedStatementBatch3(DB_URL_DFS,new Object[][]{new Object[]{YearMonth.parse("2000-01"), 0.5}});
//		 CreateTable(System.getProperty("user.dir").replaceAll("\\\\","/") +
//		 "/data/createTable_all.java",path_All,"t1");

//		 TestAutomaticSwitchingNode();

		// ResultSetTest
//		 TestResultSetInsert(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",o1,false);
//		 TestResultSetInsert(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",o1,true);
//		
//		 TestResultSetInsert(DB_URL,"t1 = loadTable(system_db,`t1)","select bool,char from ej(t1, t1, `bool)",new Object[]{true,'a'},true);
//		
//		 TestResultSetUpdate(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",o1,false);
//		 TestResultSetUpdate(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",o1,true);
//		
//		 TestResultSetDelete(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",2,false);
//		 TestResultSetDelete(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",2,true);
//
//		 TestResultSetInsert(DB_URL_DFS,"pt = loadTable(\"C:/DolphinDB/Data/seqdb\",`pt)","select top 10 * from pt",new Object[]{new BasicMonth(YearMonth.parse("2016-07")),0.007},true);
//
//		 TestResultSetUpdate(DB_URL_DFS,"pt = loadTable(\"C:/DolphinDB/Data/seqdb\",`pt)","select top 10 * from pt",new Object[]{new BasicMonth(YearMonth.parse("2016-07")),0.007},true);
//
//		 TestResultSetDelete(DB_URL_DFS,"pt = loadTable(\"C:/DolphinDB/Data/seqdb\",`pt)","select top 10 * from pt",1,true);
//
//		 TestDatabaseMetaData(DB_URL,"");

	}

	public static boolean CreateTable(String file, String savePath, String tableName, String host, String port) {
		DBConnection db = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb = new StringBuilder();
			sb.append("bool = [1b, 0b];\n");
			sb.append("char = [97c, 'A'];\n");
			sb.append("short = [122h, 123h];\n");
			sb.append("int = [21, 22];\n");
			sb.append("long = [22l, 23l];\n");
			sb.append("float  = [2.1f, 2.2f];\n");
			sb.append("double = [2.1, 2.2];\n");
			sb.append("string= [`Hello, `world];\n");
			sb.append("date = [2013.06.13, 2013.06.14];\n");
			sb.append("month = [2016.06M, 2016.07M];\n");
			sb.append("time = [13:30:10.008, 13:30:10.009];\n");
			sb.append("minute = [13:30m, 13:31m];\n");
			sb.append("second = [13:30:10, 13:30:11];\n");
			sb.append("datetime = [2012.06.13 13:30:10, 2012.06.13 13:30:10];\n");
			sb.append("timestamp = [2012.06.13 13:30:10.008, 2012.06.13 13:30:10.009];\n");
			sb.append("nanotime = [13:30:10.008007006, 13:30:10.008007007];\n");
			sb.append("nanotimestamp = [2012.06.13 13:30:10.008007006, 2012.06.13 13:30:10.008007007];\n");
			sb.append("t1= table(bool,char,short,int,long,float,double,string,date,month,time,minute,second,datetime,timestamp,nanotime,nanotimestamp);\n");
			sb.append(Driver.DB + " =( \"" + savePath + "\")\n ");
			sb.append("saveTable(").append(Driver.DB).append(", t1, `").append(tableName).append(");\n");
			db = new DBConnection();
			db.connect(host, Integer.parseInt(port));
			System.out.println(sb.toString());
			db.run(sb.toString());
			
			sb = new StringBuilder();
			db = new DBConnection();
			sb.append("existsTable( \"" + savePath + "\", \""+ tableName +"\")" );
			db.connect(host, Integer.parseInt(port));
			if( db.run(sb.toString()).getString().equals("true")) {
				return true;
			}
			return false;
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			if (db != null)
				db.close();
		}
//		return true;
	}
	
	public static boolean CreateValueTable(String host, String port) {
		DBConnection db = null;
		
			
			StringBuilder sb = new StringBuilder();
			sb.append("n=1000000\n");
			sb.append("month=take(2000.01M..2016.12M, n)\n");
			sb.append("x=rand(1.0, n)\n");
			sb.append("t=table(month, x)\n");
			sb.append("if(existsDatabase(\"dfs://valuedb\")){dropDatabase(\"dfs://valuedb\")}");
			sb.append("db=database(\"dfs://valuedb\", VALUE, 2000.01M..2016.12M)\n");
			sb.append("pt = db.createPartitionedTable(t, `pt, `month)\n");
			sb.append("pt.append!(t)\n");
			db = new DBConnection();
			StringBuilder sbe = new StringBuilder();
			try {
//				db = new DBConnection();
//				sbe.append("existsTable(\"dfs://valuedb\", \"pt\")" );
//				db.connect(host, Integer.parseInt(port));
//				if(db.run(sbe.toString()).getString().equals("true")) {
//					return true;
//				}
				
				db.connect(host, Integer.parseInt(port),"admin","123456");
				db.run(sb.toString());
				
				db = new DBConnection();
				db.connect(host, Integer.parseInt(port));
				if( db.run(sbe.toString()).getString().equals("true")) {
					return true;
				}
				return false;
				
			} catch (NumberFormatException | IOException e) {
				e.printStackTrace();
			}finally {
				if (db != null)
					db.close();
			}
			return false;
						
	}

	public static void TestStatementExecute(String url, String sql) throws Exception {
		System.out.println("TestStatementExecute begin");
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			ResultSet rs = null;
			int UpdateCount = -1;
			if (stmt.execute(sql)) {
				rs = stmt.getResultSet();
				printData(rs);
			} else {
				UpdateCount = stmt.getUpdateCount();
				if (UpdateCount != -1) {
					System.out.println(UpdateCount + " row affected");
				}
			}

			while (true) {
				if (stmt.getMoreResults()) {
					rs = stmt.getResultSet();
					printData(rs);
				} else {
					UpdateCount = stmt.getUpdateCount();
					if (UpdateCount != -1) {
						System.out.println(UpdateCount + "row affected");
					} else {
						break;
					}
				}
			}
			if (rs != null) {
				rs.close();
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("TestStatementExecute end");
	}
	
//	public static void mytest(){
//		Properties info = new Properties();
//		info.put("user", "admin");
//		info.put("password", "123456");
//		Connection conn = null;
//		PreparedStatement stmt = null;
//		try {
//			Class.forName(JDBC_DRIVER);
//			conn = DriverManager.getConnection("jdbc:dolphindb://localhost:8801", info);
//			
//			stmt = conn.prepareStatement("update t1 set v=-1 where id=1");
//			Object[] o1 = new Object[] {1,-1};
//			ResultSet rs = null;
//			stmt.execute("t1=table(1 2 3 as id, 4 5 6 as v)");
//			printData(rs);
//			if (rs != null)
//				rs.close();
//			int index = 1;
//			for (Object o : o1) {
//				stmt.setObject(index, o);
//				++index;
//			}
//			int UpdateCount = -1;
//			if (stmt.execute()) {
//				rs = stmt.getResultSet();
//				printData(rs);
//			} else {
//				UpdateCount = stmt.getUpdateCount();
//				if (UpdateCount != -1) {
//					System.out.println(UpdateCount + " row affected");
//				}
//			}
//			while (true) {
//				if (stmt.getMoreResults()) {
//					rs = stmt.getResultSet();
//					printData(rs);
//				} else {
//					UpdateCount = stmt.getUpdateCount();
//					if (UpdateCount != -1) {
//						System.out.println(UpdateCount + "row affected");
//					} else {
//						break;
//					}
//				}
//			}
//			if (rs != null) {
//				rs.close();
//			}
//			rs = stmt.executeQuery(select);
//			printData(rs);
//			if (rs != null)
//				rs.close();
//			stmt.close();
//			conn.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//			throw new Exception("fail");
//		} finally {
//			try {
//				if (stmt != null)
//					stmt.close();
//			} catch (SQLException se2) {
//			}
//			try {
//				if (conn != null)
//					conn.close();
//			} catch (SQLException se) {
//				se.printStackTrace();
//			}
//		}
//		System.out.println("TestPreparedStatement end");
//	}

	public static void TestPreparedStatement(String url, String loadTable, String select, String preSql,Object[] objects) throws Exception {
		System.out.println("TestPreparedStatement begin");
		
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "123456");
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url, info);
			stmt = conn.prepareStatement(preSql);
			if (loadTable != null && loadTable.length() > 0) {
				stmt.execute(loadTable);
			}
			ResultSet rs = null;
			rs = stmt.executeQuery(select);
			printData(rs);
			if (rs != null)
				rs.close();
			int index = 1;
			for (Object o : objects) {
				stmt.setObject(index, o);
				++index;
			}
			int UpdateCount = -1;
			if (stmt.execute()) {
				rs = stmt.getResultSet();
				printData(rs);
			} else {
				UpdateCount = stmt.getUpdateCount();
				if (UpdateCount != -1) {
					System.out.println(UpdateCount + " row affected");
				}
			}
			while (true) {
				if (stmt.getMoreResults()) {
					rs = stmt.getResultSet();
					printData(rs);
				} else {
					UpdateCount = stmt.getUpdateCount();
					if (UpdateCount != -1) {
						System.out.println(UpdateCount + "row affected");
					} else {
						break;
					}
				}
			}
			if (rs != null) {
				rs.close();
			}
			rs = stmt.executeQuery(select);
			printData(rs);
			if (rs != null)
				rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("fail");
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("TestPreparedStatement end");
	}

	public static void TestPreparedStatementBatch1(String url, Object[] objects) throws Exception {
		System.out.println("TestPreparedStatementBatch1 begin");

		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.prepareStatement("insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			stmt.execute("t1 = loadTable(system_db,`t1)");
			stmt.execute("t2 = table(1 as a, 1 as b)");
			for (int i = 0; i < objects.length; ++i) {
				int index = 1;
				Object[] o = (Object[]) objects[i];
				for (Object o1 : o) {
					stmt.setObject(index, o1);
					++index;
				}
				stmt.addBatch();
			}
			stmt.addBatch("insert into t2 values(1,2)");
			stmt.addBatch("update t2 set a = 2 where a = 1");
			stmt.addBatch("delete from t2 where b = 1");
			int[] arr_int = stmt.executeBatch();
			for (int i : arr_int) {
				System.out.print(i + " ");
			}
			System.out.println();
			ResultSet rs = stmt.executeQuery("select * from t1");
			printData(rs);
			rs.close();
			rs = stmt.executeQuery("select * from t2");
			printData(rs);
			rs.close();
			stmt.close();
			conn.close();
			// }catch (BatchUpdateException e){
			// System.out.println(e.getMessage());
			// int[] arr_int = e.getUpdateCounts();
			// for (int item : arr_int){
			// System.out.print(item+" ");
			// }
			// System.out.println();
		} catch (SQLException se) {
			se.printStackTrace();
			throw new Exception("SQL fail");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("fail");
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("TestPreparedStatementBatch1 end");
	}

	public static void TestPreparedStatementBatch2(String url, Object[] objects) throws Exception {
		System.out.println("TestPreparedStatementBatch2 begin");

		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.prepareStatement("insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			stmt.execute("t1 = loadTable(system_db,`t1)");
			stmt.execute("t2 = table(1 as a, 2 as b)");
			for (int i = 0; i < objects.length; ++i) {
				int index = 1;
				Object[] o = (Object[]) objects[i];
				for (Object o1 : o) {
					stmt.setObject(index, o1);
					++index;
				}
				stmt.addBatch();
			}
			stmt.addBatch("insert into t2 values(1,2)");
//			stmt.addBatch("select * from t2");
			int[] arr_int = stmt.executeBatch();
			for (int i : arr_int) {
				System.out.print(i + " ");
			}
			System.out.println();
			ResultSet rs = stmt.executeQuery("select * from t1");
			printData(rs);
			rs.close();
			rs = stmt.executeQuery("select * from t2");
			printData(rs);
			rs.close();
			stmt.close();
			conn.close();
		} catch (BatchUpdateException e) {
			int[] arr_int = e.getUpdateCounts();
			for (int item : arr_int) {
				System.out.print(item + " ");
			}
			System.out.println();
		} catch (SQLException se) {
			se.printStackTrace();
			throw new Exception("SQl fail");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("fail");
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("TestPreparedStatementBatch2 end");
	}

	public static void TestPreparedStatementBatch3(String url, Object[] objects) throws Exception {
		System.out.println("TestPreparedStatementBatch3 begin");
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "123456");
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url,info);
			stmt = conn.prepareStatement("insert into pt values(?, ?)");
			for (int i = 0; i < objects.length; ++i) {
				int index = 1;
				Object[] o = (Object[]) objects[i];
				for (Object o1 : o) {
					stmt.setObject(index, o1);
					++index;
				}
				stmt.addBatch();
			}
			stmt.addBatch("insert into pt values(2000.01M,0.4)");
			int[] arr_int = stmt.executeBatch();
			for (int i : arr_int) {
				System.out.print(i + " ");
			}
			System.out.println();
			ResultSet rs = stmt.executeQuery("select top 10 * from pt");
			printData(rs);
			rs.close();
			stmt.close();
			conn.close();
			// }catch (BatchUpdateException e){
			// System.out.println(e.getMessage());
			// int[] arr_int = e.getUpdateCounts();
			// for (int item : arr_int){
			// System.out.print(item+" ");
			// }
			// System.out.println();
		} catch (SQLException se) {
			se.printStackTrace();
			throw new Exception("SQL fail");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("fail");
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("TestPreparedStatementBatch3 end");
	}

	public static void TestPreparedStatementBatch(String url, String loadTable, String select, String batchSql,
			Object[] objects) throws Exception {
		System.out.println("TestPreparedStatementBatch begin");

		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.prepareStatement(batchSql);
			stmt.execute(loadTable);
			for (int i = 0; i < objects.length; ++i) {
				int index = 1;
				Object[] o = (Object[]) objects[i];
				for (Object o1 : o) {
					stmt.setObject(index, o1);
					++index;
				}
				stmt.addBatch();
			}
			stmt.executeBatch();
			ResultSet rs = stmt.executeQuery(select);
			printData(rs);
			rs.close();
			stmt.close();
			conn.close();
		} catch (SQLException se) {
			se.printStackTrace();
			throw new Exception("SQL fail");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("fail");
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("TestPreparedStatementBatch end");
	}

	public static void TestResultSetInsert(String url, String loadTable, String select, Object[] objects,
			boolean isInsert) throws Exception {
		System.out.println("TestResultSetInsert begin");

		Connection conn = null;
		Statement stmt = null;
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "123456");
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url,info);
			stmt = conn.createStatement();
			stmt.execute(loadTable);
			ResultSet rs = stmt.executeQuery(select);
			printData(rs);
			rs.absolute(2);
			rs.moveToInsertRow();
			int index = 1;
			for (Object o : objects) {
				rs.updateObject(index, o);
				++index;
			}
			if (isInsert) {
				rs.insertRow();
			}
			rs.beforeFirst();
			printData(rs);
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();		
			throw new Exception("SQL fail");
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("TestResultSetInsert end");
	}

	public static void TestResultSetUpdate(String url, String loadTable, String select, Object[] objects,
			boolean isUpdate) throws Exception {
		System.out.println("TestResultSetInsert begin");

		Connection conn = null;
		Statement stmt = null;
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "123456");
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url,info);
			stmt = conn.createStatement();
			stmt.execute(loadTable);
			ResultSet rs = stmt.executeQuery(select);
			printData(rs);
			rs.absolute(2);
			int index = 1;
			for (Object o : objects) {
				rs.updateObject(index, o);
				++index;
			}
			if (isUpdate) {
				rs.updateRow();
			}
			rs.beforeFirst();
			printData(rs);
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("fail");
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("TestResultSetInsert end");
	}

	public static void TestResultSetDelete(String url, String loadTable, String select, int row, boolean isDelete)
			throws Exception {
		System.out.println("TestResultSetDelete begin");

		Connection conn = null;
		Statement stmt = null;
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "123456");
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url,info);
			stmt = conn.createStatement();
			stmt.execute(loadTable);
			ResultSet rs = stmt.executeQuery(select);
			printData(rs);
			rs.absolute(row);
			if (isDelete) {
				rs.deleteRow();
			}
			rs.beforeFirst();
			printData(rs);
			stmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("fail");
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("TestResultSetDelete end");
	}

	public static void TestDatabaseMetaData(String url, String sql) throws Exception {
		System.out.println("TestStatementExecute begin");
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			ResultSet rs = null;
			DatabaseMetaData metaData = conn.getMetaData();
			rs = metaData.getCatalogs();
			printData(rs);
			String s = metaData.getCatalogSeparator();
			System.out.println(s);
			s = metaData.getCatalogTerm();
			System.out.println(s);
			s = metaData.getDatabaseProductName();
			System.out.println(s);
			s = metaData.getDatabaseProductVersion();
			System.out.println(s);
			s = metaData.getDriverName();
			System.out.println(s);
			System.out.println(metaData.getDriverVersion());
			System.out.println(metaData.getExtraNameCharacters());
			System.out.println(metaData.getIdentifierQuoteString());
			System.out.println(metaData.getJDBCMajorVersion());
			System.out.println(metaData.getJDBCMinorVersion());
			System.out.println(metaData.getMaxBinaryLiteralLength());
			System.out.println(metaData.getMaxCatalogNameLength());
			System.out.println(metaData.getMaxColumnNameLength());
			System.out.println(metaData.getNumericFunctions());
			System.out.println(metaData.getProcedureTerm());
			System.out.println(metaData.getResultSetHoldability());
			System.out.println(metaData.getSchemaTerm());
			System.out.println(metaData.getSearchStringEscape());
			System.out.println("getSQLKeywords() = " + metaData.getSQLKeywords());
			System.out.println("getSQLStateType() = " + metaData.getSQLStateType());
			System.out.println("getStringFunctions() =" + metaData.getStringFunctions());
			System.out.println("getSystemFunctions() = " + metaData.getSystemFunctions());
			printData(metaData.getTableTypes());
			System.out.println("getTimeDateFunctions() = " + metaData.getTimeDateFunctions());
			printData(metaData.getTypeInfo());
			System.out.println("getURL() = " + metaData.getURL());
			stmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("fail");
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("TestStatementExecute end");
	}

	public static void printData(ResultSet rs) throws SQLException {
		ResultSetMetaData resultSetMetaData = rs.getMetaData();
		int len = resultSetMetaData.getColumnCount();
		while (rs.next()) {
			for (int i = 1; i <= len; ++i) {
				System.out.print(
						MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getObject(i)));
			}
			System.out.print("\n");
		}
	}

	public static void TestTypeCast(int type) throws Exception {
		System.out.println("TestTypeCast begin");

		Object[] basicType = new Object[] { true, (byte) 97, 'A', (short) 1, 1, 1l, 1.0f, 1.0, new BasicBoolean(true),
				new BasicByte((byte) 97), new BasicShort((short) 1), new BasicInt(1), new BasicLong(1l),
				new BasicFloat(1.0f), new BasicDouble(1.0), "Hello", new BasicString("Hello") };

		Scalar[] basicTypeScalar = new Scalar[] { new BasicBoolean(true), new BasicByte((byte) 97),
				new BasicShort((short) 1), new BasicInt(1), new BasicLong(1l), new BasicFloat(1.0f),
				new BasicDouble(1.0), new BasicString("Hello") };

		Entity[] targetBasicTypeEntities = new Entity[] { new BasicBoolean(true), new BasicByte((byte) 97),
				new BasicShort((short) 1), new BasicInt(1), new BasicLong(1l), new BasicFloat(1.0f),
				new BasicDouble(1.0), new BasicString("Hello") };

		Object[] dateTime = new Object[] { Date.valueOf("2013-06-14"), Time.valueOf("13:30:10"),
				Timestamp.valueOf("2012-06-13 13:30:10.008007007"), YearMonth.parse("2016-07"),
				LocalTime.parse("13:30:10.009"), LocalDate.parse("2013-06-14"),
				LocalDateTime.parse("2012-06-13T13:30:10.008007007"), new BasicDate(LocalDate.parse("2013-06-14")),
				new BasicMonth(YearMonth.parse("2016-07")), new BasicTime(LocalTime.parse("13:30:10.009")),
				new BasicMinute(LocalTime.parse("13:31:10")), new BasicSecond(LocalTime.parse("13:30:11")),
				new BasicDateTime(LocalDateTime.parse("2012-06-13T13:30:11")),
				new BasicTimestamp(LocalDateTime.parse("2012-06-13T13:30:10.009")),
				new BasicNanoTime(LocalTime.parse("13:30:10.008007007")),
				new BasicNanoTimestamp(LocalDateTime.parse("2012-06-13T13:30:10.008007007")) };

		Scalar[] dateTimeScalar = new Scalar[] { new BasicDate(LocalDate.parse("2013-06-14")),
				new BasicMonth(YearMonth.parse("2016-07")), new BasicTime(LocalTime.parse("13:30:10.009")),
				new BasicMinute(LocalTime.parse("13:31:10")), new BasicSecond(LocalTime.parse("13:30:11")),
				new BasicDateTime(LocalDateTime.parse("2012-06-13T13:30:11")),
				new BasicTimestamp(LocalDateTime.parse("2012-06-13T13:30:10.009")),
				new BasicNanoTime(LocalTime.parse("13:30:10.008007007")),
				new BasicNanoTimestamp(LocalDateTime.parse("2012-06-13T13:30:10.008007007")) };

		Entity[] targetDateTimeEntities = new Entity[] { new BasicDate(LocalDate.parse("2013-06-14")),
				new BasicMonth(YearMonth.parse("2016-07")), new BasicTime(LocalTime.parse("13:30:10.009")),
				new BasicMinute(LocalTime.parse("13:31:10")), new BasicSecond(LocalTime.parse("13:30:11")),
				new BasicDateTime(LocalDateTime.parse("2012-06-13T13:30:11")),
				new BasicTimestamp(LocalDateTime.parse("2012-06-13T13:30:10.009")),
				new BasicNanoTime(LocalTime.parse("13:30:10.008007007")),
				new BasicNanoTimestamp(LocalDateTime.parse("2012-06-13T13:30:10.008007007")) };

		int size = 3;

		Object[] objects;
		Entity[] entities;
		switch (type) {
		case 0:
			objects = basicType;
			entities = targetBasicTypeEntities;
			break;
		case 1:
			Object[] basicTypeVector = new Object[basicTypeScalar.length];
			for (int i = 0, ilen = basicTypeScalar.length; i < ilen; ++i) {
				BasicAnyVector basicAnyVector = new BasicAnyVector(size);
				for (int j = 0; j < size; ++j) {
					basicAnyVector.set(j, basicTypeScalar[i]);
				}
				basicTypeVector[i] = basicAnyVector;
			}
			objects = basicTypeVector;
			entities = targetBasicTypeEntities;
			break;
		case 2:
			Object[] basicTypeArr = new Object[basicType.length];
			for (int i = 0, ilen = basicType.length; i < ilen; ++i) {
				Object[] objectArr = new Object[size];
				for (int j = 0; j < size; ++j) {
					System.out.println(basicType[i].getClass().getName());
					objectArr[j] = basicType[i];
				}
				basicTypeArr[i] = objectArr;
			}
			objects = basicTypeArr;
			entities = targetBasicTypeEntities;
			break;
		case 3:
			Object[] basicTypeList = new Object[basicType.length];
			for (int i = 0, ilen = basicType.length; i < ilen; ++i) {
				List<Object> objectList = new ArrayList<>();
				for (int j = 0; j < size; ++j) {
					objectList.add(basicType[i]);
				}
				basicTypeList[i] = objectList;
			}
			objects = basicTypeList;
			entities = targetBasicTypeEntities;
			break;
		case 4:
			objects = dateTime;
			entities = targetDateTimeEntities;
			break;
		case 5:
			Object[] dataTimeVector = new Object[dateTimeScalar.length];
			for (int i = 0, ilen = dateTimeScalar.length; i < ilen; ++i) {
				BasicAnyVector basicAnyVector = new BasicAnyVector(size);
				for (int j = 0; j < size; ++j) {
					basicAnyVector.set(j, dateTimeScalar[i]);
				}
				dataTimeVector[i] = basicAnyVector;
			}
			objects = dataTimeVector;
			entities = targetDateTimeEntities;
			break;
		case 6:
			Object[] dateTimeArr = new Object[dateTime.length];
			for (int i = 0, ilen = dateTime.length; i < ilen; ++i) {
				Object[] objectArr = new Object[size];
				for (int j = 0; j < size; ++j) {
					objectArr[j] = dateTime[i];
				}
				dateTimeArr[i] = objectArr;
			}
			objects = dateTimeArr;
			entities = targetDateTimeEntities;
			break;
		case 7:
			Object[] dateTimeList = new Object[dateTime.length];
			for (int i = 0, ilen = dateTime.length; i < ilen; ++i) {
				List<Object> objectList = new ArrayList<>();
				for (int j = 0; j < size; ++j) {
					objectList.add(dateTime[i]);
				}
				dateTimeList[i] = objectList;
			}
			objects = dateTimeList;
			entities = targetDateTimeEntities;
			break;
		default:
			objects = dateTime;
			entities = targetDateTimeEntities;
			break;
		}

		try {
			for (int i = 0, ilen = objects.length; i < ilen; ++i) {
				String srcValueClassName = objects[i].getClass().getName();
				for (int j = 0, jlen = entities.length; j < jlen; ++j) {
					String targetEntityClassName = entities[j].getClass().getName();
					System.out.println(srcValueClassName + "(" + objects[i] + ")" + " ==> " + targetEntityClassName);
					Entity entity = TypeCast.java2db(objects[i], targetEntityClassName);
					System.out.println(entity.getClass().getName() + " ==> " + entity.getString());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("TestTypeCast end");
	}

	public static void TestAutomaticSwitchingNode() throws Exception {
		System.out.println("TestAutomaticSwitchingNode begin");

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL_DFS);
			stmt = conn.prepareStatement("insert into pt values(?,?)");
			Thread.sleep(10000);
			List<Object> entities = Arrays.asList(
					new YearMonth[] { YearMonth.parse("2000-01"), YearMonth.parse("2000-01") },
					new double[] { 0.4, 0.5 });
			// Arrays.asList(YearMonth.parse("2018-06"),0.4);
			int index = 1;
			for (Object entity : entities) {
				stmt.setObject(index, entity);
				++index;
			}
			ResultSet rs = null;
			int UpdateCount = -1;
			long time = System.currentTimeMillis();
			if (stmt.execute()) {
				// rs = stmt.getResultSet();
				// printData(rs);
			} else {
				UpdateCount = stmt.getUpdateCount();
				if (UpdateCount != -1) {
					System.out.println(UpdateCount + " row affected");
				}
			}
			System.out.println(System.currentTimeMillis() - time + "ms");
			while (true) {
				if (stmt.getMoreResults()) {
					rs = stmt.getResultSet();
					// printData(rs);
				} else {
					UpdateCount = stmt.getUpdateCount();
					if (UpdateCount != -1) {
						System.out.println(UpdateCount + "row affected");
					} else {
						break;
					}
				}
			}

			rs = stmt.executeQuery("select * from pt");

			rs.last();

			System.out.println(rs.getRow());

			if (rs != null) {
				rs.close();
			}

			stmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		System.out.println("TestAutomaticSwitchingNode end");
	}
}
