import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dolphindb.jdbc.JDBCStatement;
import com.dolphindb.jdbc.Main;
import com.xxdb.DBConnection;
import com.xxdb.data.BasicAnyVector;
import com.xxdb.data.BasicBoolean;
import com.xxdb.data.BasicByte;
import com.xxdb.data.BasicDate;
import com.xxdb.data.BasicDateTime;
import com.xxdb.data.BasicDouble;
import com.xxdb.data.BasicFloat;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicLong;
import com.xxdb.data.BasicMinute;
import com.xxdb.data.BasicMonth;
import com.xxdb.data.BasicNanoTime;
import com.xxdb.data.BasicSecond;
import com.xxdb.data.BasicShort;
import com.xxdb.data.BasicString;
import com.xxdb.data.BasicTime;
import com.xxdb.data.BasicTimestamp;
import com.xxdb.data.Scalar;
import com.xxdb.data.Vector;

public class JDBCTest {
	private static final String path = "F:/test/jdbc";

	private static final String HOST = "127.0.0.1";
	private static final String PORT = "8848";
	private static final String USERNAME = "admin";
	private static final String PASSWD = "123456";
	private static final String SERVER = HOST + ":" + PORT;
	private static final String DB_URL = MessageFormat.format("jdbc:dolphindb://"+SERVER+"?databasePath={0}",path);
	private static final String DB_URL_WITHLOGIN = "jdbc:dolphindb://"+SERVER+"?user=admin&password=123456";
	private static final String DB_URL1 = "jdbc:dolphindb://";
	private static final String DB_URL_DFS = "jdbc:dolphindb://"+SERVER+"?databasePath=dfs://valuedb&partitionType=VALUE&partitionScheme=2000.01M..2016.12M";
	private static final String DB_URL_DFS1 = "jdbc:dolphindb://"+SERVER+"?databasePath=dfs://rangedb&partitionType=RANGE&partitionScheme= 0 5 10&locations= [`rh8503, `rh8502`rh8504]";
	
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

	@Before
	public void setUp() throws Exception{
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
		Main.CreateValueTable(HOST, PORT);
		DBConnection connection = new DBConnection();
		try {
			connection.connect(HOST, Integer.parseInt(PORT), USERNAME, PASSWD);
			connection.run("if (existsDatabase('dfs://testdb')) dropDatabase('dfs://testdb'); db = database('dfs://testdb', VALUE, 1..10); t0 = table(1..10 as id, NULL.join(2019.01.01T00:00:01 + 2..10) as time); t = db.createPartitionedTable(t0, `tb, `id); t.append!(t0)");
			connection.close();
		}
		catch (Exception e) {
			// TODO: handle exception
		}
	}


//	@Test
//	public void createTableTest(){  
//		Assert.assertTrue(Main.CreateTable(System.getProperty("user.dir")+"/data/createTable_all.java", path, "t1",host, port));
//	}
	
	@Test
	public void getDataTest() {
		JDBCStatement stmt = null;
		Connection conn = null;
	    try {
	    	Class.forName("com.dolphindb.jdbc.Driver");
	        conn = DriverManager.getConnection(DB_URL_WITHLOGIN);

	        stmt = (JDBCStatement) conn.createStatement();
	        stmt.execute("t = loadTable('dfs://testdb', `tb)");
	        //load table
	        ResultSet rs = stmt.executeQuery("select * from t");
	        rs.next();
	        Timestamp ts = rs.getTimestamp(2);
	        Assert.assertTrue(rs.wasNull());
	        rs.getInt(1);
	        Assert.assertFalse(rs.wasNull());
	        rs.next();
	        ts = rs.getTimestamp(2);
	        Assert.assertEquals(ts, Timestamp.valueOf("2019-01-01 00:00:03"));
	        Time t = rs.getTime(2);
	        Assert.assertEquals(t, Time.valueOf("00:00:03"));
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	    finally {
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
	}
	
	@Test
	public void scriptTest() {
		JDBCStatement stmt = null;
		Connection conn = null;
	    try {
	    	Class.forName("com.dolphindb.jdbc.Driver");
	        conn = DriverManager.getConnection(DB_URL_WITHLOGIN);

	        stmt = (JDBCStatement) conn.createStatement();
	        stmt.execute("a = table(1..3 as id, `a`b`c as symbol)");
	        ResultSet rs = stmt.executeQuery("b = table(1..2 as id); select * from ej(a, b, `id)");
	        rs.next();
	        Assert.assertEquals(rs.getInt(1), 1);
	        Assert.assertEquals(rs.getString(2), "a");
	        rs.next();
	        Assert.assertEquals(rs.getInt(1), 2);
	        Assert.assertEquals(rs.getString(2), "b");
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	    finally {
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
	}
	
	@Test
	public void loadTableTest(){  
		try {
			System.out.println(DB_URL);
			Main.TestPreparedStatement(DB_URL, "t1 = loadTable(\"" +path+"\",`t1,,true)", "select * from t1","insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", o4);
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void updateTableTest() {  
		try {
			Main.TestPreparedStatement(DB_URL, "t1 = loadTable(\"" +path+"\",`t1)","select * from t1","update t1 set bool = ? where char = ?", new Object[]{false, 'a'});
			Assert.fail();
		} catch (Exception e) {
		}
		
	}
	
	@Test
	public void deleteTableTest(){  
		try {
			Main.TestPreparedStatement(DB_URL, "t1 = loadTable(\"" +path+"\",`t1)","select * from t1","delete from t1 where char = ?", new Object[]{'a'});
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void DFSLoadTableInsertTest(){  
		try {
			Main.TestPreparedStatement(DB_URL_DFS, null, "select top 2 * from pt","insert into pt values(?, ?)", new Object[]{new YearMonth[]{YearMonth.parse("2000-01"),YearMonth.parse("2000-01")},new double[]{0.4,0.5}});
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void DFSLoadTableUpdateTest(){  
		try {
			Main.TestPreparedStatement(DB_URL_DFS,null,"select top 2 * from pt","update pt set x = ? where month = ?",new Object[]{0.5, YearMonth.parse("2000-01")});
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void DFSLoadTableTest(){  
		try {
			Main.TestPreparedStatement(DB_URL_DFS,"","select top 2 * from pt","delete from pt where x = ?",new Object[]{YearMonth.parse("2000-01")});
			Assert.fail();
		} catch (Exception e) {
		}
	}

	@Test
	public void batchTest1(){  
		try {
			Main.TestPreparedStatementBatch(DB_URL,"t1 = loadTable(\"" +path+"\",`t1)","select * from t1","insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",o3);
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void batchTest2(){  
		try {
			Main.TestPreparedStatementBatch1(DB_URL,o3);
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void batchTest3(){  
		try {
			Main.TestPreparedStatementBatch2(DB_URL,o3);
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void batchTest4(){  
		try {
			Main.TestPreparedStatementBatch3(DB_URL_DFS,new Object[][]{new Object[]{YearMonth.parse("2000-01"), 0.5}});
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void resultSetInsertTest(){  
		try {
			Main.TestResultSetInsert(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",o1,false);
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void resultSetUpdateTest(){  
		try {
			Main.TestResultSetUpdate(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",o1,false);
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void resultSetDeleteTest(){  
		try {
			Main.TestResultSetDelete(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",2,true);
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void TestDatabaseMetaData(){  
		try {
			Main.TestDatabaseMetaData(DB_URL,"");
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void SeqdbResultSetInsertTest(){  
		try {
			Main.TestResultSetInsert(DB_URL_DFS,"t1 = loadTable(\"" +path+"\",`t1,,true)","select top 10 * from pt",new Object[]{new BasicMonth(YearMonth.parse("2016-07")),0.007},true);
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void SeqdbResultSetUpdateTest(){  
		try {
			Main.TestResultSetUpdate(DB_URL_DFS,"t1 = loadTable(\"" +path+"\",`t1,,true)","select top 10 * from pt",new Object[]{new BasicMonth(YearMonth.parse("2016-07")),0.007},true);
			Assert.fail();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void SeqdbResultSetDeleteTest(){  
		try {
			Main.TestResultSetDelete(DB_URL_DFS,"t1 = loadTable(\"" +path+"\",`t1,,true)","select top 10 * from pt",1,true);
			Assert.fail();
		} catch (Exception e) {
		}
	}
}