import static org.junit.Assert.*;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.dolphindb.jdbc.Main;
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

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JDBCTest {
	
	
	private static final String path = "C:/DolphinDB/Data/test";
	
	private static final String host = "localhost";
	private static final String port = "8801";
	private static final String SERVER = "localhost:8801";
	private static final String DB_URL = MessageFormat.format("jdbc:dolphindb://"+SERVER+"?databasePath={0}",path);

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
		Main.CreateValueTable(host, port);
	}
	
	
	@Test
	public void createTableTest(){  
		Assert.assertTrue(Main.CreateTable(System.getProperty("user.dir")+"/data/createTable_all.java", path, "t1",host, port));
	}
	
	@Test
	public void loadTableTest(){  
		
			try {
				System.out.println(DB_URL);
				Main.TestPreparedStatement(DB_URL,"t1 = loadTable(\"" +path+"\",`t1,,true)", "select * from t1","insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", o4);
			} catch (Exception e) {
				Assert.fail();
				e.printStackTrace();
			}
		
	}
	
	@Test
	public void updateTableTest() {  
		try {
			Main.TestPreparedStatement(DB_URL,"t1 = loadTable(\"" +path+"\",`t1)","select * from t1","update t1 set bool = ? where char = ?",new Object[]{false, 'a'});
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void deleteTableTest(){  
		try {
			Main.TestPreparedStatement(DB_URL,"t1 = loadTable(\"" +path+"\",`t1)","select * from t1","delete from t1 where char = ?",new Object[]{'a'});
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void DFSLoadTableInsertTest(){  
		try {
			Main.TestPreparedStatement(DB_URL_DFS,null,"select top 2 * from pt","insert into pt values(?, ?)",new Object[]{new YearMonth[]{YearMonth.parse("2000-01"),YearMonth.parse("2000-01")},new double[]{0.4,0.5}});
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void DFSLoadTableUpdateTest(){  
		try {
			Main.TestPreparedStatement(DB_URL_DFS,null,"select top 2 * from pt","update pt set x = ? where month = ?",new Object[]{0.5, YearMonth.parse("2000-01")});
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void DFSLoadTableTest(){  
		try {
			Main.TestPreparedStatement(DB_URL_DFS,"","select top 2 * from pt","delete from pt where x = ?",new Object[]{YearMonth.parse("2000-01")});
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void batchTest1(){  
		try {
			Main.TestPreparedStatementBatch(DB_URL,"t1 = loadTable(\"" +path+"\",`t1)","select * from t1","insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",o3);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void batchTest2(){  
		try {
			Main.TestPreparedStatementBatch1(DB_URL,o3);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void batchTest3(){  
		try {
			Main.TestPreparedStatementBatch2(DB_URL,o3);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void batchTest4(){  
		try {
			Main.TestPreparedStatementBatch3(DB_URL_DFS,new Object[][]{new Object[]{YearMonth.parse("2000-01"), 0.5}});
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void resultSetInsertTest(){  
		try {
			Main.TestResultSetInsert(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",o1,false);
			fail("No exception thrown.");
		} catch (Exception e) {
			assertTrue(true);
		}
	}
	
	@Test
	public void resultSetUpdateTest(){  
		try {
			Main.TestResultSetUpdate(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",o1,false);
			fail("No exception thrown.");
		} catch (Exception e) {
			assertTrue(true);
		}
	}
	
	@Test
	public void resultSetDeleteTest(){  
		try {
			Main.TestResultSetDelete(DB_URL,"t1 = loadTable(system_db,`t1)","select * from t1",2,true);
			fail("No exception thrown.");
		} catch (Exception e) {
			assertTrue(true);
		}
	}
	
	@Test
	public void TestDatabaseMetaData(){  
		try {
			Main.TestDatabaseMetaData(DB_URL,"");
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void SeqdbResultSetInsertTest(){  
		try {
			Main.TestResultSetInsert(DB_URL_DFS,"t1 = loadTable(\"" +path+"\",`t1,,true)","select top 10 * from pt",new Object[]{new BasicMonth(YearMonth.parse("2016-07")),0.007},true);
			fail("No exception thrown.");
		} catch (Exception e) {
			assertTrue(true);
		}
	}
	
	@Test
	public void SeqdbResultSetUpdateTest(){  
		try {
			Main.TestResultSetUpdate(DB_URL_DFS,"t1 = loadTable(\"" +path+"\",`t1,,true)","select top 10 * from pt",new Object[]{new BasicMonth(YearMonth.parse("2016-07")),0.007},true);
			fail("No exception thrown.");
		} catch (Exception e) {
			assertTrue(true);
		}
	}
	
	@Test
	public void SeqdbResultSetDeleteTest(){  
		try {
			Main.TestResultSetDelete(DB_URL_DFS,"t1 = loadTable(\"" +path+"\",`t1,,true)","select top 10 * from pt",1,true);
			fail("No exception thrown.");
		} catch (Exception e) {
			assertTrue(true);
		}
	}
	
	
}


