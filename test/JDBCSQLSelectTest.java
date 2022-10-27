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

import com.dolphindb.jdbc.JDBCResultSet;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import com.xxdb.data.Vector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JDBCSQLSelectTest {
	static String HOST = JDBCTestUtil.HOST;
	static int PORT = JDBCTestUtil.PORT ;
	static String tableName = "trade";
	static String dataBase = "dfs://test_jdbc_sql";
	static ArrayList<String> colTypeString = null;
	Connection conn;
	Statement stm ;
	@Before
	public void SetUp(){
		conn = getConnection();
		try{
			stm = conn.createStatement();
			String sql = "login('admin','123456')\n" +
					"if(!existsDatabase('%s')){\n" +
					"t = table(1..100 as id, norm(1.0,0.1,100) as prc,take(`C`E,100) as ticker, take(2018.01.01..2018.10.18,100) as date, norm(15.0,0.1,100) as bid)\n" +
					"db = database('%s',RANGE,0..10*10)\n" +
					"t1 = db.createPartitionedTable(t,`%s,`id)\n" +
					"t1.append!(t)\n" +
					"}";
			sql = String.format(sql,dataBase,dataBase,tableName);
			stm.execute(sql);
		}catch (SQLException ex){
			ex.printStackTrace();
		}
	}
	@Test
	public void TestSimpleSelect(){
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("select PRC from trade");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
    @Test
	public void TestCaseMixingSelect(){
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("Select * fROM trade WHEre ID=1");
			Assert.assertTrue(rs.next());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void TestCaseMixingLast(){
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("Select lASt(ID) fROM trade");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void TestCaseMixingOr(){
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("selecT count(*) fROM trade WHEre ID=1 oR 2 ");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void TestCaseMixingOrder(){
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("SElect count(*) fROM trade oRdeR By id");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void TestCaseMixingGroup(){
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("SElect count(*) fROM trade GROUP By prc");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void TestCaseMixingCgroup(){
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("SElect avg(prc) fROM trade CGROUP By prc orDER BY prc;");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void TestCaseMixingInterval(){
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("SElect count(*) fROM trade group By INterval(date,1d,\"null\")");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void TestCaseMixingHaVing () {
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("select sum(PRC) as SUM from trade group by date HAving sum(PRC) > 0 ");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void TestCaseMixingLimit () {
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("select * from trade liMIT 3 ");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void TestCaseMixingTop () {
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("select TOP 3 * from trade");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void TestCaseMixingMap () {
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("select TOP 3 * from trade Map");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void TestCaseMixingPivot () {
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("select prc  from trade piVOT by date, ticker;");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void TestCaseMixingPartition () {
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("select * from trade where partition(ID, 2);");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void TestCaseMixingSample () {
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("select * from trade where sAMPLe(ID, 2);");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void TestVectorSelect(){
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("select prc from trade");
			Assert.assertTrue(rs.next());
			while(rs.next()) {
				System.out.println(rs.getString(1));
			}

		} catch (SQLException e) {
			e.printStackTrace();
			Assert.assertTrue("test return vector with exception ",true);
		}
	}

	@Test
	public void test_select1() throws SQLException {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("select 1 as val");
		Assert.assertTrue(rs.next());
		Assert.assertEquals("1",rs.getString(1));
		Statement s2 = conn.createStatement();
		ResultSet rs2 = s2.executeQuery("select 1");
		Assert.assertTrue(rs2.next());
		Assert.assertEquals("1",rs2.getString("val"));
		Assert.assertEquals("1",rs2.getString(1));
		Assert.assertEquals(rs2.getString(1),rs.getString(1));
	}

	@Test
	public void test_exec() throws SQLException {
		Statement stm = conn.createStatement();
		ResultSet rs = stm.executeQuery("exec top 100 * from loadTable(\"dfs://test_jdbc_sql\",\"trade\");");
		Assert.assertTrue(rs.next());
		ResultSet rs2 = stm.executeQuery("EXEC TOP 100 * FROM loadTable(\"dfs://test_jdbc_sql\",\"trade\");");
		Assert.assertTrue(rs2.next());
		ResultSet rs3 = stm.executeQuery("Exec count(*) as x fRom loadTable(\"dfs://test_jdbc_sql\",\"trade\")");
		ResultSet rs4 = stm.executeQuery("eXec top 5 * from loadTable(\"dfs://test_jdbc_sql\",\"trade\")");
		Assert.assertTrue(rs4.next());
	}

	@Test
	public void TestScalarSelect(){
		try {
			Statement s = conn.createStatement();
			ResultSet rs =s.executeQuery("1:2");
			Assert.assertTrue(rs.next());

		} catch (SQLException e) {
			Assert.assertTrue("test return pair with exception ",true);
		}
	}

	@Test
	public void test_Statement_oracle_function(){
		try{
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			JDBCResultSet rs = (JDBCResultSet) s.executeQuery("select DisTinct ticker from trade");
			Assert.assertEquals(2,rs.getResult().rows());
			JDBCResultSet jr = (JDBCResultSet) s.executeQuery("select * from trade where ticker=`C order by date AsC");
			BasicTable bt = (BasicTable) jr.getResult();
			Assert.assertEquals("2018.01.01",bt.getColumn("date").get(0).getString());
			JDBCResultSet js = (JDBCResultSet) s.executeQuery("select * from trade where ticker=`E order by id dESc");
			BasicTable jst = (BasicTable) js.getResult();
			Assert.assertEquals(98,((Scalar)jst.getColumn("id").get(0)).getNumber());
			JDBCResultSet je = (JDBCResultSet) s.executeQuery("select cOuNt(id),sUm(prc),mAX(bid),aVg(bid),mIn(prc) from trade");
			BasicTable jet = (BasicTable) je.getResult();
			Assert.assertEquals(99L,((Scalar)jet.getColumn(0).get(0)).getNumber());
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	@Test
	public void test_Statement_length_nvl_replace() throws IOException {
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
		try{
			Statement s = conn.createStatement();
			JDBCResultSet rs = (JDBCResultSet) s.executeQuery("select length(sym) from st");
			System.out.println(rs.getResult().getString());
			List<String> colNames = new ArrayList<>();
			List<Vector> cols = new ArrayList<>();
			colNames.add("strlen_sym");
			cols.add(new BasicIntVector(new int[]{1,2,2,2,3,3,1,1,1,4,2}));
			BasicTable bt = new BasicTable(colNames,cols);
			Assert.assertEquals(bt.getString(),rs.getResult().getString());
			JDBCResultSet jd = (JDBCResultSet) s.executeQuery("select sym,nvl(price,200),qty from st;");
			BasicTable jdt = (BasicTable) jd.getResult();
			Assert.assertEquals(200.0,((Scalar)jdt.getColumn(1).get(9)).getNumber());
			Assert.assertNotNull(jdt.getColumn(1).get(10));
			JDBCResultSet jb = (JDBCResultSet) s.executeQuery("select replace(sym,\"IBM\",\"FBI\") from st");
			BasicTable jbt = (BasicTable) jb.getResult();
			Assert.assertEquals("FBI",jbt.getColumn(0).get(4).getString());
			Assert.assertNotEquals("IBM",jbt.getColumn(0).get(5).getString());
			db.run("undef(`st,SHARED)");
			db.run("undef(`st2,SHARED)");
			db.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	@Test
	public void test_Statement_oracle_outer_join() throws IOException {
		DBConnection db = new DBConnection();
		db.connect(HOST,PORT,"admin","123456");
		String script = "t=table(`C`MS`MS`MS`IBM`IBM`C`C`C`APPL`XM as sym," +
				"49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29 NULL NULL as price," +
				"2200 1900 2100 3200 6800 5400 1300 2500 8800 1080 9000 as qty, " +
				"[09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12,09:40:35,09:42:27] as timestamp);" +
				"t2 = table(`IBM`IBM`XM`APPL`AMZON`MS`GOOG`ORCL as sym," +
				"'a' 'b' 'c' 'd' 'e' 'f' 'g' 'h' as char," +
				"true false false true true true false false as bool," +
				"11:30m 12:30m 13:30m 14:30m 15:30m 16:30m 17:30m 18:30m as minute);" +
				"if(existsDatabase(\"dfs://db_testStatement\")){dropDatabase(\"dfs://db_testStatement\")}" +
				"db=database(\"dfs://db_testStatement\",VALUE,`C`MS`IBM`APPL`XM`AMZON`GOOG`ORCL);" +
				"pt=db.createPartitionedTable(t,`pt,`sym).append!(t);" +
				"qt=db.createPartitionedTable(t2,`qt,`sym).append!(t2);";
		db.run(script);
		try{
			Statement s = conn.createStatement();
			s.execute("qt = loadTable(\"dfs://db_testStatement\",\"qt\")");
			s.execute("pt = loadTable(\"dfs://db_testStatement\",\"pt\")");
			JDBCResultSet rs = (JDBCResultSet) s.executeQuery("select sym,qty,price,char,bool,minute,timestamp from qt left outer join pt on qt.sym=pt.sym");
			BasicTable bt = (BasicTable) rs.getResult();
			Map<String,Entity> map = new HashMap<>();
			map.put("LJTable",bt);
			db.upload(map);
			Assert.assertEquals(0,db.run("select * from LJTable where sym=`C").rows());
			JDBCResultSet jd = (JDBCResultSet) s.executeQuery("select sym,qty,price,char,bool,minute,timestamp from qt outer join pt on qt.sym=pt.sym");
			Assert.assertEquals(16,jd.getResult().rows());
			JDBCResultSet jr = (JDBCResultSet) s.executeQuery("select sym,char,bool,minute,price,qty,timestamp from qt right outer join pt on qt.sym=pt.sym");
			BasicTable jrt = (BasicTable) jr.getResult();
			Map<String,Entity> map1 = new HashMap<>();
			map1.put("RJTable",jrt);
			db.tryUpload(map1);
			Assert.assertEquals(0,db.run("select * from RJTable where sym=`GOOG").rows());
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	@After
	public void Destroy(){
		try{
			String sql = "login('admin','123456');dropTable(database('%s'),'%s');dropDatabase('%s');";
			sql = String.format(sql,dataBase,tableName,dataBase);
			stm.execute(sql);
			stm.close();
			conn.close();
		}catch(SQLException ex){

		}finally {

		}

	}

	public static Connection getConnection(){
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "123456");
		Connection conn = null;
		try {
			Class.forName("com.dolphindb.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:dolphindb://" + HOST +":" + PORT, info);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return conn;
		
	}
	
	public static void SelectTest() {
		Connection conn = getConnection();
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("select PRC from trade");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void testSelectGroupBy() {
		Connection conn = getConnection();
		try {
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("select max(PRC), TICKER from trade group by date");
			printData(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void testSelectGroupByHaving () {
		Connection conn = getConnection();
		try {

			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("select sum(PRC) as SUM from trade group by date having sum(PRC) > 0 ");
			
			
			printData(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void loadTop5Datatest() {
		Connection conn = getConnection();
		try {

			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("select top 5 * from trade");
			
			
			printData(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void testSelectWhere() {
		Connection conn = getConnection();
		try {

			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
			ResultSet rs =s.executeQuery("select * from trade where BID = 16.5");
			
			
			printData(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void uploadDatatest() throws Exception {
		
		Connection conn = null;
		PreparedStatement ps = null;
		Statement stmt = null;
		
		String sql = "insert into trade values(?,?,?,?,?,?,?,?)";
		StringBuffer sb = null;
		try {
			
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			sb = new StringBuffer();
			sb.append("if(existsDatabase(\""+ dataBase +"\"))dropDatabase(\""+ dataBase +"\")\n");
			sb.append("db=database(\""+ dataBase +"\", RANGE, `A`F`K`O`S`ZZZ)\n");
			sb.append("t1=table(100:0, `PERMNO`date`TICKER`PRC`VOL`BID`ASK`SHROUT, [INT, DATE, SYMBOL, DOUBLE, INT, DOUBLE, DOUBLE,INT])\n");
			sb.append("db.createPartitionedTable(t1,`trade, `TICKER)\n");
			
			ps.execute("trade=loadTable(\""+ dataBase +"\", `"+ tableName +")");
			
			ResultSet rs = ps.executeQuery("select count(*) from trade");
			printData(rs);
			
			ps.execute(sb.toString());
			File f = new File("/Users/qiaojianhu/Desktop/DolphinDB/JDBC/DolphinDBJDBC/test/USPricesFewerCols.csv");
			String line = "";
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd");
	         
            System.out.println("Reading file using Buffered Reader");
            BufferedReader b = new BufferedReader(new FileReader(f));
            int batch = 0;
            int count = 0;
            
            boolean notStarted = true;
            while ((line = b.readLine()) != null) {
            		
            	if(notStarted){
            		notStarted = false;
            		continue;
            	}
                String [] cols = line.split(",",-1);
                
                if(cols.length == 8 ){                	
                ps.setInt(1, Integer.parseInt(cols[0]));              
                LocalDate localDate = LocalDate.parse(cols[1], formatter);
                ps.setObject(2, new BasicDate(localDate));
                
                if(cols[2].equals("")){
                	ps.setString(3, "NULL");
                }
                else{
                	ps.setString(3, cols[2]);
                }
                                   
                if(cols[3].equals("")){
                	ps.setNull(4, Types.DOUBLE);
                }
                else{
                	ps.setDouble(4, Double.parseDouble(cols[3]));
                }
                if(cols[4].equals("")){
                	ps.setNull(5, Types.INTEGER);
                }
                else{
                	ps.setInt(5, Integer.parseInt(cols[4]));
                }

                if(cols[5].equals("")){
                	ps.setNull(6, Types.DOUBLE);
                }
                else{
                	ps.setDouble(6, Double.parseDouble(cols[5]));
                }
                if(cols[6].equals("")){
                	ps.setNull(7, Types.DOUBLE);
                }
                else{
                	ps.setDouble(7, Double.parseDouble(cols[6]));
                }
                if(cols[7].equals("")){
                	ps.setNull(8, Types.INTEGER);
                }
                else{
                	ps.setInt(8, Integer.parseInt(cols[7]));
                }
                
                ps.addBatch();
                batch ++;
                count ++;
                if(batch % 10000 == 0){
                		ps.executeBatch();
                		ps.clearBatch();
                		batch = 0;
                		break;
                }               
                	
                }
            }
            
            if(batch>0){
            		ps.executeBatch();         		
            }

            ps.execute("trade=loadTable(\""+ dataBase +"\", `" + tableName + ")");
            rs = ps.executeQuery("select count(*) from trade");
			printData(rs);
			ps.close();
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
	
	public static void getColTypes() {
		BasicDictionary schema = null; 
		DBConnection db = new DBConnection();
		StringBuilder sb = new StringBuilder();
		sb.append("trade=loadTable(\""+ dataBase +"\", `"+ tableName +")\n");
		sb.append("schema(trade)\n");
		
		try {
			db.connect(HOST, Integer.parseInt("8921"),"admin","123456");
			schema = (BasicDictionary) db.run(sb.toString());
		
			BasicTable colDefs = (BasicTable) schema.get(new BasicString("colDefs"));
			BasicStringVector typeString = (BasicStringVector) colDefs.getColumn("typeString");
			int size = typeString.rows();
			colTypeString = new ArrayList<String>();
			for (int i = 0; i < size; i++) {
				colTypeString.add(typeString.getString(i).toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void printData(ResultSet rs) throws SQLException {
		
		getColTypes();		
		ResultSetMetaData resultSetMetaData = rs.getMetaData();
		int len = resultSetMetaData.getColumnCount();
		while (rs.next()) {			
			for (int i = 1; i <= len; ++i) {
				System.out.println(colTypeString.get(i-1));
				if(colTypeString.get(i-1).equals("DATE")) {
					System.out.print( resultSetMetaData.getColumnName(i)+ ": " +rs.getDate(i) + ",    ");
				}if(colTypeString.get(i-1).equals("SYMBOL") || colTypeString.get(i-1).equals("STRING")) {
					System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getString(i)));
				}if(colTypeString.get(i-1).equals("DOUBLE")) {
					System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getDouble(i)));
				}if(colTypeString.get(i-1).equals("INT")) {
					System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getDouble(i)));
				}if(colTypeString.get(i-1).equals("DATETIME")) {
					System.out.print( resultSetMetaData.getColumnName(i)+ ": " +rs.getTimestamp(i) + ",    ");
				}if(colTypeString.get(i-1).equals("TIMESTAMP")) {
					System.out.print( resultSetMetaData.getColumnName(i)+ ": " +rs.getTimestamp(i) + ",    ");
				}if(colTypeString.get(i-1).equals("TIME")) {
					System.out.print( resultSetMetaData.getColumnName(i)+ ": " +rs.getTime(i) + ",    ");
				}if(colTypeString.get(i-1).equals("LONG")) {
					System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getLong(i)));
				}if(colTypeString.get(i-1).equals("MONTH")) {
					System.out.print( resultSetMetaData.getColumnName(i)+ ": " +rs.getDate(i) + ",    ");
				}if(colTypeString.get(i-1).equals("BOOL")) {
					System.out.print( resultSetMetaData.getColumnName(i)+ ": " +rs.getBoolean(i) + ",    ");
				}if(colTypeString.get(i-1).equals("CHAR")) {
					System.out.print( resultSetMetaData.getColumnName(i)+ ": " +rs.getString(i) + ",    ");
				}if(colTypeString.get(i-1).equals("SHORT")) {
					System.out.print( resultSetMetaData.getColumnName(i)+ ": " +rs.getShort(i) + ",    ");
				}if(colTypeString.get(i-1).equals("MINUTE")) {
					System.out.print( resultSetMetaData.getColumnName(i)+ ": " +rs.getTime(i) + ",    ");
				}if(colTypeString.get(i-1).equals("SECOND")) {
					System.out.print( resultSetMetaData.getColumnName(i)+ ": " +rs.getTime(i) + ",    ");
				}if(colTypeString.get(i-1).equals("NANOTIME")) {
					System.out.print( resultSetMetaData.getColumnName(i)+ ": " +rs.getTime(i) + ",    ");
				}if(colTypeString.get(i-1).equals("NANOTIMESTAMP")) {
					System.out.print( resultSetMetaData.getColumnName(i)+ ": " +rs.getTimestamp(i) + ",    ");
				}if(colTypeString.get(i-1).equals("ANY")) {
					System.out.print( resultSetMetaData.getColumnName(i)+ ": " +rs.getObject(i) + ",    ");
				}
				
				
			}
			System.out.print("\n");
		}
	}
	
}
