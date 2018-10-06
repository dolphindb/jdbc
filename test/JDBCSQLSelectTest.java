import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import com.xxdb.data.BasicDate;

public class JDBCSQLSelectTest {

	static String HOST = "172.16.95.128" ;
	static int PORT = 8921 ;
	public static void main(String[] args){
		System.out.println("JDBCSQLTest");
//		SelectTest();
//		testSelectGroupByHaving();
//		testSelectGroupBy();
//		loadTop5Datatest()
//		testSelectWhere();
//		try {
//			uploadDatatest();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
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
			s.execute("trade=loadTable(\"dfs://USPrices\", `trade)");
			ResultSet rs =s.executeQuery("select PRC from trade");
			
			
			printData(rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void testSelectGroupBy() {
		Connection conn = getConnection();
		try {

			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\"dfs://USPrices\", `trade)");
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
			s.execute("trade=loadTable(\"dfs://USPrices\", `trade)");
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
			s.execute("trade=loadTable(\"dfs://USPrices\", `trade)");
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
			s.execute("trade=loadTable(\"dfs://USPrices\", `trade)");
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
			sb.append("if(existsDatabase(\"dfs://USPrices\"))dropDatabase(\"dfs://USPrices\")\n");
			sb.append("db=database(\"dfs://USPrices\", RANGE, `A`F`K`O`S`ZZZ)\n");
			sb.append("t1=table(100:0, `PERMNO`date`TICKER`PRC`VOL`BID`ASK`SHROUT, [INT, DATE, SYMBOL, DOUBLE, INT, DOUBLE, DOUBLE,INT])\n");
			sb.append("db.createPartitionedTable(t1,`trade, `TICKER)\n");
			
			ps.execute("trade=loadTable(\"dfs://USPrices\", `trade)");
			
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

            ps.execute("trade=loadTable(\"dfs://USPrices\", `trade)");
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
	
}
