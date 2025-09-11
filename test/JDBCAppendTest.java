import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Properties;

import com.xxdb.DBConnection;
import com.xxdb.data.*;

public class JDBCAppendTest {

	static String HOST = JDBCTestUtil.HOST;
	static int PORT = JDBCTestUtil.PORT ;
	static String tableName = "trade";
	static String dataBase = "dfs://USPrices";
	static ArrayList<String> colTypeString = null;
	
	public static void main(String[] args){
		System.out.println("JDBCUpdateTest");
		try {
			upload10MillionDatatest();
			Append10MillionDatatest();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Connection getConnection(){
		Properties info = JDBCTestUtil.LOGININFO;
		info.put("user", "admin");
		info.put("password", "123456");
		Connection conn = null;
		try {
			Class.forName(JDBCTestUtil.JDBC_DRIVER);
			conn = DriverManager.getConnection("jdbc:dolphindb://" + HOST +":" + PORT, info);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return conn;
		
	}
	
	public static void upload10MillionDatatest() throws Exception {
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
			sb.append("t1=table(100:0, `PERMNO`date`TICKER`PRC`VOL`BID`ASK`SHROUT`TS`NTS, [INT, DATE, SYMBOL, DOUBLE, INT, DOUBLE, DOUBLE,INT,TIMESTAMP,NANOTIMESTAMP])\n");
			sb.append("db.createPartitionedTable(t1,`trade, `TICKER)\n");
			ps.execute(sb.toString());
			ps.execute("trade=loadTable(\""+ dataBase +"\", `"+ tableName +")");

			ResultSet rs = ps.executeQuery("select count(*) from trade");
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
	
public static void Append10MillionDatatest() throws Exception {
		Connection conn = null;
		PreparedStatement ps = null;
		Statement stmt = null;
		
		String sql = "insert into trade values(?,?,?,?,?,?,?,?,?,?)";
		try {
			
			conn = getConnection();
			ps = conn.prepareStatement(sql);						
			ps.execute("trade=loadTable(\"" + dataBase + "\", `" + tableName+")");			
			ResultSet rs = ps.executeQuery("select count(*) from trade");
			printData(rs);
			

//			File f = new File("/Users/qiaojianhu/Desktop/DolphinDB/JDBC/DolphinDBJDBC/test/USPricesFewerCols.csv");
//			String line = "";
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd");
	         
//            System.out.println("Reading file using Buffered Reader");
//            BufferedReader b = new BufferedReader(new FileReader(f));
            int batch = 0;
            int count = 0;
            
            boolean notStarted = true;
            while (true) {
            		
            	if(notStarted){
            		notStarted = false;
            		continue;
            	}
//                String [] cols = line.split(",",-1);
                
//                if(cols.length == 8 ){
					ps.setInt(1, count%10000);
					LocalDate localDate = LocalDate.parse("2007.03.04", formatter);
					ps.setObject(2, new BasicDate(localDate));

					if(count%2==1){
						ps.setString(3, "NULL");
					}
					else{
						ps.setString(3, "ABC");
					}

					if(count%2==1){
						ps.setNull(4, Types.DOUBLE);
					}
					else{
						ps.setDouble(4, Double.parseDouble("2.1325"));
					}
					if(count%2==1){
						ps.setNull(5, Types.INTEGER);
					}
					else{
						ps.setInt(5, Integer.parseInt("21"));
					}

					if(count%2==1){
						ps.setNull(6, Types.DOUBLE);
					}
					else{
						ps.setDouble(6, Double.parseDouble("2.56"));
					}
					if(count%2==1){
						ps.setNull(7, Types.DOUBLE);
					}
					else{
						ps.setDouble(7, Double.parseDouble("3.131415926"));
					}
					if(count%2==1){
						ps.setNull(8, Types.INTEGER);
					}
					else{
						ps.setInt(8, Integer.parseInt("55"));
					}
				if(count%2==1){
					ps.setNull(9, Types.TIMESTAMP);
				}
				else{
					ps.setObject(9, LocalDateTime.of(2018,10,12,14,12,01,001));
				}
				if(count%2==1){
					ps.setNull(10, Types.OTHER);
				}
				else{
					ps.setObject(10,   LocalDateTime.of(2018,10,12,14,12,01,123456));
				}
					ps.addBatch();
					batch ++;
					count ++;
					if(batch % 1000 == 0){
							ps.executeBatch();
							ps.clearBatch();
							batch = 0;
							if(count == 100000) {
								break;
							}
					}
                	
                }
            if(batch>0){
            		ps.executeBatch();         		
            }

            ps.execute("trade=loadTable(\"" + dataBase + "\", `"+tableName+")");
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
		db.connect(HOST, PORT,"admin","123456");
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
			if(colTypeString.get(i-1).equals("DATE")) {
				System.out.print( resultSetMetaData.getColumnName(i)+ ": " +rs.getDate(i) + ",    ");
			}if(colTypeString.get(i-1).equals("SYMBOL") || colTypeString.get(i-1).equals("STRING")) {
				System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getString(i)));
			}if(colTypeString.get(i-1).equals("DOUBLE")) {
				System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getDouble(i)));
			}if(colTypeString.get(i-1).equals("INT")) {
				try {
					System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getInt(i)));
				}catch(ClassCastException e){
					System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getLong(i)));
				}
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
