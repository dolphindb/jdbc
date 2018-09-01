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
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.xxdb.data.BasicDate;

public class Test extends Thread{

	public Test() throws Exception {
			
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "123456");
		Connection conn = null;
		PreparedStatement ps = null;
		Statement stmt = null;
		
//		String sql = "insert into trade(PERMNO, date, TICKER, PRC, VOL, BID, ASK, SHROUT) values(?,?,?,?,?,?,?,?)";
		String sql = "insert into trade values(?,?,?,?,?,?,?,?)";
		StringBuffer sb = null;
		try {
			Class.forName("com.dolphindb.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:dolphindb://localhost:8801", info);
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
			File f = new File("C:/DolphinDB/db_testing/data/USPricesFewerCols.csv");
			String line = "";
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd");
	        
			//trade.append!(tmp);USPricesFewerCols
	 
            System.out.println("Reading file using Buffered Reader");
            BufferedReader b = new BufferedReader(new FileReader(f));
            int batch = 0;
                      
            boolean notStarted = true;
            while ((line = b.readLine()) != null) {
            	if(notStarted){
            		notStarted = false;
            		continue;
            	}
                String [] cols = line.split(",",-1);

                if(cols.length == 8){
                ps.setInt(1, Integer.parseInt(cols[0]));
                LocalDate localDate = LocalDate.parse(cols[1], formatter);
                ps.setObject(2, new BasicDate(localDate));
                ps.setString(3, cols[2]);
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
                if(batch % 100000 == 0){
                	ps.executeBatch();
                	ps.clearBatch();
                	batch = 0;
                }
                }
            }
            
            if(batch>0){
            	ps.executeBatch();
            }
//            ps.execute("login(\"admin\", \"123456\")" );
            ps.execute("trade=loadTable(\"dfs://USPrices\", `trade)");
            rs = ps.executeQuery("select count(*) from trade");
			//ResultSet rs = pstmt.executeQuery("select * from t1");
			printData(rs);
			//TimeUnit.SECONDS.sleep(30);
			//rs = pstmt.executeQuery(sql);
			///printData(rs);
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
	
	public static void main(String[] args){
		try {
			Test test = new Test();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

