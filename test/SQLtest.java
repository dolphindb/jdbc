import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import com.mysql.cj.jdbc.result.ResultSetMetaData;

public class SQLtest extends Thread{
	
	public SQLtest() throws Exception {
	
		String driver = "com.mysql.jdbc.Driver";
	    String url = "jdbc:mysql://localhost:3306/DolphinDB?serverTimezone=GMT";
	    String username = "root";
	    String password = "19961210";
	    Connection conn = null;
	    PreparedStatement ps = null;
	    String sql = "insert into trade values(?,?,?,?,?,?,?,?)";
	    Date date = new Date();
	    
	
	    
		try {
			
			Class.forName(driver); 
	        conn = (Connection) DriverManager.getConnection(url, username, password);
			ps = conn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery("select count(*) from trade");
			printData(rs);
			
			
			File f = new File("/Users/qiaojianhu/Desktop/DolphinDB/JDBC/DolphinDBJDBC/test/USPricesFewerCols.csv");
			String line = "";
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd");
			DateFormat format = new SimpleDateFormat("yyyy.MM.dd");
	 
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
                date = format.parse(cols[1]);
                ps.setObject(2, new Timestamp(date.getTime()));
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
			
            rs = ps.executeQuery("select count(*) from trade");
			printData(rs);
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}finally {
			
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
		ResultSetMetaData resultSetMetaData = (ResultSetMetaData) rs.getMetaData();
		int len = resultSetMetaData.getColumnCount();
		while (rs.next()) {
			for (int i = 1; i <= len; ++i) {
				System.out.print(
						MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getObject(i)));
			}
			System.out.print("\n");
		}
	}
	
	public static void main(String[] args) throws NumberFormatException, IOException, ParseException{

		long startTime = System.currentTimeMillis(); 
		
		try {
			SQLtest test = new SQLtest();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		long endTime = System.currentTimeMillis(); 
    		System.out.println("Running time：" + (endTime - startTime) + "ms");
	    
	}

}

