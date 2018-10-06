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

import com.dolphindb.jdbc.Driver;
import com.xxdb.DBConnection;
import com.xxdb.data.BasicDate;

public class JDBCUpdateAndDeleteTest {
	
	static Connection conn = null;
	static String HOST = "172.16.95.128" ;
	static int PORT = 8921 ;
	
	public static void main(String[] args) {
		conn = getConnection();
		try {
			CreateTable("C:/DolphinDB/Data/UpdateTest","t1", HOST, PORT);
			System.out.println();
//			DeleteTest();
			UpdateTest();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void UpdateTest() {
		try {
		
			PreparedStatement s = conn.prepareStatement("update t1 set bool = ? where char = ?"); 
			s.execute("t1=loadTable(\"C:/DolphinDB/Data/UpdateTest\", `t1)");
			Object[] objects = new Object[]{false, 'a'};
			int index = 1;
			ResultSet rs = null;
			for (Object o : objects) {
				s.setObject(index, o);
				++index;
			}
			int UpdateCount = -1;
			if (s.execute()) {
				rs = s.getResultSet();
				printData(rs);
			} else {
				ResultSet r = s.executeQuery("select * from t1");
				printData(r);
			}
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void DeleteTest() {
		try {
		
			PreparedStatement s = conn.prepareStatement("delete from t1 where char = ?"); 
			s.execute("t1=loadTable(\"C:/DolphinDB/Data/UpdateTest\", `t1)");
			Object[] objects = new Object[]{'a'};
			int index = 1;
			ResultSet rs = null;
			for (Object o : objects) {
				s.setObject(index, o);
				++index;
			}
			int UpdateCount = -1;
			if (s.execute()) {
				rs = s.getResultSet();
				printData(rs);
			} else {
				ResultSet r = s.executeQuery("select * from t1");
				printData(r);
			}
			

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static Connection getConnection(){
		Properties info = new Properties();
		info.put("user", "admin");
		info.put("password", "123456");
		Connection conn = null;
		try {
			Class.forName("com.dolphindb.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:dolphindb://172.16.95.128:8921", info);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return conn;	
	}
	
	
	
	public static boolean CreateTable(String savePath, String tableName, String host, int port) {
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
			db.connect(host, port);
//			System.out.println(sb.toString());
			db.run(sb.toString());
			
			Statement s = conn.createStatement();
			s.execute("db =( \"C:/DolphinDB/Data/UpdateTest\")");
			s.execute("t1 = loadTable(db,'t1')");
			ResultSet rs =s.executeQuery("select * from t1");			
			printData(rs);
			
			sb = new StringBuilder();
			db = new DBConnection();
			sb.append("existsTable( \"" + savePath + "\", \""+ tableName +"\")" );
			db.connect(host, (port));
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
