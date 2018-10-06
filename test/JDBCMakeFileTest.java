import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicDictionary;
import com.xxdb.data.BasicString;
import com.xxdb.data.BasicStringVector;
import com.xxdb.data.BasicTable;

public class JDBCMakeFileTest {
	
	static String HOST = "172.16.95.128" ;
	static int PORT = 8921 ;
	static String tableName = "t1";
	static String dataBase = "C:/DolphinDB/TimeTest";
	static ArrayList<String> colTypeString = null;
	public static void main(String[] args){
		System.out.println("JDBCLoadTest");
		makeFiletest();
	}
	
	public static void makeFiletest() {
		HashMap<String, ArrayList> dataTable = new HashMap<String, ArrayList>();
		Connection conn = getConnection();
		try {
			
//			makeTimeTestTable();
			
			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\""+ dataBase +"\", `"+ tableName +")");
			ResultSet rs =s.executeQuery("select * from trade");
			
			printData(rs);
//			makeFile(rs);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
				
	}
	
	public static void makeTimeTestTable() {
		DBConnection db = null;
			
		StringBuilder sb = new StringBuilder();
		sb = new StringBuilder();
		sb.append("db=database(\""+ dataBase +"\");\n");
		sb.append("t = table((2013.06.13 2013.06.14 2013.06.15) as T, (2012.06.13 13:30:10 2012.06.14 13:30:10 2012.06.15 13:30:10 )as DT, (2012.06.13 13:30:10.008 2012.06.14 13:30:10.008 2012.06.15 13:30:10.008 )as TS );\n");
		sb.append("saveTable(db, t, `"+ tableName +");");
		db = new DBConnection();
		try {
			db.connect(HOST, PORT);
			db.run(sb.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void makeFile(ResultSet rs) throws SQLException {
		
		getColTypes();
		ArrayList<ArrayList<String>> alldata=new ArrayList<ArrayList<String>>();		 
		ResultSetMetaData resultSetMetaData = rs.getMetaData();
		int len = resultSetMetaData.getColumnCount();
		
		ArrayList<String> colsName = new ArrayList<String>();
		for (int i = 1; i <= len; ++i) {
			colsName.add(resultSetMetaData.getColumnName(i).toString());
		}
		alldata.add(colsName);
		
		while (rs.next()) {
			ArrayList<String> tmp = new ArrayList<String>();
			for (int i = 1; i <= len; ++i) {
				if(colTypeString.get(i-1).equals("DATE")) {
					tmp.add(String.valueOf(rs.getDate(i)));
				}if(colTypeString.get(i-1).equals("SYMBOL")) {
					tmp.add(rs.getString(i).toString());
				}if(colTypeString.get(i-1).equals("DOUBLE")) {
					tmp.add(String.valueOf(rs.getDouble(i)));
				}if(colTypeString.get(i-1).equals("INT")) {
					tmp.add(String.valueOf(rs.getInt(i)));
				}if(colTypeString.get(i-1).equals("DATETIME")) {
					tmp.add(String.valueOf( rs.getTimestamp(i)));
				}if(colTypeString.get(i-1).equals("TIMESTAMP")) {
					tmp.add(String.valueOf( rs.getTimestamp(i)));
				}
//				tmp.add(rs.getObject(i).toString());
			}
			alldata.add(tmp);
		}
		Array2CSV(alldata,"test.csv");
		
	}
	
	public static void Array2CSV(ArrayList<ArrayList<String>> data, String path)
    {
        try {
              BufferedWriter out =new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path),"UTF-8"));         
              for (int i = 0; i < data.size(); i++)
              {
                  ArrayList<String> onerow=data.get(i);
                  for (int j = 0; j < onerow.size(); j++)
                  {
                	  	  if(j == onerow.size() - 1 ) {
                	  		  out.write(DelQuota(onerow.get(j)));
                	  	  }else {
                	  		  out.write(DelQuota(onerow.get(j)));
                	  		  out.write(",");
                	  	  }
                  }
                  out.newLine();
              }
              out.flush();
              out.close();

          } catch (Exception e) {
              e.printStackTrace();
          }

    }
    public static String DelQuota(String str)
    {
        String result = str;
        String[] strQuota = { "~", "!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "`", ";", "'", ",", ".", "/", ":", "/,", "<", ">", "?" };
        for (int i = 0; i < strQuota.length; i++)
        {
            if (result.indexOf(strQuota[i]) > -1)
                result = result.replace(strQuota[i], "");
        }
        return result;
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
				if(colTypeString.get(i-1).equals("DATE")) {
					System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getDate(i)));
				}if(colTypeString.get(i-1).equals("SYMBOL")) {
					System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getString(i)));
				}if(colTypeString.get(i-1).equals("DOUBLE")) {
					System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getDouble(i)));
				}if(colTypeString.get(i-1).equals("INT")) {
					System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getInt(i)));
				}if(colTypeString.get(i-1).equals("DATETIME")) {
					System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getTimestamp(i)));
				}if(colTypeString.get(i-1).equals("TIMESTAMP")) {
					System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getTimestamp(i)));
				}
			}
			System.out.print("\n");
		}
	}
}
