import java.io.BufferedWriter;
import java.io.FileOutputStream;
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
import java.util.Properties;

public class JDBCMakeFileTest {
	public static void main(String[] args){
		System.out.println("JDBCLoadTest");
		makeFiletest();
	}
	
	public static void makeFiletest() {
		HashMap<String, ArrayList> dataTable = new HashMap<String, ArrayList>();
		Connection conn = getConnection();
		try {

			Statement s = conn.createStatement();
			s.execute("trade=loadTable(\"dfs://USPrices\", `trade)");
			ResultSet rs =s.executeQuery("select * from trade");
			
			makeFile(rs);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
				
	}
	
	public static void makeFile(ResultSet rs) throws SQLException {
		
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
				tmp.add(rs.getObject(i).toString());
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
			conn = DriverManager.getConnection("jdbc:dolphindb://172.16.95.128:8921", info);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
		return conn;
		
	}
	
	public static void printData(ResultSet rs) throws SQLException {
		ResultSetMetaData resultSetMetaData = rs.getMetaData();
		int len = resultSetMetaData.getColumnCount();
		while (rs.next()) {			
			for (int i = 1; i <= len; ++i) {
				System.out.print(MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getObject(i)));
			}
			System.out.print("\n");
		}
	}
}
