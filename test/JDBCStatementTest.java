
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

import com.xxdb.DBConnection;

public class JDBCStatementTest {
	String HOST; 
	int PORT;
	
	@Before
    public void SetUp(){
        HOST = "localhost" ;
        PORT = 8848 ;
    }
	
	public static boolean CreateInMemoryTable(String host, Integer port){
        boolean success = false;
        DBConnection db = null;
        try{
            String script = "t = table(1..10 as id, 11..20 as val)";
            db = new DBConnection();
            db.connect(host, port);
            db.run(script);
            success = true;
        }catch(Exception e){
            e.printStackTrace();
            success = false;
        }finally{
            if(db!=null)
                db.close();
            return success;
        }
    }
    
    public static boolean CreateDfsTable(String host, Integer port){
    	boolean success = false;
    	DBConnection db = null;
    	try{
    		String script = "login(`admin, `123456); \n"+
    						"if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n"+
    						"t = table(1..10000 as id, take(1, 10000) as val) \n"+
    						"db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10001) \n"+
    						"db.createPartitionedTable(t, `pt, `id).append!(t) \n";
    		db = new DBConnection();
    		db.connect(host, port);
    		db.run(script);
    		success = true;
    	}catch(Exception e){
    		e.printStackTrace();
    		success = false;
    	}finally{
    		if(db != null){
    			db.close();
    		}
    		return success;
    	}
    }
	
	@Test
	public void Test_statement_execQuery() throws Exception{
		boolean success = CreateDfsTable(HOST, PORT);
		org.junit.Assert.assertTrue(success);
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
    	Connection conn = null;
    	Statement stmt = null;
    	ResultSet rs = null;
    	try{
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		rs = stmt.executeQuery("select * from pt");
    		ResultSetMetaData rsmd = rs.getMetaData();
    		int len = rsmd.getColumnCount();
    		org.junit.Assert.assertEquals(len, 2);
    	}catch(Exception e){
    		e.printStackTrace();
    	}finally{
    		if(rs != null){
    			try{
    				rs.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(stmt != null){
    			try{
    				stmt.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    		if(conn != null){
    			try{
    				conn.close();
    			}catch(SQLException e){
    				e.printStackTrace();
    			}
    		}
    	}
	}
	
}
