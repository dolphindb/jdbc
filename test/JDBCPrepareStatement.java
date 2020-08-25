
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

import com.xxdb.DBConnection;

import static org.hamcrest.CoreMatchers.containsString;

public class JDBCPrepareStatement {
	String HOST; 
	int PORT;
	
	@Before
    public void SetUp(){
        HOST = "localhost" ;
        PORT = 8848 ;
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
	public void Test_prepareStatement_inmemory_query_SetInt() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
    	Connection conn = null;
    	Statement stmt = null;
    	PreparedStatement pstmt = null;
    	ResultSet rs = null;
    	try{
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("t=table(1..100 as id, 201..300 as val)");
    		pstmt = conn.prepareStatement("select * from t where id <= ?");
    		pstmt.setInt(1, 10);
    		rs = pstmt.executeQuery();
    		for(int i=1; i<=10; i++){
    			rs.absolute(i);
    			org.junit.Assert.assertEquals(rs.getInt(1), i);
    		}
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

	@Test
	public void Test_prepareStatement_inmemory_query_SetString() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			stmt.execute("t=table(take('aa' 'bb' 'cc' 'dd' 'ee',100) as id, 201..300 as val)");
			pstmt = conn.prepareStatement("select * from t where id = ?");
			pstmt.setString(1, "dd");
			rs = pstmt.executeQuery();
			for(int i=1; i<=20; i++){
				rs.absolute(i);
				org.junit.Assert.assertEquals(rs.getString(1), "dd");
			}
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

	@Test
	public void Test_prepareStatement_inmemory_query_SetFloat() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			stmt.execute("t=table(take(1.1f 1.2f 1.3f 1.4f 1.5f,100) as id, 201..300 as val)");
			pstmt = conn.prepareStatement("select * from t where id = ?");
			pstmt.setFloat(1, 1.1f);
			rs = pstmt.executeQuery();
			for(int i=1; i<=20; i++){
				rs.absolute(i);
				org.junit.Assert.assertEquals(rs.getFloat(1),1.1f,1);
			}
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

	@Test

	public void Test_prepareStatement_inmemory_query_SetDouble() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			stmt.execute("t=table(take(1.1 1.2 1.3 1.4 1.5,100) as id, 201..300 as val)");
			pstmt = conn.prepareStatement("select * from t where id = ?");
			pstmt.setDouble(1, 1.1);
			rs = pstmt.executeQuery();
			for(int i=1; i<=20; i++){
				rs.absolute(i);
				org.junit.Assert.assertEquals(rs.getDouble(1), 1.1,1);
			}
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

	@Test
	public void Test_prepareStatement_inmemory_query_executeUpdate() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			stmt.execute("t=table(1..100 as id, 201..300 as val)");
			pstmt = conn.prepareStatement("insert into t values(?,?)");
			pstmt.setInt(1,101);
			pstmt.setInt(2,301);
			pstmt.executeUpdate();
			pstmt = conn.prepareStatement("select * from t");
			rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
			rs.absolute(101);
			org.junit.Assert.assertEquals(rs.getInt(1), 101);
			org.junit.Assert.assertEquals(rs.getInt(2), 301);
			pstmt = conn.prepareStatement("update t set val=? where id=?");
			pstmt.setInt(1,333);
			pstmt.setInt(2,101);
			pstmt.executeUpdate();
			pstmt = conn.prepareStatement("select * from t");
			rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
			rs.absolute(101);
			org.junit.Assert.assertEquals(rs.getInt(2), 333);
			pstmt = conn.prepareStatement("delete from t where id=?");
			pstmt.setInt(1,101);
			pstmt.executeUpdate();
			pstmt = conn.prepareStatement("select * from t");
			rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
		    rs.last();
		    int rowCount = rs.getRow();
			org.junit.Assert.assertEquals(rowCount, 100);
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

	@Test
	public void Test_prepareStatement_dfs_query_setInt() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
    	Connection conn = null;
    	Statement stmt = null;
    	PreparedStatement pstmt = null;
    	ResultSet rs = null;
    	try{
    		Class.forName(JDBC_DRIVER);
    		conn = DriverManager.getConnection(url);
    		stmt = conn.createStatement();
    		stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
    		pstmt = conn.prepareStatement("select * from pt where id <= ?");
    		pstmt.setInt(1, 10);
    		rs = pstmt.executeQuery();
    		for(int i=1; i<=10; i++){
    			rs.absolute(i);
    			org.junit.Assert.assertEquals(rs.getInt(1), i);
    		}
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

	@Test
	public void Test_prepareStatement_dfs_query_setString() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			String script = "login(`admin, `123456); \n"+
					"if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n"+
					"t = table(1..10000 as id, take('aa' 'bb' 'cc' 'dd' 'ee',10000) as val) \n"+
					"db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10001) \n"+
					"db.createPartitionedTable(t, `pt, `id).append!(t) \n";
			stmt.execute(script);
			stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
			pstmt = conn.prepareStatement("select * from pt where val = ?");
			pstmt.setString(1, "dd");
			rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getString(2));
//			}
			for(int i=1; i<=200; i++){
				rs.absolute(i);
				org.junit.Assert.assertEquals(rs.getString(2), "dd");
			}
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

	@Test
	public void Test_prepareStatement_dfs_query_setFloat() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			String script = "login(`admin, `123456); \n"+
					"if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n"+
					"t = table(1..10000 as id, take(1.1f 1.2f 1.3f 1.4f 1.5f,10000) as val) \n"+
					"db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10001) \n"+
					"db.createPartitionedTable(t, `pt, `id).append!(t) \n";
			stmt.execute(script);
			stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
			pstmt = conn.prepareStatement("select * from pt where val = ?");
			pstmt.setFloat(1, 1.1f);
			rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getFloat(2));
//			}
			for(int i=1; i<=200; i++){
				rs.absolute(i);
				org.junit.Assert.assertEquals(rs.getFloat(2), 1.1f,1);
			}
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

	@Test
	public void Test_prepareStatement_dfs_query_setDouble() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			String script = "login(`admin, `123456); \n"+
					"if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n"+
					"t = table(1..10000 as id, take(1.1 1.2 1.3 1.4 1.5,10000) as val) \n"+
					"db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10001) \n"+
					"db.createPartitionedTable(t, `pt, `id).append!(t) \n";
			stmt.execute(script);
			stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
			pstmt = conn.prepareStatement("select * from pt where val = ?");
			pstmt.setDouble(1, 1.1);
			rs = pstmt.executeQuery();
			for(int i=1; i<=200; i++){
				rs.absolute(i);
				org.junit.Assert.assertEquals(rs.getDouble(2), 1.1,1);
			}
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

	@Test
	public void Test_prepareStatement_dfs_query_executeUpdate_() throws Exception{
		String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
		String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
		Connection conn = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String s = "only local in-memory table can update";
		try{
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			String script = "login(`admin, `123456); \n"+
					"if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n"+
					"t = table(1..10000 as id, 10001..20000 as val) \n"+
					"db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10002) \n"+
					"db.createPartitionedTable(t, `pt, `id).append!(t) \n";
			stmt.execute(script);
			stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
			stmt.execute("t1 = table(10001 as id,20001 as val)");
			pstmt = conn.prepareStatement("pt.append!(t1)");
			pstmt.executeUpdate();
			pstmt = conn.prepareStatement("select * from pt");
			rs = pstmt.executeQuery();
			rs.absolute(10001);
			org.junit.Assert.assertEquals(rs.getInt(1), 10001);
			org.junit.Assert.assertEquals(rs.getInt(2), 20001);
		}catch(Exception e){
			e.printStackTrace();
		}
		try {
			pstmt = conn.prepareStatement("update pt set val=20002 where id=10001");
			pstmt.executeUpdate();
		}catch(Exception e){
			org.junit.Assert.assertThat(e.getMessage(),containsString(s));
		}
		try {
			pstmt = conn.prepareStatement("delete from pt where id=10001");
			pstmt.executeUpdate();
		}catch(Exception e){
			org.junit.Assert.assertThat(e.getMessage(),containsString(s));
		}finally {
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

