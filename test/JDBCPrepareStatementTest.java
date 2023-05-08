import com.dolphindb.jdbc.JDBCResultSet;
import com.xxdb.DBConnection;
import com.xxdb.data.BasicDate;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Scalar;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.containsString;
//import com.xxdb.DBConnection;

public class JDBCPrepareStatementTest {
    static String HOST = JDBCTestUtil.HOST;
    static int PORT = JDBCTestUtil.PORT;
    Properties LOGININFO = new Properties();
    String JDBC_DRIVER;
    String DB_URL ;
    @Before
    public void SetUp(){
        JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        LOGININFO = new Properties();
        LOGININFO.put("user", "admin");
        LOGININFO.put("password", "123456");
        DB_URL = "jdbc:dolphindb://"+HOST+":"+PORT;
    }
    public static boolean CreateDfsTable(String host, Integer port) {
        boolean success = false;
        DBConnection db = null;
        try {
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n" +
                    "t = table(1..10000 as id, take(1, 10000) as val) \n" +
                    "db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10001) \n" +
                    "db.createPartitionedTable(t, `pt, `id).append!(t) \n";
            db = new DBConnection();
            db.connect(host, port);
            db.run(script);
            success = true;
        } catch (Exception e) {
            e.printStackTrace();
            success = false;
        } finally {
            if (db != null) {
                db.close();
            }
            return success;
        }
    }

    @Test
    public  void TestPrepareStatement() throws ClassNotFoundException,SQLException{
        CreateDfsTable(HOST,PORT);
        Properties info = new Properties();
        info.put("user", "admin");
        info.put("password", "123456");
        Connection conn = null;
        String preSql="select count(*) from loadTable('dfs://db_testStatement','pt') group by id ";
        PreparedStatement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL, info);
        stmt = conn.prepareStatement(preSql);
        stmt.executeQuery();
    }

    @Test
    public void Test_prepareStatement_inmemory_query_SetInt() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..100 as id, 201..300 as val)");
            pstmt = conn.prepareStatement("select * from t where id <= ?");
            pstmt.setInt(1, 10);
            rs = pstmt.executeQuery();
            for (int i = 1; i <= 10; i++) {
                rs.absolute(i);
                org.junit.Assert.assertEquals(rs.getInt(1), i);
            }
            ResultSetMetaData rsmd = rs.getMetaData();
            int len = rsmd.getColumnCount();
            org.junit.Assert.assertEquals(len, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_SetString() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(take('aa' 'bb' 'cc' 'dd' 'ee',100) as id, 201..300 as val)");
            pstmt = conn.prepareStatement("select * from t where id = ?");
            pstmt.setString(1, "dd");
            rs = pstmt.executeQuery();
            for (int i = 1; i <= 20; i++) {
                rs.absolute(i);
                org.junit.Assert.assertEquals(rs.getString(1), "dd");
            }
            ResultSetMetaData rsmd = rs.getMetaData();
            int len = rsmd.getColumnCount();
            org.junit.Assert.assertEquals(len, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_SetFloat() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(take(1.1f 1.2f 1.3f 1.4f 1.5f,100) as id, 201..300 as val)");
            pstmt = conn.prepareStatement("select * from t where id = ?");
            pstmt.setFloat(1, 1.1f);
            rs = pstmt.executeQuery();
//			while (rs.next()) {
//				System.out.println(rs.getFloat(1) + " " + rs.getInt(2));
//			}
//			for(int i=1; i<=20; i++){
//				rs.absolute(i);
////				org.junit.Assert.assertEquals(rs.getFloat(1),1.1f,0);
//				System.out.println(rs.getFloat("id"));
//			}
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_SetDouble() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(take(1.1 1.2 1.3 1.4 1.5,100) as id, 201..300 as val)");
            pstmt = conn.prepareStatement("select * from t where id = ?");
            pstmt.setDouble(1, 1.1);
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getDouble(1)+" "+rs.getInt(2));
//			}
            for (int i = 1; i <= 20; i++) {
                rs.absolute(i);
                org.junit.Assert.assertEquals(rs.getDouble("id"), 1.1, 0);
            }
            ResultSetMetaData rsmd = rs.getMetaData();
            int len = rsmd.getColumnCount();
            org.junit.Assert.assertEquals(len, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_setNull_Boolean_byte_bytes() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        java.sql.Time insertTime;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..100 as id, 201..300 as val)");
            //setNull
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 101);
            pstmt.setNull(2, Types.INTEGER);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            rs.absolute(101);
            org.junit.Assert.assertEquals(rs.getInt(2), 0);
            //setBoolean
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 102);
            pstmt.setBoolean(2,false);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            rs.absolute(102);
            org.junit.Assert.assertEquals(rs.getBoolean(2), false);
            //setByte
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 103);
            pstmt.setByte(2,(byte) 100 );
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            rs.absolute(103);
            org.junit.Assert.assertEquals(rs.getByte(2), 100);
            //setBytes
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 104);
            pstmt.setBytes(2,new byte[]{2});
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            rs.absolute(104);
            org.junit.Assert.assertEquals(rs.getInt(2), 2);
            //setBigDecimal
//			pstmt = conn.prepareStatement("insert into t values(?,?)");
//			pstmt.setInt(1, 105);
//			BigDecimal a =new BigDecimal("12");
//			System.out.println(a);
//			pstmt.setBigDecimal(2,a);
//			pstmt.executeUpdate();
//			pstmt = conn.prepareStatement("select * from t");
//			rs = pstmt.executeQuery();
//			rs.absolute(105);
//			org.junit.Assert.assertEquals(rs.getBigDecimal(2), 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_setTime() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        java.sql.Time insertTime;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..3 as id,[13:30:10,13:30:11,13:30:12] as time)");
            //setTime
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 4);
            Time t = new Time(13,30,13);
            pstmt.setTime(2,t);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getTime(2));
//			}
            rs.absolute(4);
            org.junit.Assert.assertEquals(rs.getTime(2),t);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_setDate() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..3 as id,[2020.08.01,2020.08.02,2020.08.03] as date)");
            //setDate
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 4);
            Date date = Date.valueOf("2020-08-04");
            pstmt.setDate(2,date);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            rs.absolute(4);
            org.junit.Assert.assertEquals(rs.getDate(2), date);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    @Test
    public void Test_prepareStatement_inmemory_query_setShort() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..3 as id,[121h 122h 123h] as val)");
            //setDate
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 4);
            pstmt.setShort(2,(short)124);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            rs.absolute(4);
            org.junit.Assert.assertEquals(rs.getShort(2), 124);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_setLong() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..3 as id,[1l 2l 3l] as val)");
            //setDate
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 4);
            pstmt.setLong(2,4L);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            rs.absolute(4);
            org.junit.Assert.assertEquals(rs.getLong(2), 4l);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_setTimestamp() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..3 as id,[2012.06.13T13:30:10.006 2012.06.13T13:30:10.007 2012.06.13T13:30:10.008] as timestamp)");
            //setDate
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 4);
            Timestamp t = Timestamp.valueOf("2012-06-13 13:30:10.009");
            pstmt.setTimestamp(2, t);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getTimestamp(2));
//			}
            rs.absolute(4);
            org.junit.Assert.assertEquals(rs.getTimestamp(2), t);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_clearParameters() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String s = "No value specified for parameter 1";
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..100 as id, 201..300 as val)");
            pstmt = conn.prepareStatement("select * from t where id <= ?");
            pstmt.setInt(1, 10);
            pstmt.clearParameters();
            rs = pstmt.executeQuery();
        } catch (Exception e) {
            org.junit.Assert.assertThat(e.getMessage(), containsString(s));
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_setObject() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String s = "No value specified for parameter 1";
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..3 as id,[1.1f 1.2f 1.3f] as val)");
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setObject(1, 4);
            pstmt.setObject(2, 1.4f);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t ");
            rs = pstmt.executeQuery();
            rs.absolute(4);
            org.junit.Assert.assertEquals(rs.getInt(1), 4);
            org.junit.Assert.assertEquals(rs.getDouble(2), 1.4f,0);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_execute() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            //insert
            stmt.execute("t=table(1..100 as id, 201..300 as val)");
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 101);
            pstmt.setInt(2, 301);
            pstmt.execute();
            pstmt = conn.prepareStatement("select * from t");
            pstmt.execute();
            rs = pstmt.getResultSet();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
            rs.absolute(101);
            org.junit.Assert.assertEquals(rs.getInt(1), 101);
            org.junit.Assert.assertEquals(rs.getInt(2), 301);
            //update
            pstmt = conn.prepareStatement("update t set val=? where id=?");
            pstmt.setInt(1, 333);
            pstmt.setInt(2, 101);
            pstmt.execute();
            pstmt = conn.prepareStatement("select * from t");
            pstmt.execute();
            rs = pstmt.getResultSet();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
            rs.absolute(101);
            org.junit.Assert.assertEquals(rs.getInt(2), 333);
            //delete
            pstmt = conn.prepareStatement("delete from t where id=?");
            pstmt.setInt(1, 101);
            pstmt.execute();
            pstmt = conn.prepareStatement("select * from t");
            pstmt.execute();
            rs = pstmt.getResultSet();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
            rs.last();
            int rowCount = rs.getRow();
            org.junit.Assert.assertEquals(rowCount, 100);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_executeUpdate() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..100 as id, 201..300 as val)");
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 101);
            pstmt.setInt(2, 301);
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
            pstmt.setInt(1, 333);
            pstmt.setInt(2, 101);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
            rs.absolute(101);
            org.junit.Assert.assertEquals(rs.getInt(2), 333);
            pstmt = conn.prepareStatement("delete from t where id=?");
            pstmt.setInt(1, 101);
            pstmt.executeUpdate();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
            rs.last();
            int rowCount = rs.getRow();
            org.junit.Assert.assertEquals(rowCount, 100);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_Batch() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..100 as id, 201..300 as val)");
            pstmt = conn.prepareStatement("insert into t values(?,?)");
            pstmt.setInt(1, 101);
            pstmt.setInt(2, 301);
            pstmt.addBatch();
            pstmt.setInt(1,102);
            pstmt.setInt(2, 302);
            pstmt.addBatch();
//			pstmt.clearBatch();
            pstmt.executeBatch();
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
//			pstmt.clearBatch();
//			pstmt = conn.prepareStatement("select * from t");
//			rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getInt(2));
//			}
//			rs.absolute(102);
//			org.junit.Assert.assertEquals(rs.getInt(1), 101);
//			org.junit.Assert.assertEquals(rs.getInt(2), 301);
//			rs.last();
//			int rowCount = rs.getRow();
//			org.junit.Assert.assertEquals(rowCount, 102);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    @Test
    public void Test_prepareStatement_dfs_query_Batch1() throws Exception {
        String tableName = "trade";
        String dataBase = "dfs://test_jdbc_sql";
        Connection conn = JDBCSQLSelectTest.getConnection();
        String sql = "insert into trade values(?,?,?,?,?,?,?,?)";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd");

        PreparedStatement ps = null;


        StringBuilder sb = new StringBuilder();
        try {
            ps = conn.prepareStatement(sql);


            ps = conn.prepareStatement(sql);
            StringBuilder sb2 = new StringBuilder();
            sb2.append("if(existsDatabase(\""+ dataBase +"\"))dropDatabase(\""+ dataBase +"\")\n");
            sb2.append("db=database(\""+ dataBase +"\", RANGE, `A`F`K`O`S`ZZZ)\n");
            sb2.append("t1=table(100:0, `PERMNO`date`TICKER`PRC`VOL`BID`ASK`SHROUT, [INT, DATE, SYMBOL, DOUBLE, INT, DOUBLE, DOUBLE,INT])\n");
            sb2.append("db.createPartitionedTable(t1,`trade, `TICKER)\n");
            ps.execute(sb2.toString());
            ps.execute("trade=loadTable(\""+ dataBase +"\", `"+ tableName +")");

            ps.clearBatch();
            ps.setInt(1, Integer.parseInt("1"));
            LocalDate localDate = LocalDate.parse("2010.03.13", formatter);
            ps.setObject(2, new BasicDate(localDate));
            ps.setString(3, "NULL");
            ps.setNull(4, Types.DOUBLE);
            ps.setNull(5, Types.INTEGER);
            ps.setDouble(6, Double.parseDouble("6.1"));
            ps.setDouble(7, Double.parseDouble("6.1"));
            ps.setNull(8, Types.INTEGER);
            ps.addBatch();

            ps.setInt(1, Integer.parseInt("2"));
            ps.setObject(2, new BasicDate(localDate));
            ps.setString(3, "NULL");
            ps.setNull(4, Types.DOUBLE);
            ps.setNull(5, Types.INTEGER);
            ps.setDouble(6, Double.parseDouble("6.1"));
            ps.setDouble(7, Double.parseDouble("6.1"));
            ps.setNull(8, Types.INTEGER);
            ps.addBatch();
            ps.executeBatch();
            ps.clearBatch();


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_getMetaData() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1..100 as id, 201..300 as val)");
            pstmt = conn.prepareStatement("select * from t");
            rs = pstmt.executeQuery();
            String tableName = rs.getMetaData().getTableName(1);
            org.junit.Assert.assertEquals(tableName, "id");
            String columnName = rs.getMetaData().getColumnName(1);
            org.junit.Assert.assertEquals(columnName, "id");
            int columnCount = rs.getMetaData().getColumnCount();
            org.junit.Assert.assertEquals(columnCount, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_executeQuery1() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t=table(1 2 3 4 5 6 7 8 10 9 as id, 21 22 23 24 25 22 27 28 29 30 as val)");
            //where
            pstmt = conn.prepareStatement("select * from t where id = ?");
            pstmt.setInt(1,1);
            rs = pstmt.executeQuery();
            rs.last();
            int rowCount = rs.getRow();
            org.junit.Assert.assertEquals(rowCount,1);
            rs.absolute(1);
            org.junit.Assert.assertEquals(rs.getInt(2), 21);
            //order by
            pstmt = conn.prepareStatement("select * from t order by id");
            rs = pstmt.executeQuery();
            rs.absolute(10);
            org.junit.Assert.assertEquals(rs.getInt(1), 10);
            org.junit.Assert.assertEquals(rs.getInt(2), 29);
            //group by having
            pstmt = conn.prepareStatement("select count(val) from t group by val having count(val)>1");
            rs = pstmt.executeQuery();
            rs.last();
            int rowCount1 = rs.getRow();
            org.junit.Assert.assertEquals(rowCount1,1);
            rs.absolute(1);
            org.junit.Assert.assertEquals(rs.getInt(2), 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_executeQuery2() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("t = table(`IBM`IBM`IBM`IBM`C`C`C`C as sym, 09:30m 09:30m 09:31m 09:31m 09:30m 09:30m 09:31m 09:31m as time, 1..8 as qty, 10 .. 17 as price)");
            //cgroup by
            pstmt = conn.prepareStatement("select wavg(price, qty) as wvap from t where sym = 'IBM' cgroup by time order by time");
            rs = pstmt.executeQuery();
            rs.absolute(1);
            org.junit.Assert.assertEquals(rs.getString(1), "09:30m");
            org.junit.Assert.assertEquals(rs.getDouble(2), 10.666667,0.0001);
            rs.absolute(2);
            org.junit.Assert.assertEquals(rs.getString(1), "09:31m");
            org.junit.Assert.assertEquals(rs.getInt(2), 12);
            rs.last();
            int rowCount = rs.getRow();
            org.junit.Assert.assertEquals(rowCount,2);
            //context by
            pstmt = conn.prepareStatement("select wavg(price, qty) as wvap from t context by sym");
            rs = pstmt.executeQuery();
            rs.last();
            int rowCount1 = rs.getRow();
            org.junit.Assert.assertEquals(rowCount1,8);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_inmemory_query_executeQuery3() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("sym = `C`MS`MS`MS`IBM`IBM`C`C`C$SYMBOL\n" +
                    "price= 49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29\n" +
                    "qty = 2200 1900 2100 3200 6800 5400 1300 2500 8800\n" +
                    "timestamp = [09:34:07,09:35:42,09:36:51,09:36:59,09:35:47,09:36:26,09:34:16,09:35:26,09:36:12]\n" +
                    "t = table(timestamp, sym, qty, price)");
            //pivot by
            pstmt = conn.prepareStatement("select last(price) from t pivot by timestamp.minute(), sym");
            rs = pstmt.executeQuery();
            String columnName = rs.getMetaData().getColumnName(1);
            org.junit.Assert.assertEquals(columnName, "minute_timestamp");
            String columnName1 = rs.getMetaData().getColumnName(2);
            org.junit.Assert.assertEquals(columnName1, "C");
            String columnName2 = rs.getMetaData().getColumnName(3);
            org.junit.Assert.assertEquals(columnName2, "IBM");
            rs.last();
            int rowCount = rs.getRow();
            org.junit.Assert.assertEquals(rowCount,3);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_dfs_query_setInt() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
            pstmt = conn.prepareStatement("select * from pt where id <= ?");
            pstmt.setInt(1, 10);
            rs = pstmt.executeQuery();
            for (int i = 1; i <= 10; i++) {
                rs.absolute(i);
                org.junit.Assert.assertEquals(rs.getInt(1), i);
            }
            ResultSetMetaData rsmd = rs.getMetaData();
            int len = rsmd.getColumnCount();
            org.junit.Assert.assertEquals(len, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_dfs_query_setString() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n" +
                    "t = table(1..10000 as id, take('aa' 'bb' 'cc' 'dd' 'ee',10000) as val) \n" +
                    "db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10001) \n" +
                    "db.createPartitionedTable(t, `pt, `id).append!(t) \n";
            stmt.execute(script);
            stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
            pstmt = conn.prepareStatement("select * from pt where val = ?");
            pstmt.setString(1, "dd");
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getString(2));
//			}
            for (int i = 1; i <= 200; i++) {
                rs.absolute(i);
                org.junit.Assert.assertEquals(rs.getString(2), "dd");
            }
            ResultSetMetaData rsmd = rs.getMetaData();
            int len = rsmd.getColumnCount();
            org.junit.Assert.assertEquals(len, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_dfs_query_setFloat() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n" +
                    "t = table(1..10000 as id, take(1.1f 1.2f 1.3f 1.4f 1.5f,10000) as val) \n" +
                    "db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10001) \n" +
                    "db.createPartitionedTable(t, `pt, `id).append!(t) \n";
            stmt.execute(script);
            stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");
            pstmt = conn.prepareStatement("select * from pt where val = ?");
            pstmt.setFloat(1, 1.1f);
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getFloat(2));
//			}
//			for (int i = 1; i <= 200; i++) {
//				rs.absolute(i);
//				org.junit.Assert.assertEquals(rs.getFloat(2), 1.1f, 1);
//			}
            ResultSetMetaData rsmd = rs.getMetaData();
            int len = rsmd.getColumnCount();
            org.junit.Assert.assertEquals(len, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_dfs_query_setDouble() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n" +
                    "t = table(1..10000 as id, take(1.1 1.2 1.3 1.4 1.5,10000) as val) \n" +
                    "db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10001) \n" +
                    "db.createPartitionedTable(t, `pt, `id).append!(t) \n";
            stmt.execute(script);
            stmt.execute("pt=loadTable('dfs://db_testStatement', 'pt')");

            pstmt = conn.prepareStatement("select * from pt where val = ?");
            pstmt.setDouble(1, 1.1);
            rs = pstmt.executeQuery();
//			while(rs.next()){
//				System.out.println(rs.getInt(1)+" "+rs.getDouble(2));
//			}
            for (int i = 1; i <= 200; i++) {
                rs.absolute(i);
                org.junit.Assert.assertEquals(rs.getDouble(2), 2, 6);
                System.out.println(rs.getDouble(2));
            }
            ResultSetMetaData rsmd = rs.getMetaData();
            int len = rsmd.getColumnCount();
            org.junit.Assert.assertEquals(len, 2);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void Test_prepareStatement_dfs_query_executeUpdate_() throws Exception {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String s = "only local in-memory table can update";
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n" +
                    "t = table(1..10000 as id, 10001..20000 as val) \n" +
                    "db=database('dfs://db_testStatement', RANGE, 1 2001 4001 6001 8001 10002) \n" +
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
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            pstmt = conn.prepareStatement("update pt set val=20002 where id=10001");
            pstmt.executeUpdate();
        } catch (Exception e) {
            org.junit.Assert.assertThat(e.getMessage(), containsString(s));
        }
        try {
            pstmt = conn.prepareStatement("delete from pt where id=10001");
            pstmt.executeUpdate();
        } catch (Exception e) {
            org.junit.Assert.assertThat(e.getMessage(), containsString(s));
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void test_PreparedStatement_executeTable(){
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String s = "only local in-memory table can update";
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("use testOuRui;query()");
            rs = stmt.getResultSet();
            Assert.assertTrue(rs.next());
            System.out.println(rs.getString(1));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void test_PreparedStatement_executeScalar(){
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        JDBCResultSet rs = null;
        String s = "only local in-memory table can update";
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("use testOuRui;printStr('123')");
            rs = (JDBCResultSet) stmt.getResultSet();
            Assert.assertEquals("b123",rs.getResult().getString());
            Assert.assertTrue(rs.getResult().isScalar());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void test_PreparedStatement_executeVector(){
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        JDBCResultSet rs = null;
        String s = "only local in-memory table can update";
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            stmt.execute("use testOuRui;printVector()");
            rs = (JDBCResultSet) stmt.getResultSet();
            Assert.assertEquals("[1,2,3,4]",rs.getResult().getString());
            Assert.assertTrue(rs.getResult().isVector());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    @Test
    public void test_PreparedStatement_execute() throws ClassNotFoundException, SQLException {
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456";
        Connection conn = null;
        Statement stmt = null;
        JDBCResultSet rs = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        Statement stm = conn.createStatement();
        String sql = "login('admin','123456');\n " +
                "t = table(1..100 as id, norm(1.0,0.1,100) as prc,take(`C`E,100) as ticker, take(2018.01.01..2018.10.18,100) as date, norm(15.0,0.1,100) as bid)" +
                "share t as tt" ;
        stm.execute(sql);
        PreparedStatement  ps = conn.prepareStatement("select * from tt where  id = ?, date = ?, ticker = ?");
        ps.setObject(1,1);
        ps.setObject(2,new BasicDate(LocalDate.parse("2018-01-01")));
        ps.setObject(3,"C");
        ps.execute();
        Assert.assertTrue(ps.getResultSet().next());
        conn.close();
    }
    @After
    public void Destroy(){
        LOGININFO = null;
    }
}
