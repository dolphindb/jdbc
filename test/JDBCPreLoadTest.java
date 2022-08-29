package test;

import com.xxdb.DBConnection;
import com.xxdb.data.BasicTable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class JDBCPreLoadTest {
    static String HOST = JDBCTestUtil.HOST;
    static int PORT = JDBCTestUtil.PORT;
    static String url = "jdbc:dolphindb://"+HOST+":"+PORT;
    Connection conn;
    Properties info = new Properties();
    public static boolean createPartitionTable(){
        boolean success = false;
        DBConnection db = null;

        try{
            String script = "n=1000000\n" +
                    "date=take(2006.01.01..2006.01.31, n);\n" +
                    "x=rand(10.0, n);\n" +
                    "t=table(date, x);\n" +
                    "id = rand(20.0,n);\n" +
                    "t2 = table(date,id);\n" +
                    "login(\"admin\",\"123456\")\n" +
                    "if(existsDatabase(\"dfs://valuedb\")){dropDatabase(\"dfs://valuedb\")}\n" +
                    "db=database(\"dfs://valuedb\", VALUE, 2006.01.01..2006.01.31)\n" +
                    "pt = db.createPartitionedTable(t, `pt, `date);\n" +
                    "dt = db.createPartitionedTable(t2,`dt,`date)\n" +
                    "pt.append!(t);\n" +
                    "dt.append!(t2);";
            String script2 = "n = 1000000\n" +
                    "date = take(2006.01.01..2006.01.31,n);\n" +
                    "de = rand(30.0,n)\n" +
                    "t3 = table(date,de)\n" +
                    "if(existsDatabase(\"dfs://testValue\")){\n" +
                    "    dropDatabase(\"dfs://testValue\")\n" +
                    "}\n" +
                    "db2 = database(\"dfs://testValue\",VALUE,2006.01.01..2006.01.31)\n" +
                    "nt = db2.createPartitionedTable(t3,`nt,`date);\n" +
                    "nt.append!(t3);";
            db = new DBConnection();
            db.connect(HOST, PORT);
            db.run(script);
            db.run(script2);
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
    @Before
    public void setup() throws ClassNotFoundException {
        info.put("user","admin");
        info.put("password","123456");
        Class.forName("com.dolphindb.jdbc.Driver");
        Assert.assertTrue(createPartitionTable());
    }

    @Test
    public void test_PreLoad_normal() throws SQLException, IOException {
        conn = DriverManager.getConnection(url+"?tb_pt=dfs://valuedb+pt",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select top 100 * from pt");
        ResultSet resultSet = stm.executeQuery("select top 100 * from loadTable(\"dfs://valuedb\",\"pt\")");
        Assert.assertTrue(resultSet.next());
        Assert.assertTrue(rs.next());
        System.out.println(rs);
        while(rs.next() && resultSet.next()){
            System.out.println(rs.getString(1)+" "+rs.getString(2));
            Assert.assertEquals(resultSet.getString(1),rs.getString(1));
            Assert.assertEquals(resultSet.getString(2),rs.getString(2));
        }
    }

    @Test(expected = SQLException.class)
    public void test_PreLoad_nullTable() throws SQLException {
        conn = DriverManager.getConnection(url+"tb_ft=dfs://valuedb+ft",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select * from ft");
        Assert.assertTrue(rs.next());
    }

    @Test(expected = SQLException.class)
    public void test_PreLoad_nullLoad() throws SQLException {
        conn = DriverManager.getConnection(url,info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select * from dt");
        Assert.assertTrue(rs.next());
}

    @Test
    public void test_PreLoad_multiTable() throws IOException, SQLException {
        conn = DriverManager.getConnection(url+"?tb_pt=dfs://valuedb+pt&tb_dt=dfs://valuedb+dt",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select top 100 * from ej(dt,pt,`date)");
        ResultSet resultSet = stm.executeQuery("select top 100 * from ej(loadTable(\"dfs://valuedb\",\"dt\")" +
                ",loadTable(\"dfs://valuedb\",\"pt\"),`date)");
        Assert.assertTrue(resultSet.next());
        Assert.assertTrue(rs.next());
        while(rs.next()&&resultSet.next()){
            System.out.println(rs.getString(1)+" "+rs.getString(2)+" "+rs.getString(3));
            Assert.assertEquals(resultSet.getString(1),rs.getString(1));
            Assert.assertEquals(resultSet.getString(2),rs.getString(2));
            Assert.assertEquals(resultSet.getString(3),rs.getString(3));
        }
    }

    @Test
    public void test_PreLoad_multiTable_multiDb() throws IOException, SQLException {
        conn = DriverManager.getConnection(url+"?tb_pt=dfs://valuedb+pt&tb_nt=dfs://testValue+nt",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select top 100 * from pt;");
        ResultSet rs2 = stm.executeQuery("select top 100 * from nt;");
        ResultSet rs3 = stm.executeQuery("select top 100 * from ej(pt,nt,`date)");
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs2.next());
        Assert.assertTrue(rs3.next());
        while(rs.next()&& rs2.next() && rs3.next()){
            System.out.println(rs.getString(1)+" "+rs.getString(2));
            System.out.println(rs2.getString(1)+" "+rs2.getString(2));
            System.out.println(rs3.getString(1)+" "+rs3.getString(2)+" "+rs3.getString(3));
        }
    }
}
