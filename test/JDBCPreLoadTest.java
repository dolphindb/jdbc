import com.dolphindb.jdbc.JDBCResultSet;
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
    static int COLPORT = JDBCTestUtil.COLPORT ;
    Connection conn;
    Properties info = new Properties();
    public static boolean createPartitionTable(){
        boolean success = false;
        DBConnection db = null;

        try{
            String script = "\n" +
                    "n=500800;\n" +
                    "date=take(2006.01.01..2006.01.31, n);\n" +
                    "x=rand(10.0, n);\n" +
                    "t=table(date, x);\n" +
                    "id = rand(20.0,n);\n" +
                    "t2 = table(date,id);\n" +
                    "t3 = table(date,id,x)\n" +
                    "if(existsDatabase(\"dfs://valuedb\")){dropDatabase(\"dfs://valuedb\")}\n" +
                    "db=database(\"dfs://valuedb\", VALUE, 2006.01.01..2006.01.31)\n" +
                    "pt = db.createPartitionedTable(t, `pt, `date);\n" +
                    "dt = db.createPartitionedTable(t2,`dt, `date);\n" +
                    "Order_tmp = db.createPartitionedTable(t3,`Order_tmp,`date);\n" +
                    "pt.append!(t);\n" +
                    "dt.append!(t2);\n" +
                    "Order_tmp.append!(t3);";
            String script2 = "n = 5000\n" +
                    "date = take(2006.01.01..2006.01.31,n);\n" +
                    "de = rand(30.0,n)\n" +
                    "t4 = table(date,de)\n" +
                    "if(existsDatabase(\"dfs://testValue\")){\n" +
                    "    dropDatabase(\"dfs://testValue\")\n" +
                    "}\n" +
                    "db2 = database(\"dfs://testValue\",VALUE,2006.01.01..2006.01.31)\n" +
                    "nt = db2.createPartitionedTable(t4,`nt,`date);\n" +
                    "nt.append!(t4);";
            db = new DBConnection();
            db.connect(HOST, PORT,"admin","123456");
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

    public static boolean createPartitionTable(String name){
        boolean success = false;
        DBConnection db = null;

        try{
            String script = "\n" +
                    "n=5008;\n" +
                    "date=take(2006.01.01..2006.01.31, n);\n" +
                    "x=rand(10.0, n);\n" +
                    "t=table(date, x);\n" +
                    "id = rand(20.0,n);\n" +
                    "t2 = table(date,id);\n" +
                    "t3 = table(date,id,x)\n" +
                    "if(existsDatabase(\"dfs://valuedb\")){dropDatabase(\"dfs://valuedb\")}\n" +
                    "db=database(\"dfs://valuedb\", VALUE, 2006.01.01..2006.01.31)\n" +
                    "pt = db.createPartitionedTable(t, `pt, `date);\n" +
                    "dt = db.createPartitionedTable(t2,`dt, `date);\n" +
                    name+" = db.createPartitionedTable(t3,`"+name+",`date);\n" +
                    "pt.append!(t);\n" +
                    "dt.append!(t2);\n" +
                    name+".append!(t3);";
            String script2 = "n = 500800\n" +
                    "date = take(2006.01.01..2006.01.31,n);\n" +
                    "de = rand(30.0,n)\n" +
                    "t3 = table(date,de)\n" +
                    "if(existsDatabase(\"dfs://testValue\")){\n" +
                    "    dropDatabase(\"dfs://testValue\")\n" +
                    "}\n" +
                    "db2 = database(\"dfs://testValue\",VALUE,2006.01.01..2006.01.31)\n" +
                    "pt = db2.createPartitionedTable(t3,`pt,`date);\n" +
                    "pt.append!(t3);\n" +
                    "nt = db2.createPartitionedTable(t3,`nt,`date);\n" +
                    "nt.append!(t3);";
            db = new DBConnection();
            db.connect(HOST, PORT,"admin","123456");
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
    //@Test
    public void test_PreLoad_normal_disconnected() throws SQLException, IOException {
        Connection conn1 = null;
        conn = DriverManager.getConnection(url+"?tb_pt=dfs://valuedb+pt&highAvailability=true",info);
        String url1 = "jdbc:dolphindb://"+HOST+":"+COLPORT+"?user=admin&password=123456";
        Statement stmt = null;
        conn1 = DriverManager.getConnection(url1);
        stmt = conn1.createStatement();
        try{
            stmt.execute("stopDataNode([\"192.168.1.167:18921\",\"192.168.1.167:18922\",\"192.168.1.167:18923\",\"192.168.1.167:18924\"])");
        }catch(Exception ex)
        {
            System.out.println(ex.getMessage());
        }
        stmt.execute(" sleep(2000)");
        try{
            stmt.execute("startDataNode([\"192.168.1.167:18921\",\"192.168.1.167:18922\",\"192.168.1.167:18923\",\"192.168.1.167:18924\"])");
        }catch(Exception ex)
        {
            System.out.println(ex.getMessage());
        }
        stmt.execute(" sleep(2000)");
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select  top  100 * from pt");
        ResultSet resultSet = stm.executeQuery("select top  100 * from loadTable(\"dfs://valuedb\",\"pt\")");
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
        conn = DriverManager.getConnection(url+"?tb_ft=dfs://valuedb+ft",info);
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
        while(rs.next()&& rs2.next()){
            System.out.println(rs.getString(1)+" "+rs.getString(2));
            System.out.println(rs2.getString(1)+" "+rs2.getString(2));
            System.out.println(rs3.getString(1)+" "+rs3.getString(2)+" "+rs3.getString(3));
        }
    }

    @Test
    public void test_PreLoad_allTable() throws SQLException {
         conn = DriverManager.getConnection(url+"?databasePath=dfs://valuedb",info);
         Statement stm = conn.createStatement();
         ResultSet rs = stm.executeQuery("select TOP 100 * from pt");
         int index=0;
        while(rs.next()){
            System.out.println((++index)+":"+rs.getString(1)+" "+rs.getString(2));
        }
        Assert.assertEquals(100,index);
        ResultSet resultSet = stm.executeQuery("select top 100 * from dt");
        int count = 0;
        while(resultSet.next()){
            System.out.println((++count)+":"+resultSet.getString(1)+" "+resultSet.getString(2));
        }
        Assert.assertEquals(100,count);
    }

    @Test
    public void test_Order_tmp() throws SQLException {
        conn = DriverManager.getConnection(url+"?databasePath=dfs://valuedb",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select TOP 101 * from Order_tmp");
        Assert.assertTrue(rs.next());
        int index = 0;
        while(rs.next()){
            System.out.println((index++)+":"+rs.getString(1)+" "+rs.getString(2));
        }
        Assert.assertEquals(100,index);
    }

    @Test
    public void test_Group_tmp() throws SQLException {
        Assert.assertTrue(createPartitionTable("Group_tmp"));
        conn = DriverManager.getConnection(url+"?databasePath=dfs://valuedb",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select TOP 101 * from Group_tmp");
        Assert.assertTrue(rs.next());
        int index = 0;
        while(rs.next()){
            System.out.println((index++)+":"+rs.getString(1)+" "+rs.getString(2));
        }
        Assert.assertEquals(100,index);
    }

    @Test
    public void test_Where_tmp() throws SQLException {
        Assert.assertTrue(createPartitionTable("Where_tmp"));
        conn = DriverManager.getConnection(url+"?databasePath=dfs://valuedb",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select TOP 101 * from Where_tmp");
        Assert.assertTrue(rs.next());
        int index = 0;
        while(rs.next()){
            System.out.println((index++)+":"+rs.getString(1)+" "+rs.getString(2));
        }
        Assert.assertEquals(100,index);
    }

    @Test
    public void test_Where_Order_group() throws SQLException {
        Assert.assertTrue(createPartitionTable("Where_Order_group"));
        conn = DriverManager.getConnection(url+"?databasePath=dfs://valuedb",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select TOP 101 * from Where_Order_group");
        Assert.assertTrue(rs.next());
        int index = 0;
        while(rs.next()){
            System.out.println((index++)+":"+rs.getString(1)+" "+rs.getString(2));
        }
        Assert.assertEquals(100,index);
    }

    @Test(expected = AssertionError.class)
    public void test_by() throws SQLException {
        Assert.assertTrue(createPartitionTable("by"));
        conn = DriverManager.getConnection(url+"?databasePath=dfs://valuedb",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select TOP 101 * from by");
        Assert.assertTrue(rs.next());
        int index = 0;
        while(rs.next()){
            System.out.println((index++)+":"+rs.getString(1)+" "+rs.getString(2));
        }
        Assert.assertEquals(100,index);
    }

    @Test
    public void test_Preload_Hash_PartitionedDatabase() throws SQLException, IOException {
        String script = "n=100;\n" +
                " cbool = take(true false false true,n);\n" +
                " cchar = take('a'..'z',n);\n" +
                " cshort = take(1h..200h,n);\n" +
                " cint = rand(1000,n);\n" +
                " clong = take(200l..2000l,n)\n" +
                " cdate = take(2011.08.16..2022.09.30,n)\n" +
                " cmonth = take(2012.01M..2022.10M,n)\n" +
                " ctime = take(00:00:00.001..23:59:59.999,n)\n" +
                " cminute = take(00:01m..23:59m,n)\n" +
                " csecond = take(00:00:01..23:59:59,n)\n" +
                " cdatetime = take(2011.01.01 00:00:01..2022.09.30 23:59:59,n)\n" +
                " ctimestamp = take(2022.09.30 00:00:00.001..2022.09.30 23:59:59.999,n)\n" +
                " cnanotime = take(23:59:58.000000001..23:59:58.000007016,n)\n" +
                " cnanotimestamp = take(2022.09.30 23:59:58.000000001..2022.09.30 23:59:58.000001112,n)\n" +
                " cfloat = rand(300.0f,n)\n" +
                " cdouble = rand(230.0,n)\n" +
                " cstring = take(\"hello\" \"world\" \"dolphindb\",n)\n" +
                " cdatehour = datehour(take(2011.01.01 01:00:00..2022.09.30 23:59:59,n))\n" +
                " cdecimal32 = decimal32(take(1..2022,n),2)\n" +
                " cdecimal64 = decimal64(take(2022..4044,n),2)\n" +
                " cdecimal128 = decimal128(take(4044..8088,n),2)\n" +
                " t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cfloat,cdouble,cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128)\n" +
                " if(existsDatabase(\"dfs://testPreload\")){\n" +
                "     dropDatabase(\"dfs://testPreload\")\n" +
                " }\n" +
                " db = database(\"dfs://testPreload\",HASH,[INT, 2])\n" +
                " pt = db.createPartitionedTable(t,`pt,`cint)\n" +
                " pt.append!(t)";
        DBConnection db = new DBConnection();
        db.connect(HOST,PORT,"admin","123456");
        db.run(script);
        db.close();
        conn = DriverManager.getConnection(url+"?databasePath=dfs://testPreload",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select TOP 101 * from pt");
        Assert.assertTrue(rs.next());
        int index = 0;
        do{
            System.out.println((index++)+":"+rs.getString(1)+" "+rs.getString(2));

        }while(rs.next());
        Assert.assertEquals(100,index);
    }

    @Test
    public void test_PreLoad_range_PartitionedDatabase() throws IOException, SQLException {
        String script = "n=1000000;\n" +
                " cbool = take(true false false true,n);\n" +
                " cchar = take('a'..'z',n);\n" +
                " cshort = take(1h..200h,n);\n" +
                " cint = rand(1000,n);\n" +
                " clong = take(200l..2000l,n)\n" +
                " cdate = take(2011.08.16..2022.09.30,n)\n" +
                " cmonth = take(2012.01M..2022.10M,n)\n" +
                " ctime = take(00:00:00.001..23:59:59.999,n)\n" +
                " cminute = take(00:01m..23:59m,n)\n" +
                " csecond = take(00:00:01..23:59:59,n)\n" +
                " cdatetime = take(2011.01.01 00:00:01..2022.09.30 23:59:59,n)\n" +
                " ctimestamp = take(2022.09.30 00:00:00.001..2022.09.30 23:59:59.999,n)\n" +
                " cfloat = rand(300.0f,n)\n" +
                " cdouble = rand(230.0,n)\n" +
                " cstring = take(\"hello\" \"world\" \"dolphindb\",n)\n" +
                " cdatehour = datehour(take(2011.01.01 01:00:00..2022.09.30 23:59:59,n))\n" +
                " cdecimal32 = decimal32(take(1..2022,n),2)\n" +
                " cdecimal64 = decimal64(take(2022..4044,n),2)\n" +
                " cdecimal128 = decimal128(take(4044..8088,n),2)\n" +
                " t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cfloat,cdouble,cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128)\n" +
                " if(existsDatabase(\"dfs://testPreload\")){\n" +
                "     dropDatabase(\"dfs://testPreload\")\n" +
                " }\n" +
                " db = database(\"dfs://testPreload\",RANGE,0 200 500 900 1000)\n" +
                " pt = db.createPartitionedTable(t,`pt,`cint)\n" +
                " pt.append!(t)";
        DBConnection db = new DBConnection();
        db.connect(HOST,PORT,"admin","123456");
        db.run(script);
        db.close();
        conn = DriverManager.getConnection(url+"?databasePath=dfs://testPreload",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select TOP 100 * from pt");
        Assert.assertTrue(rs.next());
        int index = 0;
        while(rs.next()){
            System.out.println((index++)+":"+rs.getString(1)+" "+rs.getString(2));
        }
        Assert.assertEquals(99,index);
    }

    @Test
    public void test_Preload_List_PartitonedTable() throws SQLException, IOException {
        String script = "n=1000000;\n" +
                " cbool = take(true false false true,n);\n" +
                " cchar = take('a'..'z',n);\n" +
                " cshort = take(1h..200h,n);\n" +
                " cint = rand(1000,n);\n" +
                " clong = take(200l..2000l,n)\n" +
                " cdate = take(2011.08.16..2022.09.30,n)\n" +
                " cmonth = take(2012.01M..2022.10M,n)\n" +
                " ctime = take(00:00:00.001..23:59:59.999,n)\n" +
                " cminute = take(00:01m..23:59m,n)\n" +
                " csecond = take(00:00:01..23:59:59,n)\n" +
                " cdatetime = take(2011.01.01 00:00:01..2022.09.30 23:59:59,n)\n" +
                " ctimestamp = take(2022.09.30 00:00:00.001..2022.09.30 23:59:59.999,n)\n" +
                " cfloat = rand(300.0f,n)\n" +
                " cdouble = rand(230.0,n)\n" +
                " cstring = take(\"hello\" \"world\" \"dolphindb\",n)\n" +
                " cdatehour = datehour(take(2011.01.01 01:00:00..2022.09.30 23:59:59,n))\n" +
                " cdecimal32 = decimal32(take(1..2022,n),2)\n" +
                " cdecimal64 = decimal64(take(2022..4044,n),2)\n" +
                " cdecimal128 = decimal128(take(4044..8088,n),2)\n" +
                " t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cfloat,cdouble,cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128)\n" +
                " if(existsDatabase(\"dfs://testPreload\")){\n" +
                "     dropDatabase(\"dfs://testPreload\")\n" +
                " }\n" +
                " db = database(\"dfs://testPreload\",LIST,[\"hello\" \"world\",\"dolphindb\"])\n" +
                " pt = db.createPartitionedTable(t,`pt,`cstring)\n" +
                " pt.append!(t)";
        DBConnection db = new DBConnection();
        db.connect(HOST,PORT,"admin","123456");
        db.run(script);
        db.close();
        conn = DriverManager.getConnection(url+"?databasePath=dfs://testPreload",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select TOP 101 * from pt");
        Assert.assertTrue(rs.next());
        int index = 0;
        while(rs.next()){
            System.out.println((index++)+":"+rs.getString(1)+" "+rs.getString(2));
        }
        Assert.assertEquals(100,index);
    }

    @Test
    public void test_Preload_COMPO_PartitionedTable() throws SQLException, IOException {
        String script = "n=1000000;\n" +
                " cbool = take(true false false true,n);\n" +
                " cchar = take('a'..'z',n);\n" +
                " cshort = take(1h..200h,n);\n" +
                " cint = rand(1000,n);\n" +
                " clong = take(200l..2000l,n)\n" +
                " cdate = take(2011.08.16..2022.09.30,n)\n" +
                " cmonth = take(2012.01M..2022.10M,n)\n" +
                " ctime = take(00:00:00.001..23:59:59.999,n)\n" +
                " cminute = take(00:01m..23:59m,n)\n" +
                " csecond = take(00:00:01..23:59:59,n)\n" +
                " cdatetime = take(2011.01.01 00:00:01..2022.09.30 23:59:59,n)\n" +
                " ctimestamp = take(2022.09.30 00:00:00.001..2022.09.30 23:59:59.999,n)\n" +
                " cfloat = rand(300.0f,n)\n" +
                " cdouble = rand(230.0,n)\n" +
                " cstring = take(\"hello\" \"world\" \"dolphindb\",n)\n" +
                " cdatehour = datehour(take(2011.01.01 01:00:00..2022.09.30 23:59:59,n))\n" +
                " cdecimal32 = decimal32(take(1..2022,n),2)\n" +
                " cdecimal64 = decimal64(take(2022..4044,n),2)\n" +
                " cdecimal128 = decimal128(take(4044..8088,n),2)\n" +
                " t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cfloat,cdouble,cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128)\n" +
                " if(existsDatabase(\"dfs://testPreload\")){\n" +
                "     dropDatabase(\"dfs://testPreload\")\n" +
                " }\n" +
                "dbDate = database(,VALUE,2011.08.16..2022.09.30);" +
                "dbInt = database(,RANGE,0 200 300 500 1000);" +
                " db = database(\"dfs://testPreload\",COMPO,[dbDate,dbInt])\n" +
                " pt = db.createPartitionedTable(t,`pt,`cdate`cint)\n" +
                " pt.append!(t)";
        DBConnection db = new DBConnection();
        db.connect(HOST,PORT,"admin","123456");
        db.run(script);
        db.close();
        conn = DriverManager.getConnection(url+"?databasePath=dfs://testPreload",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select TOP 100 * from pt");
        Assert.assertTrue(rs.next());
        int index = 0;
        do{
            System.out.println((index++)+":"+rs.getString(1)+" "+rs.getString(2));
        }while(rs.next());
        Assert.assertEquals(100,index);
    }

    @Test
    public void test_Preload_value_PartitionedTable() throws SQLException, IOException {
        String script = "n=10000;\n" +
                " cbool = take(true false false true,n);\n" +
                " cchar = take('a'..'z',n);\n" +
                " cshort = take(1h..200h,n);\n" +
                " cint = rand(1000,n);\n" +
                " clong = take(200l..2000l,n)\n" +
                " cdate = take(2011.08.16..2022.09.30,n)\n" +
                " cmonth = take(2012.01M..2022.10M,n)\n" +
                " ctime = take(00:00:00.001..23:59:59.999,n)\n" +
                " cminute = take(00:01m..23:59m,n)\n" +
                " csecond = take(00:00:01..23:59:59,n)\n" +
                " cdatetime = take(2011.01.01 00:00:01..2022.09.30 23:59:59,n)\n" +
                " ctimestamp = take(2022.09.30 00:00:00.001..2022.09.30 23:59:59.999,n)\n" +
                " cfloat = rand(300.0f,n)\n" +
                " cdouble = rand(230.0,n)\n" +
                " cstring = take(\"hello\" \"world\" \"dolphindb\",n)\n" +
                " cdatehour = datehour(take(2011.01.01 01:00:00..2022.09.30 23:59:59,n))\n" +
                " cdecimal32 = decimal32(take(1..2022,n),2)\n" +
                " cdecimal64 = decimal64(take(2022..4044,n),2)\n" +
                " cdecimal128 = decimal128(take(4044..8088,n),2)\n" +
                " t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cfloat,cdouble,cstring,cdatehour,cdecimal32,cdecimal64,cdecimal128)\n" +
                " if(existsDatabase(\"dfs://testPreload\")){\n" +
                "     dropDatabase(\"dfs://testPreload\")\n" +
                " }\n" +
                " db = database(\"dfs://testPreload\",VALUE,1..1000)\n" +
                " pt = db.createPartitionedTable(t,`pt,`cint)\n" +
                " pt.append!(t)";
        DBConnection db = new DBConnection();
        db.connect(HOST,PORT,"admin","123456");
        db.run(script);
        db.close();
        conn = DriverManager.getConnection(url+"?databasePath=dfs://testPreload",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select TOP 101 * from pt");
        Assert.assertTrue(rs.next());
        int index = 0;
        while(rs.next()){
            System.out.println((index++)+":"+rs.getString(1)+" "+rs.getString(2));
        }
        Assert.assertEquals(100,index);
    }
    @Test
    public void test_PreLoad_tableAlias_dfs_table_default() throws SQLException, IOException {
        conn = DriverManager.getConnection(url+"?tableAlias=dfs://valuedb/pt",info);
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
    @Test
    public void test_PreLoad_tableAlias_dfs_table_alias_1() throws SQLException, IOException {
        conn = DriverManager.getConnection(url+"?tableAlias=ttt:dfs://valuedb/pt",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select top 100 * from ttt");
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
    @Test
    public void test_PreLoad_tableAlias_dfs_table_alias_2() throws SQLException, IOException {
        conn = DriverManager.getConnection(url+"?tableAlias=t1212:dfs://valuedb/pt",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select top 100 * from t1212");
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
    @Test
    public void test_PreLoad_tableAlias_dfs_table_alias_3() throws SQLException, IOException {
        conn = DriverManager.getConnection(url+"?tableAlias=col_中文1212:dfs://valuedb/pt",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select top 100 * from col_中文1212");
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
    @Test
    public void test_PreLoad_tableAlias_dfs_table_alias_4() throws SQLException, IOException {
        conn = DriverManager.getConnection(url+"?tableAlias=中文中问帆帆帆帆dfs:dfs://valuedb/pt,col1:dfs://valuedb/pt,w__v_1aluedbpt:dfs://valuedb/pt,dfs:dfs://valuedb/pt,dfs://valuedb/pt",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select top 100 * from 中文中问帆帆帆帆dfs");
        ResultSet resultSet = stm.executeQuery("select top 100 * from loadTable(\"dfs://valuedb\",\"pt\")");
        Assert.assertTrue(resultSet.next());
        Assert.assertTrue(rs.next());
        System.out.println(rs);
        while(rs.next() && resultSet.next()){
            System.out.println(rs.getString(1)+" "+rs.getString(2));
            Assert.assertEquals(resultSet.getString(1),rs.getString(1));
            Assert.assertEquals(resultSet.getString(2),rs.getString(2));
        }
        ResultSet rs1 = stm.executeQuery("select top 100 * from col1");
        ResultSet resultSet1 = stm.executeQuery("select top 100 * from loadTable(\"dfs://valuedb\",\"pt\")");
        Assert.assertTrue(resultSet1.next());
        Assert.assertTrue(rs1.next());
        System.out.println(rs1);
        while(rs1.next() && resultSet1.next()){
            System.out.println(rs1.getString(1)+" "+rs1.getString(2));
            Assert.assertEquals(resultSet1.getString(1),rs1.getString(1));
            Assert.assertEquals(resultSet1.getString(2),rs1.getString(2));
        }
        ResultSet rs2 = stm.executeQuery("select top 100 * from w__v_1aluedbpt");
        ResultSet resultSet2 = stm.executeQuery("select top 100 * from loadTable(\"dfs://valuedb\",\"pt\")");
        Assert.assertTrue(resultSet2.next());
        Assert.assertTrue(rs2.next());
        System.out.println(rs2);
        while(rs2.next() && resultSet2.next()){
            System.out.println(rs2.getString(1)+" "+rs2.getString(2));
            Assert.assertEquals(resultSet2.getString(1),rs2.getString(1));
            Assert.assertEquals(resultSet2.getString(2),rs2.getString(2));
        }
        ResultSet rs3 = stm.executeQuery("select top 100 * from dfs");
        ResultSet resultSet3 = stm.executeQuery("select top 100 * from loadTable(\"dfs://valuedb\",\"pt\")");
        Assert.assertTrue(resultSet3.next());
        Assert.assertTrue(rs3.next());
        System.out.println(rs3);
        while(rs3.next() && resultSet3.next()){
            System.out.println(rs3.getString(1)+" "+rs3.getString(2));
            Assert.assertEquals(resultSet3.getString(1),rs3.getString(1));
            Assert.assertEquals(resultSet3.getString(2),rs3.getString(2));
        }
        ResultSet rs4 = stm.executeQuery("select top 100 * from pt");
        ResultSet resultSet4 = stm.executeQuery("select top 100 * from loadTable(\"dfs://valuedb\",\"pt\")");
        Assert.assertTrue(resultSet4.next());
        Assert.assertTrue(rs4.next());
        System.out.println(rs4);
        while(rs4.next() && resultSet4.next()){
            System.out.println(rs4.getString(1)+" "+rs4.getString(2));
            Assert.assertEquals(resultSet4.getString(1),rs4.getString(1));
            Assert.assertEquals(resultSet4.getString(2),rs4.getString(2));
        }
    }
    @Test
    public void test_PreLoad_tableAlias_dfs_table_alias_5() throws SQLException, IOException {
        info.put("tableAlias","中文中问帆帆帆帆dfs:dfs://valuedb/pt,col1:dfs://valuedb/pt,w__v_1aluedbpt:dfs://valuedb/pt,dfs:dfs://valuedb/pt,dfs://valuedb/pt");
        conn = DriverManager.getConnection(url,info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select top 100 * from 中文中问帆帆帆帆dfs");
        ResultSet resultSet = stm.executeQuery("select top 100 * from loadTable(\"dfs://valuedb\",\"pt\")");
        Assert.assertTrue(resultSet.next());
        Assert.assertTrue(rs.next());
        System.out.println(rs);
        while(rs.next() && resultSet.next()){
            System.out.println(rs.getString(1)+" "+rs.getString(2));
            Assert.assertEquals(resultSet.getString(1),rs.getString(1));
            Assert.assertEquals(resultSet.getString(2),rs.getString(2));
        }
        ResultSet rs1 = stm.executeQuery("select top 100 * from col1");
        ResultSet resultSet1 = stm.executeQuery("select top 100 * from loadTable(\"dfs://valuedb\",\"pt\")");
        Assert.assertTrue(resultSet1.next());
        Assert.assertTrue(rs1.next());
        System.out.println(rs1);
        while(rs1.next() && resultSet1.next()){
            System.out.println(rs1.getString(1)+" "+rs1.getString(2));
            Assert.assertEquals(resultSet1.getString(1),rs1.getString(1));
            Assert.assertEquals(resultSet1.getString(2),rs1.getString(2));
        }
        ResultSet rs2 = stm.executeQuery("select top 100 * from w__v_1aluedbpt");
        ResultSet resultSet2 = stm.executeQuery("select top 100 * from loadTable(\"dfs://valuedb\",\"pt\")");
        Assert.assertTrue(resultSet2.next());
        Assert.assertTrue(rs2.next());
        System.out.println(rs2);
        while(rs2.next() && resultSet2.next()){
            System.out.println(rs2.getString(1)+" "+rs2.getString(2));
            Assert.assertEquals(resultSet2.getString(1),rs2.getString(1));
            Assert.assertEquals(resultSet2.getString(2),rs2.getString(2));
        }
        ResultSet rs3 = stm.executeQuery("select top 100 * from dfs");
        ResultSet resultSet3 = stm.executeQuery("select top 100 * from loadTable(\"dfs://valuedb\",\"pt\")");
        Assert.assertTrue(resultSet3.next());
        Assert.assertTrue(rs3.next());
        System.out.println(rs3);
        while(rs3.next() && resultSet3.next()){
            System.out.println(rs3.getString(1)+" "+rs3.getString(2));
            Assert.assertEquals(resultSet3.getString(1),rs3.getString(1));
            Assert.assertEquals(resultSet3.getString(2),rs3.getString(2));
        }
        ResultSet rs4 = stm.executeQuery("select top 100 * from pt");
        ResultSet resultSet4 = stm.executeQuery("select top 100 * from loadTable(\"dfs://valuedb\",\"pt\")");
        Assert.assertTrue(resultSet4.next());
        Assert.assertTrue(rs4.next());
        System.out.println(rs4);
        while(rs4.next() && resultSet4.next()){
            System.out.println(rs4.getString(1)+" "+rs4.getString(2));
            Assert.assertEquals(resultSet4.getString(1),rs4.getString(1));
            Assert.assertEquals(resultSet4.getString(2),rs4.getString(2));
        }
    }

    @Test
    public void test_PreLoad_tableAlias_dfs_table_alias_6() throws SQLException, IOException {
        conn = DriverManager.getConnection(url+"?tableAlias=dfs://valuedb/pt,dfs://valuedb/dt,dfs://valuedb/Order_tmp,dfs://testValue/nt",info);
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
        ResultSet rs1 = stm.executeQuery("select top 100 * from dt");
        ResultSet resultSet1 = stm.executeQuery("select top 100 * from loadTable(\"dfs://valuedb\",\"dt\")");
        Assert.assertTrue(resultSet1.next());
        Assert.assertTrue(rs1.next());
        System.out.println(rs1);
        while(rs1.next() && resultSet1.next()){
            System.out.println(rs1.getString(1)+" "+rs1.getString(2));
            Assert.assertEquals(resultSet1.getString(1),rs1.getString(1));
            Assert.assertEquals(resultSet1.getString(2),rs1.getString(2));
        }
        ResultSet rs2 = stm.executeQuery("select top 100 * from Order_tmp");
        ResultSet resultSet2 = stm.executeQuery("select top 100 * from loadTable(\"dfs://valuedb\",\"Order_tmp\")");
        Assert.assertTrue(resultSet2.next());
        Assert.assertTrue(rs2.next());
        System.out.println(rs2);
        while(rs2.next() && resultSet2.next()){
            System.out.println(rs2.getString(1)+" "+rs2.getString(2));
            Assert.assertEquals(resultSet2.getString(1),rs2.getString(1));
            Assert.assertEquals(resultSet2.getString(2),rs2.getString(2));
        }
        ResultSet rs3 = stm.executeQuery("select top 100 * from nt");
        ResultSet resultSet3 = stm.executeQuery("select top 100 * from loadTable(\"dfs://testValue\",\"nt\")");
        Assert.assertTrue(resultSet3.next());
        Assert.assertTrue(rs3.next());
        System.out.println(rs3);
        while(rs3.next() && resultSet3.next()){
            System.out.println(rs3.getString(1)+" "+rs3.getString(2));
            Assert.assertEquals(resultSet3.getString(1),rs3.getString(1));
            Assert.assertEquals(resultSet3.getString(2),rs3.getString(2));
        }
    }
    @Test
    public void test_PreLoad_tableAlias_dfs_table_alias_7() throws SQLException, IOException {
        String e = null;
        try{
            conn = DriverManager.getConnection(url+"?tableAlias=dfs://valuedb/ppt",info);

        }catch(Exception ex){
            e = ex.getMessage().toString();
        }
        Assert.assertNotNull(e);
        Assert.assertTrue(e.contains("path does not exist"));
    }
    @Test
    public void test_PreLoad_tableAlias_dfs_table_alias_8() throws SQLException, IOException {
        String e = null;
        try{
            conn = DriverManager.getConnection(url+"?tableAlias=dfs://valuedb/pt,dfs://testValue/pt",info);
        }catch(Exception ex){
            e = ex.getMessage().toString();
        }
        Assert.assertNotNull(e);
        Assert.assertTrue(e.contains("Duplicate table alias found in property tableAlias"));

    }
    @Test
    public void test_PreLoad_tableAlias_dfs_table_alias_9() throws SQLException, IOException {
        DBConnection connection = new DBConnection();
        connection.connect(HOST,PORT,"admin","123456");
        String script = "try{dropDatabase(\"dfs://test_allDateType/eee/11\");}catch(EX){}\n" +
                "colNames=\"col\"+string(1..28);\n" +
                "colTypes=[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)]\n" +
                "t=table(1:0,colNames,colTypes);\n" +
                "insert into t values(true,'a',2h,2,22l,2012.12.06,2012.06M,12:30:00.008,12:30m,12:30:00,2012.06.12 12:30:00,2012.06.12 12:30:00.008,13:30:10.008007006,2012.06.13 13:30:10.008007006,2.1f,2.1,\"hello\",\"world\",uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\"),datehour(2012.06.13 13:30:10),ipaddr(\"192.168.1.253\"),int128(\"e1671797c52e15f763380b45e841ec32\"),blob(\"123\"),complex(111,1),point(1,2),decimal32(1.1,2),decimal64(1.1,7),decimal128(1.1,19)) ;\n" +
                "dbName = \"dfs://test_allDateType/eee/11\";\n" +
                "db = database(directory=dbName, partitionType=RANGE, partitionScheme=1 5 10, engine=\"TSDB\");\n" +
                "pt1 = db.createPartitionedTable(table=t, tableName=`pt1, partitionColumns=`col4, sortColumns=`col4);\n" +
                "pt1.append!(t)\n" ;
        connection.run(script);
        conn = DriverManager.getConnection(url+"?tableAlias=test1:dfs://test_allDateType/eee/11/pt1",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select  * from test1");
        JDBCResultSet resultSet = (JDBCResultSet)stm.executeQuery("select top 100 * from loadTable(\"dfs://test_allDateType/eee/11\",\"pt1\")");
        BasicTable result = (BasicTable)resultSet.getResult();
        Assert.assertEquals(28,result.columns());
        Assert.assertEquals(1,result.rows());
    }
    //@Test
    public void test_PreLoad_tableAlias_dfs_table_alias_10() throws SQLException, IOException {
        DBConnection connection = new DBConnection();
        connection.connect(HOST,PORT,"admin","123456");
        String script = "try{dropDatabase(\"dfs://test_allDateType1\");}catch(EX){}\n" +
                "colNames=\"col\"+string(1..28);\n" +
                "colTypes=[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)]\n" +
                "t=table(1:0,colNames,colTypes);\n" +
                "insert into t values(true,'a',2h,2,22l,2012.12.06,2012.06M,12:30:00.008,12:30m,12:30:00,2012.06.12 12:30:00,2012.06.12 12:30:00.008,13:30:10.008007006,2012.06.13 13:30:10.008007006,2.1f,2.1,\"hello\",\"world\",uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\"),datehour(2012.06.13 13:30:10),ipaddr(\"192.168.1.253\"),int128(\"e1671797c52e15f763380b45e841ec32\"),blob(\"123\"),complex(111,1),point(1,2),decimal32(1.1,2),decimal64(1.1,7),decimal128(1.1,19)) ;\n" +
                "dbName = \"dfs://test_allDateType1\";\n" +
                "db = database(directory=dbName, partitionType=RANGE, partitionScheme=1 5 10, engine=\"TSDB\");\n" +
                "dfs = db.createPartitionedTable(table=t, tableName=`dfs, partitionColumns=`col4, sortColumns=`col4);\n" +
                "dfs.append!(t)\n" ;
        connection.run(script);
        conn = DriverManager.getConnection(url+"?tableAlias=test1:dfs://test_allDateType/dfs",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select  * from test1");
        JDBCResultSet resultSet = (JDBCResultSet)stm.executeQuery("select top 100 * from loadTable(\"dfs://test_allDateType\",\"dfs\")");
        BasicTable result = (BasicTable)resultSet.getResult();
        Assert.assertEquals(28,result.columns());
        Assert.assertEquals(1,result.rows());
    }
    @Test
    public void test_PreLoad_tableAlias_dfs_table_alias_11() throws SQLException, IOException {
        DBConnection connection = new DBConnection();
        connection.connect(HOST,PORT,"admin","123456");
        String script = "try{dropDatabase(\"dfs://test_dfs\");}catch(EX){}\n" +
                "colNames=\"col\"+string(1..4);\n" +
                "colTypes=[BOOL,CHAR,SHORT,INT]\n" +
                "t=table(1:0,colNames,colTypes);\n" +
                "dbName = \"dfs://test_dfs\";\n" +
                "db = database(directory=dbName, partitionType=RANGE, partitionScheme=1 5 10, engine=\"TSDB\");\n" +
                "dfs = db.createPartitionedTable(table=t, tableName=`dfs, partitionColumns=`col4, sortColumns=`col4);\n";
        connection.run(script);
        conn = DriverManager.getConnection(url+"?tableAlias=test1:dfs://test_dfs/dfs",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select  * from test1");
        JDBCResultSet resultSet = (JDBCResultSet)stm.executeQuery("select top 100 * from loadTable(\"dfs://test_dfs\",\"dfs\")");
        BasicTable result = (BasicTable)resultSet.getResult();
        Assert.assertEquals(4,result.columns());
        Assert.assertEquals(0,result.rows());
    }
    @Test
    public void test_PreLoad_tableAlias_dfs_table_alias_12() throws SQLException, IOException {
        String e = null;
        try{
            conn = DriverManager.getConnection(url+"?tableAlias=",info);
        }catch(Exception ex){
            e = ex.getMessage().toString();
        }
        Assert.assertNotNull(e);
        Assert.assertTrue(e.contains("tableAlias=     is error"));
    }
    @Test
    public void test_PreLoad_tableAlias_dfs_table_alias_13() throws SQLException, IOException {
        String e = null;
        try{
            conn = DriverManager.getConnection(url+"?tableAlias=dfs://valuedb/pt,,dfs://testValue/nt",info);
        }catch(Exception ex){
            e = ex.getMessage().toString();
        }
        Assert.assertNotNull(e);
        Assert.assertTrue(e.contains("tableAlias's value cannot be null!"));
    }
    @Test
    public void test_PreLoad_tableAlias_dfs_table_alias_14() throws SQLException, IOException {
        DBConnection connection = new DBConnection();
        connection.connect(HOST,PORT,"admin","123456");
        String script = "try{dropDatabase(\"dfs://test_dfs.ee.123\");}catch(EX){}\n" +
                "colNames=\"col\"+string(1..4);\n" +
                "colTypes=[BOOL,CHAR,SHORT,INT]\n" +
                "t=table(1:0,colNames,colTypes);\n" +
                "insert into t values(true,'a',2h,2);\n" +
                "dbName = \"dfs://test_dfs.ee.123\";\n" +
                "db = database(directory=dbName, partitionType=RANGE, partitionScheme=1 5 10, engine=\"TSDB\");\n" +
                "dfs = db.createPartitionedTable(table=t, tableName=`dfs, partitionColumns=`col4, sortColumns=`col4);\n";
        connection.run(script);
        conn = DriverManager.getConnection(url+"?tableAlias=test1:dfs://test_dfs.ee.123/dfs",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select  * from test1");
        JDBCResultSet resultSet = (JDBCResultSet)stm.executeQuery("select top 100 * from loadTable(\"dfs://test_dfs.ee.123\",\"dfs\")");
        BasicTable result = (BasicTable)resultSet.getResult();
        Assert.assertEquals(4,result.columns());
        Assert.assertEquals(0,result.rows());
    }

    @Test
    public void test_PreLoad_tableAlias_table_alias_1() throws SQLException, IOException {
        conn = DriverManager.getConnection(url+"?tableAlias=dfs://valuedb/pt,ppt:pt,pppt:ppt",info);
        Statement stm = conn.createStatement();
        ResultSet rs = stm.executeQuery("select top 100 * from pt");
        ResultSet resultSet = stm.executeQuery("select top 100 * from loadTable(\"dfs://valuedb\",\"pt\")");
        ResultSet rs1 = stm.executeQuery("select top 100 * from ppt");
        ResultSet rs2 = stm.executeQuery("select top 100 * from pppt");
        Assert.assertTrue(resultSet.next());
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs1.next());
        Assert.assertTrue(rs2.next());
        System.out.println(rs);
        while(rs.next() && resultSet.next()){
            System.out.println(rs.getString(1)+" "+rs.getString(2));
            Assert.assertEquals(resultSet.getString(1),rs.getString(1));
            Assert.assertEquals(resultSet.getString(2),rs.getString(2));
            Assert.assertEquals(resultSet.getString(1),rs1.getString(1));
            Assert.assertEquals(resultSet.getString(1),rs1.getString(1));
            Assert.assertEquals(resultSet.getString(1),rs2.getString(1));
            Assert.assertEquals(resultSet.getString(1),rs2.getString(1));
        }
    }

    @Test
    public void test_PreLoad_tableAlias_memory_table_alias_1() throws SQLException, IOException {
        DBConnection connection = new DBConnection();
        connection.connect(HOST,PORT,"admin","123456");
        String script = "\n" +
                "n=500;\n" +
                "date=take(2006.01.01..2006.01.31, n);\n" +
                "x=rand(10.0, n);\n" +
                "id = rand(20.0,n);\n" +
                "share table(date) as t1;\n" +
                "share  table(date,id) as t2;\n" +
                "share table(date,id,x) as t3\n" ;
        connection.run(script);
        conn = DriverManager.getConnection(url+"?tableAlias=t1:t1,t2:t2,t3:t3",info);
        Statement stm = conn.createStatement();
        JDBCResultSet rs = (JDBCResultSet)stm.executeQuery("select  * from t1");
        JDBCResultSet rs1 = (JDBCResultSet)stm.executeQuery("select  * from t2");
        JDBCResultSet rs2 = (JDBCResultSet)stm.executeQuery("select  * from t3");
        BasicTable rss = (BasicTable) rs.getResult();
        BasicTable rss1 = (BasicTable) rs1.getResult();
        BasicTable rss2 = (BasicTable) rs2.getResult();
        Assert.assertEquals(500, rss.rows());
        Assert.assertEquals(500, rss1.rows());
        Assert.assertEquals(500, rss2.rows());
        Assert.assertEquals(1, rss.columns());
        Assert.assertEquals(2, rss1.columns());
        Assert.assertEquals(3, rss2.columns());
    }
    @Test
    public void test_PreLoad_tableAlias_memory_table_alias_2() throws SQLException, IOException {
        DBConnection connection = new DBConnection();
        connection.connect(HOST,PORT,"admin","123456");
        String script = "\n" +
                "n=500;\n" +
                "date=take(2006.01.01..2006.01.31, n);\n" +
                "x=rand(10.0, n);\n" +
                "id = rand(20.0,n);\n" +
                "share table(date) as t1;\n" +
                "share  table(date,id) as t2;\n" +
                "share table(date,id,x) as t3\n" ;
        connection.run(script);
        String e = null;
        try{
            conn = DriverManager.getConnection(url+"?tableAlias=t1:t1,t1:t2,t1:t3",info);
        }catch(Exception ex){
            e = ex.getMessage().toString();
        }
        Assert.assertNotNull(e);
        Assert.assertTrue(e.contains("Duplicate table alias found in property tableAlias"));

    }
    @Test
    public void test_PreLoad_tableAlias_memory_table_alias_3() throws SQLException, IOException {
        DBConnection connection = new DBConnection();
        connection.connect(HOST,PORT,"admin","123456");
        String script = "\n" +
                "n=500;\n" +
                "date=take(2006.01.01..2006.01.31, n);\n" +
                "x=rand(10.0, n);\n" +
                "id = rand(20.0,n);\n" +
                "share table(date) as t1;\n" +
                "share  table(date,id) as t2;\n" +
                "share table(date,id,x) as t3\n" ;
        connection.run(script);
        conn = DriverManager.getConnection(url+"?tableAlias=t1:t1,t2:t2,t3:t3,测试测试测试别名的呃呃呃:t1,w__dfd知道是的:t2,dfsdfsQAZWSXEDCRFVTGBYHNNNNNNNeeDFS___中国dfs:t3,mvcc123mvcc:t1,count:t2,COUNT:t3,",info);
        //conn = DriverManager.getConnection(url+"?tableAlias=count1:t2,COUNT:t3,",info);

        Statement stm = conn.createStatement();
        JDBCResultSet rs = (JDBCResultSet)stm.executeQuery("select  * from 测试测试测试别名的呃呃呃");
        JDBCResultSet rs1 = (JDBCResultSet)stm.executeQuery("select  * from w__dfd知道是的");
        JDBCResultSet rs2 = (JDBCResultSet)stm.executeQuery("select  * from dfsdfsQAZWSXEDCRFVTGBYHNNNNNNNeeDFS___中国dfs");
        JDBCResultSet rs3 = (JDBCResultSet)stm.executeQuery("select  * from mvcc123mvcc");
        JDBCResultSet rs4 = (JDBCResultSet)stm.executeQuery("select  * from count");
        JDBCResultSet rs5 = (JDBCResultSet)stm.executeQuery("select  * from COUNT");
        BasicTable rss = (BasicTable) rs.getResult();
        BasicTable rss1 = (BasicTable) rs1.getResult();
        BasicTable rss2 = (BasicTable) rs2.getResult();
        BasicTable rss3 = (BasicTable) rs3.getResult();
        BasicTable rss4 = (BasicTable) rs4.getResult();
        BasicTable rss5 = (BasicTable) rs5.getResult();
        Assert.assertEquals(500, rss.rows());
        Assert.assertEquals(500, rss1.rows());
        Assert.assertEquals(500, rss2.rows());
        Assert.assertEquals(1, rss.columns());
        Assert.assertEquals(2, rss1.columns());
        Assert.assertEquals(3, rss2.columns());
        Assert.assertEquals(500, rss3.rows());
        Assert.assertEquals(500, rss4.rows());
        Assert.assertEquals(500, rss5.rows());
        Assert.assertEquals(1, rss3.columns());
        Assert.assertEquals(2, rss4.columns());
        Assert.assertEquals(3, rss5.columns());
    }
    @Test
    public void test_PreLoad_tableAlias_memory_table_alias_4() throws SQLException, IOException {
        DBConnection connection = new DBConnection();
        connection.connect(HOST,PORT,"admin","123456");
        connection.run("share table (1..10 as id) as dfs");
        String e = null;
        try{
            conn = DriverManager.getConnection(url+"?tableAlias=dfs",info);
        }catch(Exception ex){
            e = ex.getMessage().toString();
        }
        Assert.assertNotNull(e);
        Assert.assertTrue(e.contains("Failed to parse tableAlias"));
    }
    @Test
    public void test_PreLoad_tableAlias_mvcc_table_alias_1() throws SQLException, IOException {
        DBConnection connection = new DBConnection();
        connection.connect(HOST,PORT,"admin","123456");
        String script = "\n" +
                "n=500;\n" +
                "date=take(13:30:10.008..13:30:10.108, n);\n" +
                "x=rand(10.0, n);\n" +
                "id = rand(20,n);\n" +
                "t2=table(date,id);\n" +
                "t3=table(date,id,x);\n" +
                "share mvccTable(date) as mvcc1;\n" +
                "mvcc12=mvccTable(500:0,`date`id,[TIME,INT],\"/home/wsun/Adolphindb/2.00.6/server/work_dir\",\"mvcc12\");\n" +
                "mvcc12.append!(t2);\n" +
                "mvcc13=mvccTable(500:0,`date`id`x,[TIME,INT,DOUBLE],\"work_dir\",`mvcc13);\n" +
                "mvcc13.append!(t3);\n" ;
        //connection.run(script);
        conn = DriverManager.getConnection(url+"?tableAlias=mvcc2:mvcc:///home/wsun/Adolphindb/2.00.6/server/work_dir/mvcc12,mvcc1:mvcc1,mvcc3:mvcc://work_dir/mvcc13",info);
        Statement stm = conn.createStatement();
        JDBCResultSet rs = (JDBCResultSet)stm.executeQuery("select  * from mvcc1");
        JDBCResultSet rs1 = (JDBCResultSet)stm.executeQuery("select  * from mvcc2");
        JDBCResultSet rs2 = (JDBCResultSet)stm.executeQuery("select  * from mvcc3");
        BasicTable rss = (BasicTable) rs.getResult();
        BasicTable rss1 = (BasicTable) rs1.getResult();
        BasicTable rss2 = (BasicTable) rs2.getResult();
        Assert.assertEquals(500, rss.rows());
        Assert.assertEquals(500, rss1.rows());
        Assert.assertEquals(500, rss2.rows());
        Assert.assertEquals(1, rss.columns());
        Assert.assertEquals(2, rss1.columns());
        Assert.assertEquals(3, rss2.columns());
    }
    @Test//linux
    public void test_PreLoad_tableAlias_mvcc_table_alias_2() throws SQLException, IOException {
        DBConnection connection = new DBConnection();
        connection.connect(HOST,PORT,"admin","123456");
        String script = "\n" +
                "n=500;\n" +
                "date=take(13:30:10.008..13:30:10.108, n);\n" +
                "x=rand(10.0, n);\n" +
                "x1=rand(10.0, n);\n" +
                "id = rand(20,n);\n" +
                "t4=table(date,id,x,x1);\n" +
                "mvcc14=mvccTable(500:0,`date`id`x`x1,[TIME,INT,DOUBLE,DOUBLE],\"C://DolphinDB/Data1/db12/\",\"mvcc14\");\n" +
                "mvcc14.append!(t4);\n" ;
       // connection.run(script);
        conn = DriverManager.getConnection(url+"?tableAlias=mvcc14:mvcc://C://DolphinDB/Data1/db12/mvcc14",info);
        Statement stm = conn.createStatement();
        JDBCResultSet rs = (JDBCResultSet)stm.executeQuery("select  * from mvcc14");
        BasicTable rss = (BasicTable) rs.getResult();
        Assert.assertEquals(500, rss.rows());
        Assert.assertEquals(4, rss.columns());
    }

    @Test//win
    public void test_PreLoad_tableAlias_mvcc_table_alias_3() throws SQLException, IOException {
        String url1 = "jdbc:dolphindb://192.168.0.9:8849";
        DBConnection connection = new DBConnection();
        connection.connect("192.168.0.9",8849,"admin","123456");
        String script = "\n" +
                "n=500;\n" +
                "date=take(13:30:10.008..13:30:10.108, n);\n" +
                "x=rand(10.0, n);\n" +
                "id = rand(20,n);\n" +
                "t2=table(date,id);\n" +
                "t3=table(date,id,x);\n" +
                "share mvccTable(date) as mvcc1;\n" +
                "mvcc12=mvccTable(500:0,`date`id,[TIME,INT],\"/home/wsun/Adolphindb/2.00.6/server/work_dir\",\"mvcc12\");\n" +
                "mvcc12.append!(t2);\n" +
                "mvcc13=mvccTable(500:0,`date`id`x,[TIME,INT,DOUBLE],\"work_dir\",`mvcc13);\n" +
                "mvcc13.append!(t3);\n" ;
        //connection.run(script);
        conn = DriverManager.getConnection(url1+"?tableAlias=mvcc2:mvcc:///home/wsun/Adolphindb/2.00.6/server/work_dir/mvcc12,mvcc1:mvcc1,mvcc3:mvcc://work_dir/mvcc13",info);
        Statement stm = conn.createStatement();
        JDBCResultSet rs = (JDBCResultSet)stm.executeQuery("select  * from mvcc1");
        JDBCResultSet rs1 = (JDBCResultSet)stm.executeQuery("select  * from mvcc2");
        JDBCResultSet rs2 = (JDBCResultSet)stm.executeQuery("select  * from mvcc3");
        BasicTable rss = (BasicTable) rs.getResult();
        BasicTable rss1 = (BasicTable) rs1.getResult();
        BasicTable rss2 = (BasicTable) rs2.getResult();
        Assert.assertEquals(500, rss.rows());
        Assert.assertEquals(500, rss1.rows());
        Assert.assertEquals(500, rss2.rows());
        Assert.assertEquals(1, rss.columns());
        Assert.assertEquals(2, rss1.columns());
        Assert.assertEquals(3, rss2.columns());
    }
    @Test//win
    public void test_PreLoad_tableAlias_mvcc_table_alias_4() throws SQLException, IOException {
        String url1 = "jdbc:dolphindb://192.168.0.9:8849";
        DBConnection connection = new DBConnection();
        connection.connect("192.168.0.9",8849,"admin","123456");
        String script = "\n" +
                "n=500;\n" +
                "date=take(13:30:10.008..13:30:10.108, n);\n" +
                "x=rand(10.0, n);\n" +
                "x1=rand(10.0, n);\n" +
                "id = rand(20,n);\n" +
                "t4=table(date,id,x,x1);\n" +
                "mvcc14=mvccTable(500:0,`date`id`x`x1,[TIME,INT,DOUBLE,DOUBLE],\"C://DolphinDB/Data1/db12/\",\"mvcc14\");\n" +
                "mvcc14.append!(t4);\n" ;
        // connection.run(script);
        conn = DriverManager.getConnection(url1+"?tableAlias=mvcc14:mvcc://C://DolphinDB/Data1/db12/mvcc14",info);
        Statement stm = conn.createStatement();
        JDBCResultSet rs = (JDBCResultSet)stm.executeQuery("select  * from mvcc14");
        BasicTable rss = (BasicTable) rs.getResult();
        Assert.assertEquals(500, rss.rows());
        Assert.assertEquals(4, rss.columns());
    }
    @Test//win
    public void test_PreLoad_tableAlias_mvcc_table_alias_5() throws SQLException, IOException {
        String url1 = "jdbc:dolphindb://192.168.0.9:8849";
        DBConnection connection = new DBConnection();
        connection.connect("192.168.0.9",8849,"admin","123456");
        String script = "\n" +
                "n=500;\n" +
                "date=take(13:30:10.008..13:30:10.108, n);\n" +
                "x=rand(10.0, n);\n" +
                "x1=rand(10.0, n);\n" +
                "x2=rand(10.0, n);\n" +
                "id = rand(20,n);\n" +
                "t5=table(date,id,x,x1);\n" +
                "mvcc15=mvccTable(500:0,`date`id`x`x1`x2,[TIME,INT,DOUBLE,DOUBLE,DOUBLE],\"C:\\\\DolphinDB\\\\Data1\\\\db12\",\"mvcc15\");\n" +
                "mvcc15.append!(t5);\n" ;
        // connection.run(script);
        conn = DriverManager.getConnection(url1+"?tableAlias=mvcc14:mvcc://C:\\\\DolphinDB\\\\Data1\\\\db12\\\\mvcc15",info);
        Statement stm = conn.createStatement();
        JDBCResultSet rs = (JDBCResultSet)stm.executeQuery("select  * from mvcc15");
        BasicTable rss = (BasicTable) rs.getResult();
        Assert.assertEquals(500, rss.rows());
        Assert.assertEquals(5, rss.columns());
    }
}
