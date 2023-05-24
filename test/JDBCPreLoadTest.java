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
                    "t3 = table(date,de)\n" +
                    "if(existsDatabase(\"dfs://testValue\")){\n" +
                    "    dropDatabase(\"dfs://testValue\")\n" +
                    "}\n" +
                    "db2 = database(\"dfs://testValue\",VALUE,2006.01.01..2006.01.31)\n" +
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
            String script2 = "n = 50080000\n" +
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
    @Test
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

    @Test(expected = RuntimeException.class)
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
                " t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cfloat,cdouble,cstring,cdatehour)\n" +
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
                " t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cfloat,cdouble,cstring,cdatehour)\n" +
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
                " t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cfloat,cdouble,cstring,cdatehour)\n" +
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
                " t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cfloat,cdouble,cstring,cdatehour)\n" +
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
                " t = table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cfloat,cdouble,cstring,cdatehour)\n" +
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
}
