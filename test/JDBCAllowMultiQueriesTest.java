
import com.xxdb.DBConnection;
import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Scalar;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;

public class JDBCAllowMultiQueriesTest {
    static String HOST = JDBCTestUtil.HOST;
    static int PORT = JDBCTestUtil.PORT;
    static String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?allowMultiQueries=true";
    static String dataBase = "dfs://db1";
    static String TableName = "trades";
    Connection conn;
    @Before
    public void createDataBaseTable() throws IOException {
        DBConnection connection = new DBConnection();
        connection.connect(HOST,PORT,"admin","123456");
        String script = "n=1000000\n" +
                "date=rand(2018.08.01..2018.08.03,n)\n" +
                "sym=rand(`AAPL`MS`C`YHOO,n)\n" +
                "qty=rand(1..1000,n)\n" +
                "price=rand(100.0,n)\n" +
                "t=table(date,sym,qty,price)\n" +
                "if(existsDatabase(\"dfs://db1\")){\n" +
                "\tdropDatabase(\"dfs://db1\")\n" +
                "}\n" +
                "db=database(\"dfs://db1\",VALUE,2018.08.01..2018.08.03)\n" +
                "trades=db.createPartitionedTable(t,`trades,`date).append!(t)";
        connection.run(script);


    }
    @Before
    public void setup() throws ClassNotFoundException, SQLException {
        Class.forName("com.dolphindb.jdbc.Driver");
        conn = DriverManager.getConnection(url,"admin","123456");
    }

    @Test
    public void test_MultiQueries_select_statement() throws SQLException, IOException {
//        CallableStatement cstm = conn.prepareCall("select qty,date from loadTable(\"dfs://db1\",\"trades\") where sym=`AAPL and price=13.403838942758739;\n" +
//                "select date from loadTable(\"dfs://db1\",\"trades\") where sym=`MS and price=78.17140694241971;");
//        Assert.assertTrue(cstm.execute());
        Statement stm = conn.createStatement();
        ResultSet resultSet = stm.executeQuery("select date from loadTable(\"dfs://db1\",\"trades\") where sym=`AAPL and price=13.403838942758739;\n" +
                "select date from loadTable(\"dfs://db1\",\"trades\") where sym=`MS and price=78.17140694241971;");
        Assert.assertFalse(stm.getMoreResults());
    }

    @Test
    public void test_MultiQueries_select_callableStatement() throws SQLException {
        CallableStatement cstm = conn.prepareCall("select qty,date from loadTable(\"dfs://db1\",\"trades\") where sym=`AAPL and price=13.403838942758739;\n" +
                "select date from loadTable(\"dfs://db1\",\"trades\") where sym=`MS and price=78.17140694241971;");
        Assert.assertTrue(cstm.execute());

        while(cstm.getMoreResults()) {
            ResultSet rs = cstm.getResultSet();
            System.out.println(rs.getMetaData().getColumnCount());
            while (rs.next()) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    System.out.println(rs.getString(i));
                }
            }
        }
    }

    @Test
    public void test_MultiQueries_insert() throws SQLException, IOException {
        CallableStatement cstm = conn.prepareCall("pt1=loadTable(\"dfs://db1\",\"trades\");\n" +
                "tableInsert(pt1,table(2018.09.19 as date,`HW as sym,432 as qty,62.662662662 as price));\n" +
                "tableInsert(pt1,table(2019.08.18 as date,`XM as sym,251 as qty,26.226226226 as price));");
        System.out.println(cstm.execute());
        DBConnection conndb = new DBConnection();
        conndb.connect(HOST,PORT,"admin","123456");
        Assert.assertNotNull(conndb.run("select * from loadTable(\""+dataBase+"\",\""+TableName+"\") where price=62.662662662"));
        Assert.assertNotNull(conndb.run("select * from loadTable(\""+dataBase+"\",\""+TableName+"\") where price=26.226226226"));
    }

    @Test
    public void test_MultiQueries_update() throws SQLException,IOException{
        DBConnection conndb = new DBConnection();
        conndb.connect(HOST,PORT,"admin","123456");
        conndb.run("tableInsert(loadTable(\"dfs://db1\",\"trades\"),table(2019.08.18 as date,`XM as sym,251 as qty,26.226226226 as price));");
        conndb.run("tableInsert(loadTable(\"dfs://db1\",\"trades\"),table(2019.08.20 as date,`OPP as sym,257 as qty,13.403838942758739 as price));");
        BasicTable bi = (BasicTable) conndb.run("select qty from loadTable(\""+dataBase+"\",\""+TableName+"\") where price=26.226226226");

        int qty = Integer.parseInt(bi.getColumn(0).get(0).getString());
        CallableStatement cstm = conn.prepareCall("pt=loadTable(\"dfs://db1\",\"trades\");\n" +
                "update pt set qty=qty+1;\n" +
                "update pt set sym=`OP where price=13.403838942758739;");
        System.out.println(cstm.execute());
        Assert.assertTrue(conndb.run("select sym from loadTable(\""+dataBase+"\",\""+TableName+"\") where price=13.403838942758739").getString().contains("OP"));
        BasicTable bt = (BasicTable) conndb.run("select qty from loadTable(\""+dataBase+"\",\""+TableName+"\") where price=26.226226226");
        Assert.assertEquals(qty+1,Integer.parseInt(bt.getColumn(0).get(0).getString()));
    }

    @Test
    public void test_MultiQueries_delete() throws IOException, SQLException {
        DBConnection conndb = new DBConnection();
        conndb.connect(HOST,PORT,"admin","123456");
        CallableStatement cstm = conn.prepareCall("pt=loadTable(\"dfs://db1\",\"trades\");\n" +
                "delete from pt where price=26.226226226;\n" +
                "delete from pt where date=2018.08.18");
        cstm.execute();
        BasicTable bt = (BasicTable) conndb.run("select * from loadTable(\""+dataBase+"\",\""+TableName+"\") where price=26.226226226");
        BasicTable bt2 = (BasicTable) conndb.run("select * from loadTable(\""+dataBase+"\",\""+TableName+"\") where date=2018.08.18");
        Assert.assertEquals("[]",bt.getColumn(1).getString());
        Assert.assertEquals("[]",bt2.getColumn(2).getString());
    }

    @Test
    public void test_MultiQueries_Mixed() throws IOException, SQLException {
        DBConnection conndb = new DBConnection();
        conndb.connect(HOST,PORT,"admin","123456");
        BasicTable bi = (BasicTable) conndb.run("select qty from loadTable(\""+dataBase+"\",\""+TableName+"\")");
        int qty = Integer.parseInt(bi.getColumn(0).get(0).getString());
        CallableStatement cstm = conn.prepareCall("pt=loadTable(\"dfs://db1\",\"trades\");\n" +
                "tableInsert(pt,table(2022.08.04 as date,`VO as sym,266 as qty,17.117711117 as price));\n" +
                "update pt set qty=qty-1;\n" +
                "delete from pt where qty=25;\n" +
                "select date,sym,qty from pt where price=17.117711117;");
        System.out.println(cstm.execute());
        Assert.assertNotNull(conndb.run("select * from loadTable(\""+dataBase+"\",\""+TableName+"\") where price = 17.117711117"));
        BasicTable bt = (BasicTable) conndb.run("select * from loadTable(\""+dataBase+"\",\""+TableName+"\") where qty=25");
        Assert.assertEquals("[]",bt.getColumn(1).getString());
        BasicTable bt1 = (BasicTable) conndb.run("select qty from loadTable(\""+dataBase+"\",\""+TableName+"\")");
        Assert.assertEquals(qty-1,Integer.parseInt(bt1.getColumn(0).get(0).getString()));
        while(cstm.getMoreResults()) {
            ResultSet rs = cstm.getResultSet();
            System.out.println(rs.getMetaData().getColumnCount());
            while (rs.next()) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    System.out.println(rs.getString(i));
                }
            }
        }
    }

    @Test
    public void test_allowMultiQueries_false_statement() throws ClassNotFoundException, SQLException {
        Connection connNew;
        Class.forName("com.dolphindb.jdbc.Driver");
        connNew = DriverManager.getConnection("jdbc:dolphindb://"+HOST+":"+PORT,"admin","123456");
        Statement stm = connNew.createStatement();
        stm.executeQuery("select * from loadTable(\""+dataBase+"\",\""+TableName+"\") where price = 17.117711117;\n" +
                "select * from loadTable(\""+dataBase+"\",\""+TableName+"\") where price = 17.117711117");
        Assert.assertFalse(stm.getMoreResults());
    }

    @Test(expected=SQLException.class)
    public void test_allowMultiQueries_false_CallableStatement() throws ClassNotFoundException, SQLException {
        Connection connNew;
        Class.forName("com.dolphindb.jdbc.Driver");
        connNew = DriverManager.getConnection("jdbc:dolphindb://"+HOST+":"+PORT,"admin","123456");
        CallableStatement cstm = connNew.prepareCall("select * from loadTable(\""+dataBase+"\",\""+TableName+"\") where price = 17.117711117;\n" +
                "select * from loadTable(\""+dataBase+"\",\""+TableName+"\") where price = 17.117711117;");
        cstm.execute();
    }
}
