import com.dolphindb.jdbc.JDBCResultSet;
import com.xxdb.DBConnection;
import com.xxdb.comm.SqlStdEnum;
import com.xxdb.data.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.Properties;
import java.util.UUID;

import static java.sql.Statement.SUCCESS_NO_INFO;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class JDBCPrepareStatementMysqlTest {
    static String HOST = JDBCTestUtil.HOST;
    static int PORT = JDBCTestUtil.PORT;
    Properties LOGININFO = new Properties();
    String JDBC_DRIVER;
    String DB_URL ;
    Statement stm ;
    Connection conn;
    @Before
    public void SetUp(){
        JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        LOGININFO = new Properties();
        LOGININFO.put("user", "admin");
        LOGININFO.put("password", "123456");
        DB_URL = "jdbc:dolphindb://"+HOST+":"+PORT;
        JDBCTestUtil.LOGININFO.put("user", "admin");
        JDBCTestUtil.LOGININFO.put("password", "123456");
        JDBCTestUtil.LOGININFO.put("sqlStd", String.valueOf(SqlStdEnum.MySQL));
        conn = JDBCTestUtil.getConnection(JDBCTestUtil.LOGININFO);
        try {
            stm = conn.createStatement();
        }catch (SQLException ex){

        }
    }

    @Test
    public void test_PreparedStatement_insert_one_column_backtickQuoted() throws SQLException {
        stm.execute("`inser_tradeseq_info` = table(array(INT, 0) as TRADESEQ_ID)");

        PreparedStatement ps = conn.prepareStatement("insert into `inser_tradeseq_info` (`TRADESEQ_ID`) values (?)");
        ps.setInt(1, 1001);
        int rows = ps.executeUpdate();
        assertTrue(rows == SUCCESS_NO_INFO || rows == 1);

        JDBCResultSet rs = (JDBCResultSet) stm.executeQuery("select `TRADESEQ_ID` from `inser_tradeseq_info` where `TRADESEQ_ID` = 1001");
        BasicTable result = (BasicTable) rs.getResult();
        assertEquals(1, result.rows());
        assertEquals("TRADESEQ_ID", result.getColumnName(0));
    }

    @Test
    public void test_PreparedStatement_insert_colName_chinese_backtickQuoted() throws SQLException {
        stm.execute("inser_tradeseq_info = table(array(INT, 0) as `TRADESEQ_ID`,array(STRING, 0) as `COL2中文_1`)");
        JDBCResultSet rs11 = (JDBCResultSet) stm.executeQuery("select * from `inser_tradeseq_info`");
        BasicTable result11 = (BasicTable) rs11.getResult();
        System.out.println(result11.getString());

        PreparedStatement ps = conn.prepareStatement("insert into `inser_tradeseq_info` (`TRADESEQ_ID`,`COL2中文_1`) values (?,?)");
        ps.setInt(1, 1001);
        ps.setString(2, "wewe");
        int rows = ps.executeUpdate();
        assertTrue(rows == SUCCESS_NO_INFO || rows == 1);

        PreparedStatement ps1 = conn.prepareStatement("insert into `inser_tradeseq_info`  values (?,?)");
        ps1.setInt(1, 1001);
        ps1.setString(2, "wewe");
        ps1.addBatch();
        ps1.executeBatch();

        JDBCResultSet rs = (JDBCResultSet) stm.executeQuery("select `TRADESEQ_ID`,`inser_tradeseq_info`.`COL2中文_1` from `inser_tradeseq_info` where `TRADESEQ_ID` = 1001 AND  `COL2中文_1` = 'wewe'\n");
        BasicTable result = (BasicTable) rs.getResult();
        assertEquals(2, result.rows());
        System.out.println(result.getString());
        assertEquals("[wewe,wewe]", result.getColumn("COL2中文_1").getString());
    }

    @Test
    public void test_PreparedStatement_insert_colName_number_backtickQuoted() throws SQLException {
        stm.execute("insert_into = table(array(INT, 0) as `COL`,array(STRING, 0) as `colcol`,array(STRING, 0) as `1234`,array(STRING, 0) as `COL_col_1234`)");
        JDBCResultSet rs11 = (JDBCResultSet) stm.executeQuery("select * from `insert_into`");
        BasicTable result11 = (BasicTable) rs11.getResult();
        System.out.println(result11.getString());

        PreparedStatement ps = conn.prepareStatement("insert into `insert_into` (`COL`,`colcol`,`1234`,`COL_col_1234`) values (?,?,?,?)");
        ps.setInt(1, 1);
        ps.setString(2, "w");
        ps.setString(3, "1234");
        ps.setString(4, "COL_col_1234");
        int rows = ps.executeUpdate();
        assertTrue(rows == SUCCESS_NO_INFO || rows == 1);

        PreparedStatement ps1 = conn.prepareStatement("insert into `insert_into`   values (?,?,?,?)");
        ps1.setInt(1, 2);
        ps1.setString(2, "w");
        ps1.setString(3, "1234");
        ps1.setString(4, "COL_col_1234");
        ps1.addBatch();
        ps1.executeBatch();

        PreparedStatement ps2 = conn.prepareStatement("insert into insert_into(COL,colcol,`1234`,COL_col_1234)    values (?,?,?,?)");
        ps2.setInt(1, 3);
        ps2.setString(2, "w");
        ps2.setString(3, "1234");
        ps2.setString(4, "COL_col_1234");
        ps2.execute();
        JDBCResultSet rs = (JDBCResultSet) stm.executeQuery("select `COL`,`colcol`,`1234`,`COL_col_1234` from `insert_into` where COL in [1,2,3] and `colcol`=`w and `1234`='1234' and COL_col_1234=\"COL_col_1234\"\n");
        BasicTable result = (BasicTable) rs.getResult();
        assertEquals(3, result.rows());
        System.out.println(result.getString());
    }

    @Test
    public void test_PreparedStatement_update_backtickQuoted() throws SQLException {
        stm.execute("update_into = table(array(INT, 0) as `COL`,array(STRING, 0) as `colcol`,array(STRING, 0) as `1234`,array(STRING, 0) as `COL_col_1234`)");
        stm.execute("insert into `update_into` (`COL`,`colcol`,`1234`,`COL_col_1234`) values (1,'w','w','d')");
        stm.execute("insert into update_into values (2,'w','w','d');");
        stm.execute("insert into update_into(COL,colcol,`1234`,COL_col_1234)   values (3,'w','w','d')");

        PreparedStatement ps = conn.prepareStatement("update `update_into` set `COL`=?,`colcol`=?,`1234`=?,`COL_col_1234`=? where COL=?");
        ps.setInt(1, 11);
        ps.setString(2, "2w");
        ps.setString(3, "11");
        ps.setNull(4, Types.VARCHAR);
        ps.setInt(5, 1);
        int rows = ps.executeUpdate();
        assertTrue(rows == SUCCESS_NO_INFO || rows == 1);

        PreparedStatement ps1 = conn.prepareStatement("update update_into set `COL`=?,colcol=?,`1234`=?,`COL_col_1234`=? where `COL`=?");
        ps1.setInt(1, 22);
        ps1.setString(2, "w22");
        ps1.setString(3, "1234");
        ps1.setString(4, "COL_col_1234");
        ps1.setInt(5, 2);
        ps1.addBatch();
        ps1.setInt(1, 33);
        ps1.setString(2, "w");
        ps1.setString(3, "123422");
        ps1.setString(4, "COL_col_1234");
        ps1.setInt(5, 3);
        ps1.addBatch();
        ps1.executeBatch();
//
        PreparedStatement ps2 = conn.prepareStatement("update update_into set `COL`=?,colcol=?,`1234`=?,`COL_col_1234`=? where `COL`=?");
        ps2.setInt(1, 2);
        ps2.setString(2, "w11");
        ps2.setString(3, "123411");
        ps2.setString(4, "COL_col_123224");
        ps2.setInt(5, 33);
        ps2.execute();
        JDBCResultSet rs = (JDBCResultSet) stm.executeQuery("select `COL`,`colcol`,`1234`,`COL_col_1234` from `update_into` ");
        BasicTable result = (BasicTable) rs.getResult();
        assertEquals(3, result.rows());
        assertEquals("[11,22,2]", result.getColumn(0).getString());
        assertEquals("[2w,w22,w11]", result.getColumn(1).getString());
        assertEquals("[11,1234,123411]", result.getColumn(2).getString());
        assertEquals("[,COL_col_1234,COL_col_123224]", result.getColumn(3).getString());
    }

    @Test
    public void test_PreparedStatement_delete_backtickQuoted() throws SQLException {
        stm.execute("update_into = table(array(INT, 0) as `COL`,array(STRING, 0) as `colcol`,array(STRING, 0) as `1234`,array(STRING, 0) as `COL_col_1234`)");
        stm.execute("insert into `update_into` (`COL`,`colcol`,`1234`,`COL_col_1234`) values (1,'w','w','d')");
        stm.execute("insert into update_into values (2,'w','w','d');");
        stm.execute("insert into update_into(COL,colcol,`1234`,COL_col_1234)   values (3,'w','w','d')");
        stm.execute("insert into update_into(COL,colcol,`1234`,COL_col_1234)   values (4,'w','w','d')");

        PreparedStatement ps = conn.prepareStatement("delete from `update_into` where `update_into`.`COL`=?,colcol=?,`1234`=?,`COL_col_1234`=?");
        ps.setInt(1, 1);
        ps.setString(2, "w");
        ps.setString(3, "w");
        ps.setString(4,"d");
        int rows = ps.executeUpdate();
        assertTrue(rows == SUCCESS_NO_INFO || rows == 1);

        PreparedStatement ps1 = conn.prepareStatement("delete from update_into where `COL`=?,colcol=?,`1234`=?,`COL_col_1234`=?");
        ps1.setInt(1, 2);
        ps1.setString(2, "w");
        ps1.setString(3, "w");
        ps1.setString(4, "d");
        ps1.addBatch();
        ps1.setInt(1, 3);
        ps1.setString(2, "w");
        ps1.setString(3, "w");
        ps1.setString(4, "d");
        ps1.addBatch();
        ps1.executeBatch();

        PreparedStatement ps2 = conn.prepareStatement("DELETE from update_into where `COL`=?,colcol=?,`1234`=?,`COL_col_1234`=? ");
        ps2.setInt(1, 4);
        ps2.setString(2, "w");
        ps2.setString(3, "w");
        ps2.setString(4, "d");
        ps2.execute();
        JDBCResultSet rs = (JDBCResultSet) stm.executeQuery("select `update_into`.`COL`,`update_into`.`colcol`,`update_into`.`1234`,`update_into`.`COL_col_1234` from `update_into` ");
        BasicTable result = (BasicTable) rs.getResult();
        assertEquals(0, result.rows());
    }

    @Test
    public void test_PreparedStatement_insert_dfs_backtickQuoted() throws SQLException, IOException {
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_tsdb1'))" +
                "{ dropDatabase('dfs://test_tsdb1')} \n"+
                " update_into = table(array(INT, 0) as `COL`,array(STRING, 0) as `colcol`,array(STRING, 0) as `1234`,array(STRING, 0) as `COL_col_1234`);\n" +
                " insert into `update_into` (`COL`,`colcol`,`1234`,`COL_col_1234`) values (1,`w,`w,`d);\n" +
                "insert into update_into values (1,`w,`w,`d);\n" +
                "insert into update_into(COL,colcol,`1234`,COL_col_1234)   values (1,`w,`w,`d);\n" +
                "db=database('dfs://test_tsdb1', RANGE, 1 2001 4001 6001 8001 10001 10000000,,'TSDB') \n"+
                "db.createPartitionedTable(update_into, `update_into, `COL,,`COL)\n";
        DBConnection db = new DBConnection(SqlStdEnum.MySQL);
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);
        JDBCTestUtil.LOGININFO.put("sqlStd", String.valueOf(SqlStdEnum.MySQL));
        Connection conn = JDBCTestUtil.getConnection(JDBCTestUtil.LOGININFO);
        Statement stm = conn.createStatement();
        stm.execute("pt=loadTable('dfs://test_tsdb1',`update_into)");
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_tsdb1',`update_into)(`COL`,`colcol`,`1234`,`COL_col_1234`) values (?,?,?,?)");
        ps.setInt(1, 1);
        ps.setString(2, "w");
        ps.setString(3, "1234");
        ps.setString(4, "COL_col_1234");
        int rows = ps.executeUpdate();
        assertTrue(rows == SUCCESS_NO_INFO || rows == 1);

        PreparedStatement ps1 = conn.prepareStatement("insert into loadTable('dfs://test_tsdb1',`update_into)  values (?,?,?,?)");
        ps1.setInt(1, 2);
        ps1.setString(2, "w");
        ps1.setString(3, "1234");
        ps1.setString(4, "COL_col_1234");
        ps1.addBatch();
        ps1.executeBatch();

        PreparedStatement ps2 = conn.prepareStatement("insert into loadTable('dfs://test_tsdb1',`update_into)(COL,colcol,`1234`,COL_col_1234)   values (?,?,?,?)");
        ps2.setInt(1, 3);
        ps2.setString(2, "w");
        ps2.setString(3, "1234");
        ps2.setString(4, "COL_col_1234");
        ps2.execute();

        PreparedStatement ps3 = conn.prepareStatement("insert into `pt`(COL,colcol,`1234`,COL_col_1234)   values (?,?,?,?)");
        ps3.setInt(1, 4);
        ps3.setString(2, "w");
        ps3.setString(3, "1234");
        ps3.setString(4, "COL_col_1234");
        ps3.execute();

        JDBCResultSet rs = (JDBCResultSet) stm.executeQuery("select `COL`,`colcol`,`1234`,`COL_col_1234` from loadTable('dfs://test_tsdb1',`update_into) ");
        BasicTable result = (BasicTable) rs.getResult();
        assertEquals(4, result.rows());
        System.out.println(result.getString());
    }

    @Test
    public void test_PreparedStatement_update_dfs_backtickQuoted() throws SQLException, IOException {
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_tsdb1'))" +
                "{ dropDatabase('dfs://test_tsdb1')} \n"+
                " update_into = table(array(INT, 0) as `COL`,array(STRING, 0) as `colcol`,array(STRING, 0) as `1234`,array(STRING, 0) as `COL_col_1234`);\n" +
                " insert into `update_into` (`COL`,`colcol`,`1234`,`COL_col_1234`) values (1,`w,`w,`d);\n" +
                "insert into update_into values (2,`w,`w,`d);\n" +
                "insert into update_into(COL,colcol,`1234`,COL_col_1234)   values (3,`w,`w,`d);\n" +
                "db=database('dfs://test_tsdb1', RANGE, 1 2001 4001 6001 8001 10001 10000000,,'TSDB') \n"+
                "db.createPartitionedTable(update_into, `update_into, `COL,,`COL).append!(update_into)\n";
        DBConnection db = new DBConnection(SqlStdEnum.MySQL);
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);
        stm = conn.createStatement();
        stm.execute("pt=loadTable('dfs://test_tsdb1',`update_into)");

        PreparedStatement ps = conn.prepareStatement("update loadTable('dfs://test_tsdb1',`update_into) set `colcol`=?,`1234`=?,`COL_col_1234`=? where COL=?");
        ps.setString(1, "2w");
        ps.setString(2, "11");
        ps.setNull(3,Types.VARCHAR);
        ps.setInt(4, 1);
        int rows = ps.executeUpdate();
        assertTrue(rows == SUCCESS_NO_INFO || rows == 1);

        PreparedStatement ps1 = conn.prepareStatement("update loadTable('dfs://test_tsdb1',`update_into) set colcol=?,`1234`=?,`COL_col_1234`=? where `COL`=?");
        ps1.setString(1, "w22");
        ps1.setString(2, "1234");
        ps1.setString(3, "COL_col_1234");
        ps1.setInt(4, 2);
        ps1.addBatch();
        ps1.setString(1, "w");
        ps1.setString(2, "123422");
        ps1.setString(3, "COL_col_1234");
        ps1.setInt(4, 3);
        ps1.addBatch();
        ps1.executeBatch();

        PreparedStatement ps2 = conn.prepareStatement("update `pt` set colcol=?,`1234`=?,`COL_col_1234`=? where `COL`=?");
        ps2.setString(1, "w11");
        ps2.setString(2, "123411");
        ps2.setString(3, "COL_col_123224");
        ps2.setInt(4, 2);
        ps2.execute();
        JDBCResultSet rs = (JDBCResultSet) stm.executeQuery("select `COL`,`colcol`,`1234`,`COL_col_1234` from `pt`  ");
        BasicTable result = (BasicTable) rs.getResult();
        assertEquals(3, result.rows());
        assertEquals("[1,2,3]", result.getColumn(0).getString());
        assertEquals("[2w,w11,w]", result.getColumn(1).getString());
        assertEquals("[11,123411,123422]", result.getColumn(2).getString());
        assertEquals("[,COL_col_123224,COL_col_1234]", result.getColumn(3).getString());
    }

    @Test
    public void test_PreparedStatement_delete_dfs_backtickQuoted() throws SQLException, IOException {
        String script = "login(`admin, `123456); \n"+
                "if(existsDatabase('dfs://test_tsdb1'))" +
                "{ dropDatabase('dfs://test_tsdb1')} \n"+
                " update_into = table(array(INT, 0) as `COL`,array(STRING, 0) as `colcol`,array(STRING, 0) as `1234`,array(STRING, 0) as `COL_col_1234`);\n" +
                " insert into `update_into` (`COL`,`colcol`,`1234`,`COL_col_1234`) values (1,`w,`w,`d);\n" +
                "insert into update_into values (2,`w,`w,`d);\n" +
                "insert into update_into values (4,`w,`w,`d);\n" +
                "insert into update_into(COL,colcol,`1234`,COL_col_1234)   values (3,`w,`w,`d);\n" +
                "db=database('dfs://test_tsdb1', RANGE, 1 2001 4001 6001 8001 10001 10000000,,'TSDB') \n"+
                "db.createPartitionedTable(update_into, `update_into, `COL,,`COL).append!(update_into)\n";
        DBConnection db = new DBConnection(SqlStdEnum.MySQL);
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);
        stm = conn.createStatement();
        stm.execute("pt=loadTable('dfs://test_tsdb1',`update_into)");
        PreparedStatement ps = conn.prepareStatement("delete from loadTable('dfs://test_tsdb1',`update_into) where `COL`=?,colcol=?,`1234`=?,`COL_col_1234`=?");
        ps.setInt(1, 1);
        ps.setString(2, "w");
        ps.setString(3, "w");
        ps.setString(4,"d");
        int rows = ps.executeUpdate();
        assertTrue(rows == SUCCESS_NO_INFO || rows == 1);

        PreparedStatement ps1 = conn.prepareStatement("delete from loadTable('dfs://test_tsdb1',`update_into) where `COL`=?,colcol=?,`1234`=?,`COL_col_1234`=?");
        ps1.setInt(1, 2);
        ps1.setString(2, "w");
        ps1.setString(3, "w");
        ps1.setString(4, "d");
        ps1.addBatch();
        ps1.setInt(1, 4);
        ps1.setString(2, "w");
        ps1.setString(3, "w");
        ps1.setString(4, "d");
        ps1.addBatch();
        ps1.executeBatch();

        PreparedStatement ps2 = conn.prepareStatement("DELETE from `pt` where `COL`=?,colcol=?,`1234`=?,`COL_col_1234`=? ");
        ps2.setInt(1, 3);
        ps2.setString(2, "w");
        ps2.setString(3, "w");
        ps2.setString(4, "d");
        ps2.execute();
        JDBCResultSet rs = (JDBCResultSet) stm.executeQuery("select `COL`,`colcol`,`1234`,`COL_col_1234` from loadTable('dfs://test_tsdb1',`update_into)  ");
        BasicTable result = (BasicTable) rs.getResult();
        assertEquals(0, result.rows());
    }

    @Test
    public void test_PreparedStatement_insert_into_allDateType_DFS_executeBatch_backtickQuoted() throws SQLException {
        JDBCPrepareStatementTest.createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(`col1`,`col2`,`col3`,`col4`,`col5`,`col6`,`col7`,`col8`,`col9`,`col10`,`col11`,`col12`,`col13`,`col14`,`col15`,`col16`,`col17`,`col18`,`col19`,`col20`,`col21`,`col22`,`col23`,`col24`,`col25`,`col26`,`col27`,`col28`,`col29`) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        ps.setInt(1,1000);
        ps.setBoolean(2,true);
        ps.setByte(3, (byte) 12);
        ps.setShort(4, (short) 12);
        ps.setInt(5,100);
        ps.setLong(6, (long) 12);
        ps.setDate(7, Date.valueOf(LocalDate.of(2021,1,1)));
        BasicMonth tmp_month = new BasicMonth(YearMonth.of(2021,1));
        ps.setObject(8, tmp_month);
        ps.setTime(9, Time.valueOf(LocalTime.of(1,1,1)));
        BasicMinute tmp_minute = new BasicMinute(LocalTime.of(1,1));
        ps.setObject(10, tmp_minute);
        BasicSecond tmp_second = new BasicSecond(LocalTime.of(1,1,1));
        ps.setObject(11, tmp_second);
        BasicDateTime tmp_datetime = new BasicDateTime(LocalDateTime.of(2021,1,1,1,1,1));
        ps.setObject(12, tmp_datetime);
        BasicTimestamp tmp_timestamp = new BasicTimestamp(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(13, tmp_timestamp);
        BasicNanoTime tmp_nanotime = new BasicNanoTime(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(14, tmp_nanotime);
        BasicNanoTimestamp tmp_nanotimestamp = new BasicNanoTimestamp(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(15, tmp_nanotimestamp);
        ps.setFloat(16, (float) 12.23);
        ps.setDouble(17, (double) 12.23);
        ps.setString(18, "test1");
        ps.setString(19, "test1");
        BasicUuid uuids = new BasicUuid(1,2);
        ps.setObject(20, uuids);
        BasicDateHour tmp_datehour = new BasicDateHour(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(21, tmp_datehour);
        BasicIPAddr ipaddrs = new BasicIPAddr(1,2);
        ps.setObject(22, ipaddrs);
        BasicInt128 int128 = new BasicInt128(1,2);
        ps.setObject(23, int128);
        ps.setObject(24, "TEST BLOB");
        BasicComplex complexs = new BasicComplex(1,2);
        ps.setObject(25, complexs);
        BasicPoint points = new BasicPoint(0,0);
        ps.setObject(26, points);
        ps.setObject(27,123421.00012,37,4);
        ps.setObject(28,123421.00012,38,4);
        ps.setObject(29,"123421.00012",39,4);
        ps.addBatch();
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt') where `col1`=1000");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
        org.junit.Assert.assertEquals(rs.getByte("col3"), 12);
        org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
        org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
        org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
        org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
        org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
        org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("col11"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col12"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col13"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1),rs.getObject("col14"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("col15"));
        org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
        org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
        org.junit.Assert.assertEquals("test1",rs.getString("col18"));
        org.junit.Assert.assertEquals("test1",rs.getString("col19"));
        org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
        org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
        org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
        org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
        org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
        org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
        org.junit.Assert.assertEquals("123421.00",rs.getObject("col27").toString());
        org.junit.Assert.assertEquals("123421.0001200",rs.getObject("col28").toString());
        org.junit.Assert.assertEquals("123421.0001200000000000000",rs.getObject("col29").toString());
    }

    @Test
    public void test_PreparedStatement_insert_into_DFS_executeUpdate_backtickQuoted() throws SQLException {
        JDBCPrepareStatementTest.createPartitionTable1();
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_type_tsdb1','pt')(`col21`,`col22`,`col23`,`col24`,`col25`,`col26`,`col27`,`col28`,`col29`,`col1`,`col2`,`col3`,`col4`,`col5`,`col6`,`col7`,`col8`,`col9`,`col10`,`col11`,`col12`,`col13`,`col14`,`col15`,`col16`,`col17`,`col18`,`col19`,`col20`) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        BasicDateHour tmp_datehour = new BasicDateHour(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(1, tmp_datehour);
        BasicIPAddr ipaddrs = new BasicIPAddr(1,2);
        ps.setObject(2, ipaddrs);
        BasicInt128 int128 = new BasicInt128(1,2);
        ps.setObject(3, int128);
        ps.setObject(4, "TEST BLOB");
        BasicComplex complexs = new BasicComplex(1,2);
        ps.setObject(5, complexs);
        BasicPoint points = new BasicPoint(0,0);
        ps.setObject(6, points);
        ps.setObject(7,123421.00012,37,4);
        ps.setObject(8,123421.00012,38,4);
        ps.setObject(9,"123421.00012",39,4);
        ps.setInt(10,1000);
        ps.setBoolean(11,true);
        ps.setByte(12, (byte) 12);
        ps.setShort(13, (short) 12);
        ps.setInt(14,100);
        ps.setLong(15, (long) 12);
        ps.setDate(16, Date.valueOf(LocalDate.of(2021,1,1)));
        BasicMonth tmp_month = new BasicMonth(YearMonth.of(2021,1));
        ps.setObject(17, tmp_month);
        ps.setTime(18, Time.valueOf(LocalTime.of(1,1,1)));
        BasicMinute tmp_minute = new BasicMinute(LocalTime.of(1,1));
        ps.setObject(19, tmp_minute);
        BasicSecond tmp_second = new BasicSecond(LocalTime.of(1,1,1));
        ps.setObject(20, tmp_second);
        BasicDateTime tmp_datetime = new BasicDateTime(LocalDateTime.of(2021,1,1,1,1,1));
        ps.setObject(21, tmp_datetime);
        BasicTimestamp tmp_timestamp = new BasicTimestamp(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(22, tmp_timestamp);
        BasicNanoTime tmp_nanotime = new BasicNanoTime(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(23, tmp_nanotime);
        BasicNanoTimestamp tmp_nanotimestamp = new BasicNanoTimestamp(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(24, tmp_nanotimestamp);
        ps.setFloat(25, (float) 12.23);
        ps.setDouble(26, (double) 12.23);
        ps.setString(27, "test1");
        ps.setString(28, "test1");
        BasicUuid uuids = new BasicUuid(1,2);
        ps.setObject(29, uuids);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from loadTable('dfs://test_append_type_tsdb1','pt')");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("col2"), true);
        org.junit.Assert.assertEquals(rs.getByte("col3"), 12);
        org.junit.Assert.assertEquals(rs.getShort("col4"), 12);
        org.junit.Assert.assertEquals(rs.getInt("col5"), 100);
        org.junit.Assert.assertEquals(rs.getLong("col6"), 12);
        org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("col7"));
        org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("col8"));
        org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("col9"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("col10"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("col11"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col12"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("col13"));
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1),rs.getObject("col14"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("col15"));
        org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("col16"),4);
        org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("col17"),4);
        org.junit.Assert.assertEquals("test1",rs.getString("col18"));
        org.junit.Assert.assertEquals("test1",rs.getString("col19"));
        org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("col20"));
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("col21"));
        org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("col22"));
        org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("col23"));
        org.junit.Assert.assertEquals("TEST BLOB",rs.getString("col24"));
        org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("col25"));
        org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("col26"));
        org.junit.Assert.assertEquals("123421.00",rs.getObject("col27").toString());
        org.junit.Assert.assertEquals("123421.0001200",rs.getObject("col28").toString());
        org.junit.Assert.assertEquals("123421.0001200000000000000",rs.getObject("col29").toString());
    }

    @Test
    public void test_PreparedStatement_update_DFS_executeUpdate_backtickQuoted() throws SQLException, IOException {
        JDBCPrepareStatementTest.createPartitionTable_Array("NANOTIMESTAMP");
        JDBCTestUtil.LOGININFO.put("sqlStd", String.valueOf(SqlStdEnum.MySQL));
        Connection conn = JDBCTestUtil.getConnection(JDBCTestUtil.LOGININFO);
        String re = null;
        PreparedStatement ps = conn.prepareStatement("insert into loadTable('dfs://test_append_array_tsdb1','pt') values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2,new LocalDateTime[]{LocalDateTime.of(2030,12,31,23,59,59,999999999),LocalDateTime.of(1969,1,1,1,1,1,001)});
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();

        PreparedStatement ps3 = conn.prepareStatement("update loadTable('dfs://test_append_array_tsdb1','pt') set `col2` = ? where `col1` = ?");
        ps3.setObject(1,new BasicNanoTimestampVector(new long[]{960967025000000000l}));
        ps3.setInt(2,2);
        ps3.executeUpdate();

        BasicNanoTimestampVector bv= new BasicNanoTimestampVector(1);
        bv.setNull(0);
        ps3.setObject(1,bv);
        ps3.setInt(2,1);
        ps3.executeUpdate();
        JDBCResultSet rs3 = (JDBCResultSet) ps.executeQuery("select * from loadTable('dfs://test_append_array_tsdb1','pt')");
        BasicTable re3 = (BasicTable) rs3.getResult();
        Assert.assertEquals("[2000.06.14T07:17:05.000000000]", re3.getColumn(1).get(1).getString());
        Assert.assertEquals("[]", re3.getColumn(1).get(0).getString());
    }
}
