import com.dolphindb.jdbc.JDBCStatement;
import com.xxdb.DBConnection;
import com.xxdb.data.*;
import org.junit.*;
import com.xxdb.data.BasicTimestamp;
import org.junit.Test;

import java.time.*;
import java.time.format.DateTimeFormatter;
import javax.sound.sampled.Port;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.ResourceBundle;

public class JDBCAppendNewTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("setup/settings");
    static String HOST = bundle.getString("HOST");
    static int PORT = Integer.parseInt(bundle.getString("PORT"));
    Connection conn;
    Statement stm ;

    @Before
    public void Setup(){
        JDBCTestUtil.LOGININFO.put("user", "admin");
        JDBCTestUtil.LOGININFO.put("password", "123456");
        conn = JDBCTestUtil.getConnection(JDBCTestUtil.LOGININFO);
        try {
            stm = conn.createStatement();
        }catch (SQLException ex){

        }
    }

    public static boolean createPartitionTable(String dataType){
        boolean success = false;
        DBConnection db = null;

        try{
            String script = "login(`admin, `123456); \n"+
                    "if(existsDatabase('dfs://test_append_type'))" +
                    "{ dropDatabase('dfs://test_append_type')} \n"+
                    "t = table(10:0,`id`dataType,[INT,"+dataType+"]) \n"+
                    "db=database('dfs://test_append_type', RANGE, 1 2001 4001 6001 8001 10001) \n"+
                    "db.createPartitionedTable(t, `pt, `id) \n";
            db = new DBConnection();
            db.connect(HOST, PORT);
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

    public static boolean createTSDBPartitionTable(String dataType){
        boolean success = false;
        DBConnection db = null;
        try{
            String script = "login(`admin, `123456); \n"+
                    "if(existsDatabase('dfs://test_append_type_tsdb'))" +
                    "{ dropDatabase('dfs://test_append_type_tsdb')} \n"+
                    "t = table(10:0,`id`dataType,[INT,"+dataType+"]) \n"+
                    "db=database('dfs://test_append_type_tsdb', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n"+
                    "db.createPartitionedTable(t, `pt, `id,,`id) \n";
            db = new DBConnection();
            db.connect(HOST, PORT);
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
    public void testAppendTypeBoolean() throws SQLException {
        createPartitionTable("BOOL");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        ps.setBoolean(2,true);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.BOOLEAN);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getBoolean("dataType"), true);
        rs.next();
        rs.getBoolean("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeInt() throws SQLException {
        createPartitionTable("INT");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        ps.setInt(2,100);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.INTEGER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getInt("dataType"), 100);
        rs.next();
        rs.getInt("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeChar() throws SQLException {
        createPartitionTable("CHAR");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        ps.setByte(2, (byte) 12);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.CHAR);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getByte("dataType"), 12);
        rs.next();
        rs.getByte("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeShort() throws SQLException {
        createPartitionTable("SHORT");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        ps.setShort(2, (short) 12);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getShort("dataType"), 12);
        rs.next();
        rs.getShort("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeLong() throws SQLException {
        createPartitionTable("LONG");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        ps.setLong(2, (long) 12);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getLong("dataType"), 12);
        rs.next();
        rs.getLong("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeFloat() throws SQLException {
        createPartitionTable("FLOAT");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        ps.setFloat(2, (float) 12.23);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.FLOAT);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals((float) 12.23,rs.getFloat("dataType"),4);
        rs.next();
        rs.getFloat("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeDouble() throws SQLException {
        createPartitionTable("DOUBLE");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        ps.setDouble(2, (double) 12.23);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.DOUBLE);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals((Double) 12.23,rs.getDouble("dataType"),4);
        rs.next();
        rs.getDouble("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeString() throws SQLException {
        createPartitionTable("STRING");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        ps.setString(2, "test1");
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("test1",rs.getString("dataType"));
        rs.next();
        rs.getString("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeSymbol() throws SQLException {
        createPartitionTable("SYMBOL");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        ps.setString(2, "test1");
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("test1",rs.getString("dataType"));
        rs.next();
        rs.getString("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeDate() throws SQLException {
        createPartitionTable("DATE");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        ps.setDate(2, Date.valueOf(LocalDate.of(2021,1,1)));
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.DATE);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate("dataType"));
        rs.next();
        rs.getDate("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeMonth() throws SQLException {
        createPartitionTable("MONTH");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        BasicMonth tmp_month = new BasicMonth(YearMonth.of(2021,1));
        ps.setObject(2, tmp_month);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(tmp_month,rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeTime() throws SQLException {
        createPartitionTable("TIME");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        ps.setTime(2, Time.valueOf(LocalTime.of(1,1,1)));
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.TIME);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime("dataType"));
        rs.next();
        rs.getTime("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeMinute() throws SQLException {
        createPartitionTable("MINUTE");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        BasicMinute tmp_minute = new BasicMinute(LocalTime.of(1,1));
        ps.setObject(2, tmp_minute);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(tmp_minute,rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeSecond() throws SQLException {
        createPartitionTable("SECOND");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        BasicSecond tmp_second = new BasicSecond(LocalTime.of(1,1,1));
        ps.setObject(2, tmp_second);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(tmp_second,rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeDatetime() throws SQLException {
        createPartitionTable("DATETIME");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        BasicDateTime tmp_datetime = new BasicDateTime(LocalDateTime.of(2021,1,1,1,1,1));
        ps.setObject(2, tmp_datetime);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(tmp_datetime,rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeTimestamp() throws SQLException {
        createPartitionTable("TIMESTAMP");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        BasicTimestamp tmp_timestamp = new BasicTimestamp(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(2, tmp_timestamp);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(tmp_timestamp,rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeNanotime() throws SQLException {
        createPartitionTable("NANOTIME");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        BasicNanoTime tmp_nanotime = new BasicNanoTime(LocalDateTime.of(2021,1,1,1,1,1,001));
        ps.setObject(2, tmp_nanotime);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(tmp_nanotime,rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeNanotimestamp() throws SQLException {
        createPartitionTable("NANOTIMESTAMP");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        BasicNanoTimestamp tmp_nanotimestamp = new BasicNanoTimestamp(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(2, tmp_nanotimestamp);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(tmp_nanotimestamp,rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeDatehour() throws SQLException {
        createPartitionTable("DATEHOUR");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        BasicDateHour tmp_nanotimestamp = new BasicDateHour(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(2, tmp_nanotimestamp);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(tmp_nanotimestamp,rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeUuid() throws SQLException {
        createPartitionTable("UUID");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        BasicUuid uuids = new BasicUuid(1,2);
        ps.setObject(2, uuids);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(uuids,rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeIpaddr() throws SQLException {
        createPartitionTable("IPADDR");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        BasicIPAddr ipaddrs = new BasicIPAddr(1,2);
        ps.setObject(2, ipaddrs);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(ipaddrs,rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeInt128() throws SQLException {
        createPartitionTable("INT128");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        BasicInt128 int128 = new BasicInt128(1,2);
        ps.setObject(2, int128);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(int128,rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeBlob() throws SQLException {
        createTSDBPartitionTable("BLOB");
        stm.execute("pt=loadTable('dfs://test_append_type_tsdb','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        ps.setObject(2, "TEST BLOB");
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("TEST BLOB",rs.getString("dataType"));
        //getBlob还没有实现,等实现后替换getString
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeComplex() throws SQLException {
        createPartitionTable("COMPLEX");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        BasicComplex complexs = new BasicComplex(1,2);
        ps.setObject(2, complexs);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(complexs,rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }


    @After
    public void Destroy(){

    }
}