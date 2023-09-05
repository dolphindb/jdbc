import com.xxdb.DBConnection;
import com.xxdb.data.*;
//import jdk.internal.org.objectweb.asm.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.UUID;

import static com.dolphindb.jdbc.Main.printData;

public class JDBCAppendNewTest {
    static String HOST = JDBCTestUtil.HOST;
    static int PORT = JDBCTestUtil.PORT;
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
        org.junit.Assert.assertEquals(YearMonth.of(2021, 1),rs.getObject("dataType"));
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
        org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject("dataType"));
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
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject("dataType"));
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
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("dataType"));
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
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject("dataType"));
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
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1,1),rs.getObject("dataType"));
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
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject("dataType"));
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
        BasicDateHour tmp_datehour = new BasicDateHour(LocalDateTime.of(2021,1,1,1,1,1,123456));
        ps.setObject(2, tmp_datehour);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,0),rs.getObject("dataType"));
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
        org.junit.Assert.assertEquals(UUID.fromString("00000000-0000-0001-0000-000000000002"),rs.getObject("dataType"));
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
        org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject("dataType"));
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
        org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject("dataType"));
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
        org.junit.Assert.assertEquals("1.0+2.0i",rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypePoint() throws SQLException {
        createPartitionTable("POINT");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1);
        BasicPoint points = new BasicPoint(0,0);
        ps.setObject(2, points);
        ps.executeUpdate();
        ps.setInt(1,2);
        ps.setNull(2,Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject("dataType"));
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void testAppendMul() throws SQLException {
        createPartitionTable("INT");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        for(int i =1;i<=10;i++) {
            ps.setInt(1, i);
            ps.setInt(2, i*100);
            ps.executeUpdate();
        }
        ResultSet rs = ps.executeQuery("select count(*) from pt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getInt("count"), 10);
    }

    @Test
    public void testAppendAddBatch() throws SQLException {
        createPartitionTable("INT");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        for(int i =1;i<=10;i++) {
            ps.setInt(1, i);
            ps.setInt(2, i*100);
            ps.addBatch();
        }
        ps.executeBatch();
        ResultSet rs = ps.executeQuery("select count(*) from pt");
        rs.next();
        org.junit.Assert.assertEquals(rs.getInt("count"), 10);
    }

    @Test
    public void testAppendTypeDecimal32_normal() throws SQLException {
        createPartitionTable("DECIMAL32(4)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,1.2345,37,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("1.2345",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal32_null() throws SQLException {
        createPartitionTable("DECIMAL32(4)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,2);
        ps.setNull(2, Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }


    @Test
    public void testAppendTypeDecimal32_0() throws SQLException {
        createPartitionTable("DECIMAL32(4)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,0,37,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("0.0000",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal32_minus() throws SQLException {
        createPartitionTable("DECIMAL32(4)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,-1.34214,37,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("-1.3421",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal32_size_over_scale() throws SQLException {
        createPartitionTable("DECIMAL32(4)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,23211.34232114,37,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("23211.3423",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal32_size_minus_scale() throws SQLException {
        createPartitionTable("DECIMAL32(4)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,23211,37,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("23211.0000",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal32_special1() throws SQLException {
        createPartitionTable("DECIMAL32(4)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,0.000001,37,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("0.0000",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal32_special2() throws SQLException {
        createPartitionTable("DECIMAL32(4)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,1.00012,37,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("1.0001",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal32_overflow() throws SQLException {
        createPartitionTable("DECIMAL32(4)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,3421.00012,37,5);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("class java.math.BigDecimal",rs.getObject("dataType").getClass().toString());
    }

    @Test
    public void testAppendTypeDecimal32_scale_diff() throws SQLException {
        createPartitionTable("DECIMAL32(2)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,37,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("123421.00",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal32_scale_0() throws SQLException {
        createPartitionTable("DECIMAL32(0)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,37,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("123421",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal32_scale_9() throws SQLException {
        createPartitionTable("DECIMAL32(9)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,1.0001,37,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("1.000100000",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal32_table_overflow() throws SQLException {
        createPartitionTable("DECIMAL32(6)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,38,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        org.junit.Assert.assertEquals(0,rs.getRow());
    }

    @Test
    public void testAppendTypeDecimal32_scale_invalue() throws SQLException {
        createPartitionTable("DECIMAL32(4)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,37,10);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        org.junit.Assert.assertEquals(0,rs.getRow());
    }

    @Test
    public void testAppendTypeDecimal32_dataType_not_match() throws SQLException {
        createPartitionTable("DECIMAL32(4)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,38,10);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        org.junit.Assert.assertEquals(0,rs.getRow());
    }

    @Test
    public void testAppendTypeDecimal64_normal() throws SQLException {
        createPartitionTable("DECIMAL64(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,1412.234532,38,8);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("1412.23453200",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal64_null() throws SQLException {
        createPartitionTable("DECIMAL64(4)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,2);
        ps.setNull(2, Types.OTHER);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }

    @Test
    public void testAppendTypeDecimal64_0() throws SQLException {
        createPartitionTable("DECIMAL64(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,0,38,8);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("0.00000000",((BigDecimal)rs.getObject("dataType")).toPlainString());
    }

    @Test
    public void testAppendTypeDecimal64_minus() throws SQLException {
        createPartitionTable("DECIMAL64(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,-1.34214,38,8);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("-1.34214000",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal64_size_over_scale() throws SQLException {
        createPartitionTable("DECIMAL64(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,26411.342641432414,38,8);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("26411.34264143",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal64_size_minus_scale() throws SQLException {
        createPartitionTable("DECIMAL64(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,26411,38,8);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("26411.00000000",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal64_special1() throws SQLException {
        createPartitionTable("DECIMAL64(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,0.00000000000001,38,8);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("0.00000000",((BigDecimal)rs.getObject("dataType")).toPlainString());
    }

    @Test
    public void testAppendTypeDecimal64_special2() throws SQLException {
        createPartitionTable("DECIMAL64(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,1.00000012,38,8);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("1.00000012",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal64_overflow() throws SQLException {
        createPartitionTable("DECIMAL64(10)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,38,5);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("class java.math.BigDecimal",rs.getObject("dataType").getClass().toString());
    }

    @Test
    public void testAppendTypeDecimal64_scale_diff() throws SQLException {
        createPartitionTable("DECIMAL64(2)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,38,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("123421.00",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal64_scale_0() throws SQLException {
        createPartitionTable("DECIMAL64(0)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,38,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("123421",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal64_scale_9() throws SQLException {
        createPartitionTable("DECIMAL64(9)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,1.0001,38,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("1.000100000",rs.getObject("dataType").toString());
    }

    @Test
    public void testAppendTypeDecimal64_table_overflow() throws SQLException {
        createPartitionTable("DECIMAL64(6)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,38,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        org.junit.Assert.assertEquals(0,rs.getRow());
    }
    @Test
    public void testAppendTypeDecimal64_scale_invalue() throws SQLException {
        createPartitionTable("DECIMAL64(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,38,20);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        org.junit.Assert.assertEquals(0,rs.getRow());
    }

    @Test
    public void testAppendTypeDecimal64_dataType_not_match() throws SQLException {
        createPartitionTable("DECIMAL64(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,123421.00012,37,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        org.junit.Assert.assertEquals(0,rs.getRow());
    }
    @Test
    public void testAppendTypeDecimal128_normal() throws SQLException {
        createPartitionTable("DECIMAL128(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"1412.234532",39,8);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("1412.23453200",rs.getObject("dataType").toString());
    }
    @Test
    public void testAppendTypeDecimal128_null() throws SQLException {
        createPartitionTable("DECIMAL128(4)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,2);
        ps.setNull(2, Types.OTHER);
        //ps.setInt(1,1000);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        rs.getObject("dataType");
        org.junit.Assert.assertTrue(rs.wasNull());
    }
    @Test
    public void testAppendTypeDecimal128_0() throws SQLException {
        createPartitionTable("DECIMAL128(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,0,39,8);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("0.00000000",((BigDecimal)rs.getObject("dataType")).toPlainString());
    }
    @Test
    public void testAppendTypeDecimal128_minus() throws SQLException {
        createPartitionTable("DECIMAL128(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"-1.34214",39,8);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("-1.34214000",rs.getObject("dataType").toString());
    }
    @Test
    public void testAppendTypeDecimal128_size_over_scale() throws SQLException {
        createPartitionTable("DECIMAL128(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"26411.342641432414",39,10);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("26411.34264143",rs.getObject("dataType").toString());
    }
    @Test
    public void testAppendTypeDecimal128_size_minus_scale() throws SQLException {
        createPartitionTable("DECIMAL128(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"26411",39,8);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        BigDecimal dataType = (BigDecimal) rs.getObject("dataType");
        org.junit.Assert.assertEquals("26411.00000000",rs.getObject("dataType").toString());
    }
    @Test
    public void testAppendTypeDecimal128_special1() throws SQLException {
        createPartitionTable("DECIMAL128(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"0.00000000000001",39,8);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("0.00000000",((BigDecimal)rs.getObject("dataType")).toPlainString());
    }
    @Test
    public void testAppendTypeDecimal128_special2() throws SQLException {
        createPartitionTable("DECIMAL128(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"1.00000012",39,8);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("1.00000012",rs.getObject("dataType").toString());
    }
    @Test
    public void testAppendTypeDecimal128_overflow() throws SQLException {
        createPartitionTable("DECIMAL128(10)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"123421.00012",39,5);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("class java.math.BigDecimal",rs.getObject("dataType").getClass().toString());
    }
    @Test
    public void testAppendTypeDecimal128_scale_diff() throws SQLException {
        createPartitionTable("DECIMAL128(2)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"123421.00012",39,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("123421.00",rs.getObject("dataType").toString());
    }
    @Test
    public void testAppendTypeDecimal128_scale_0() throws SQLException {
        createPartitionTable("DECIMAL128(0)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"123421.00012",39,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("123421",rs.getObject("dataType").toString());
    }
    @Test
    public void testAppendTypeDecimal128_scale_9() throws SQLException {
        createPartitionTable("DECIMAL128(9)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"1.0001",39,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("1.000100000",rs.getObject("dataType").toString());
    }
    @Test
    public void testAppendTypeDecimal128_memory_table_scale_37() throws SQLException {
        stm.execute("t = table(10:0,`id`dataType,[INT,DECIMAL128(37)]) ");
        PreparedStatement ps = conn.prepareStatement("insert into t values(?,?)");
        Statement stmt = null;
        stmt = conn.createStatement();
        ps.setInt(1,1000);
        ps.setObject(2,"1.9999999999",39,37);
        ps.executeUpdate();
        ResultSet rs = stmt.executeQuery("select * from t");
        //printData(rs);
        rs.next();
        org.junit.Assert.assertEquals("1.9999999999000000000000000000000000000",rs.getObject(2).toString());
    }
    @Test
    public void testAppendTypeDecimal128_DFS_table_scale_37() throws SQLException {
        createPartitionTable("DECIMAL128(37)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        Statement stmt = null;
        stmt = conn.createStatement();
        ps.setInt(1,1000);
        ps.setObject(2,"1.9999999999",39,37);
        ps.executeUpdate();
        ResultSet rs = stmt.executeQuery("select * from pt");
        //printData(rs);
        rs.next();
        org.junit.Assert.assertEquals("1.9999999999000000000000000000000000000",rs.getObject(2).toString());
    }
    @Test
    public void testAppendTypeDecimal128_table_overflow() throws SQLException {
        createPartitionTable("DECIMAL128(6)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"123421.00012",39,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        org.junit.Assert.assertEquals(0,rs.getRow());
    }
    @Test
    public void testAppendTypeDecimal128_scale_invalue() throws SQLException {
        createPartitionTable("DECIMAL128(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"123421.00012",39,20);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        org.junit.Assert.assertEquals(0,rs.getRow());
    }
    @Test
    public void testAppendTypeDecimal128_dataType_not_match() throws SQLException {
        createPartitionTable("DECIMAL128(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        ps.setInt(1,1000);
        ps.setObject(2,"123421.00012",39,4);
        ps.executeUpdate();
        ResultSet rs = ps.executeQuery("select * from pt");
        org.junit.Assert.assertEquals(0,rs.getRow());
    }

    @After
    public void Destroy(){

    }
}