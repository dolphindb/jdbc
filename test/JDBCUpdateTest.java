import com.xxdb.DBConnection;
import com.xxdb.data.*;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.ResourceBundle;

import static org.junit.Assert.*;

public class JDBCUpdateTest {
    static ResourceBundle bundle = ResourceBundle.getBundle("setup/settings");
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

    public static boolean createTable() {
        DBConnection db = null;
        try {
            StringBuilder sb = new StringBuilder();
            sb = new StringBuilder();
            sb.append("bool = [1b, 0b];\n");
            sb.append("char = [97c, 'A'];\n");
            sb.append("short = [122h, 123h];\n");
            sb.append("int = [21, 22];\n");
            sb.append("long = [22l, 23l];\n");
            sb.append("float  = [2.1f, 2.2f];\n");
            sb.append("double = [2.1, 2.2];\n");
            sb.append("string= [`Hello, `world];\n");
            sb.append("symbol= [`symbol1, `symbol2];\n");
            sb.append("date = [2013.06.13, 2013.06.14];\n");
            sb.append("month = [2016.06M, 2016.07M];\n");
            sb.append("time = [13:30:10.008, 13:30:10.009];\n");
            sb.append("minute = [13:30m, 13:31m];\n");
            sb.append("second = [13:30:10, 13:30:11];\n");
            sb.append("datetime = [2012.06.13 13:30:10, 2012.06.13 13:30:10];\n");
            sb.append("timestamp = [2012.06.13 13:30:10.008, 2012.06.13 13:30:10.009];\n");
            sb.append("nanotime = [13:30:10.008007006, 13:30:10.008007007];\n");
            sb.append("nanotimestamp = [2012.06.13 13:30:10.008007006, 2012.06.13 13:30:10.008007007];\n");
            sb.append("datehour = [datehour(10),datehour(20)];\n");
            sb.append("uuid =  rand(uuid(),2);\n");
            sb.append("ipaddr =  rand(ipaddr(),2);\n");
            sb.append("int128 =  rand(int128(),2);\n");
            sb.append("blob= [`Hello, `world];\n");
            sb.append("complex= [complex(1,2),complex(2,3)];\n");
            sb.append("point= [point(1,2),point(2,3)];\n");
            sb.append("decimal32= decimal32([213.432,1.12],4);\n");
            sb.append("decimal64= decimal64([13.43241,231.54323],8);\n");
            sb.append("decimal128= decimal128([13.43241,231.54323],16);\n");
            sb.append("TT1= table(bool,char,short,int,long,float,double,string,symbol,date,month,time,minute,second,datetime,timestamp,nanotime,nanotimestamp,datehour,uuid,ipaddr,int128,blob,complex,point,decimal32,decimal64,decimal128);\n");
            sb.append("share TT1 as trade;");
            db = new DBConnection();
            db.connect(HOST,PORT);
            db.run(sb.toString());
            return false;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            if (db != null)
                db.close();
        }
    }

    @Test
    public void testUpdateBool() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set bool = ?");
        s.setObject(1,false);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals(false,rs.getBoolean(1));
        rs.next();
        org.junit.Assert.assertEquals(false,rs.getBoolean(1));
    }

    @Test
    public void testUpdateChar() throws SQLException, ClassNotFoundException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set char = ?");
        s.setObject(1,'z');
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals('z',rs.getByte(2));
        rs.next();
        org.junit.Assert.assertEquals('z',rs.getByte(2));
    }

    @Test
    public void testUpdateShort() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set short = ?");
        s.setObject(1,100);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals(100,rs.getShort(3));
        rs.next();
        org.junit.Assert.assertEquals(100,rs.getShort(3));
    }

    @Test
    public void testUpdateInt() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set int = ?");
        s.setObject(1,100);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals(100,rs.getInt(4));
        rs.next();
        org.junit.Assert.assertEquals(100,rs.getInt(4));
    }

    @Test
    public void testUpdateLong() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set long = ?");
        s.setObject(1,100);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals(100,rs.getLong(5));
        rs.next();
        org.junit.Assert.assertEquals(100,rs.getLong(5));
    }

    @Test
    public void testUpdateFloat() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set float = ?");
        s.setObject(1,1.111);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals(1.111,rs.getFloat(6),3);
        rs.next();
        org.junit.Assert.assertEquals(1.111,rs.getFloat(6),3);
    }

    @Test
    public void testUpdateDouble() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set double = ?");
        s.setObject(1,1.111f);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals(1.111,rs.getFloat(7),3);
        rs.next();
        org.junit.Assert.assertEquals(1.111,rs.getFloat(7),3);
    }

    @Test
    public void testUpdateString() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set string = ?");
        s.setString(1,"testtest");
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals("testtest",rs.getString(8));
        rs.next();
        org.junit.Assert.assertEquals("testtest",rs.getString(8));
    }

    @Test
    public void testUpdateSymbol() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set symbol = ?");
        s.setObject(1,"testtest");
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals("testtest",rs.getString(9));
        rs.next();
        org.junit.Assert.assertEquals("testtest",rs.getString(9));
    }

    @Test
    public void testUpdateDate() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set date = ?");
        s.setObject(1,Date.valueOf(LocalDate.of(2021,1,1)));
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate(10));
        rs.next();
        org.junit.Assert.assertEquals(Date.valueOf(LocalDate.of(2021,1,1)),rs.getDate(10));
    }

    @Test
    public void testUpdateMonth() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set month = ?");
        BasicMonth tmp_month = new BasicMonth(YearMonth.of(2021,1));
        s.setObject(1,tmp_month);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals(YearMonth.of(2021,1),rs.getObject(11));
        rs.next();
        org.junit.Assert.assertEquals(YearMonth.of(2021,1),rs.getObject(11));
    }

    @Test
    public void testUpdateTime() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set time = ?");
        s.setObject(1,Time.valueOf(LocalTime.of(1,1,1)));
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime(12));
        rs.next();
        org.junit.Assert.assertEquals(Time.valueOf(LocalTime.of(1,1,1)),rs.getTime(12));
    }

    @Test
    public void testUpdateMinute() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set minute = ?");
        BasicMinute tmp_minute = new BasicMinute(LocalTime.of(1,1));
        s.setObject(1,tmp_minute);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject(13));
        rs.next();
        org.junit.Assert.assertEquals(LocalTime.of(1,1),rs.getObject(13));
    }

    @Test
    public void testUpdateSecond() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set second = ?");
        BasicSecond tmp_second = new BasicSecond(LocalTime.of(1,1,1));
        s.setObject(1,tmp_second);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject(14));
        rs.next();
        org.junit.Assert.assertEquals(LocalTime.of(1,1,1),rs.getObject(14));
    }

    @Test
    public void testUpdateDatetime() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set datetime = ?");
        BasicDateTime tmp_datetime = new BasicDateTime(LocalDateTime.of(2021,1,1,1,1,1));
        s.setObject(1,tmp_datetime);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject(15));
        rs.next();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1),rs.getObject(15));
    }

    @Test
    public void testUpdateTimestamp() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set timestamp = ?");
        BasicTimestamp tmp_timestamp = new BasicTimestamp(LocalDateTime.of(2021,1,1,1,1,1,1));
        s.setObject(1,tmp_timestamp);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,1),rs.getObject(16));
        rs.next();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,1),rs.getObject(16));
    }

    @Test
    public void testUpdateNanotime() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set nanotime = ?");
        BasicNanoTime tmp_nanotime = new BasicNanoTime(LocalDateTime.of(2021,1,1,1,1,1,001));
        s.setObject(1,tmp_nanotime);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,001),rs.getObject(17));
        rs.next();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,001),rs.getObject(17));
    }

    @Test
    public void testUpdateNanotimestamp() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set nanotimestamp = ?");
        BasicNanoTimestamp tmp_nanotimestamp = new BasicNanoTimestamp(LocalDateTime.of(2021,1,1,1,1,1,123456));
        s.setObject(1,tmp_nanotimestamp);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject(18));
        rs.next();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject(18));
    }

    @Test
    public void testUpdateDatehour() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set datehour = ?");
        BasicDateHour tmp_datehour = new BasicDateHour(LocalDateTime.of(2021,1,1,1,1,1,123456));
        s.setObject(1,tmp_datehour);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject(19));
        rs.next();
        org.junit.Assert.assertEquals(LocalDateTime.of(2021,1,1,1,1,1,123456),rs.getObject(19));
    }

    @Test
    public void testUpdateUuid() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set uuid = ?");
        BasicUuid uuids = new BasicUuid(1,2);
        s.setObject(1,uuids);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals("00000000-0000-0001-0000-000000000002",rs.getObject(20));
        rs.next();
        org.junit.Assert.assertEquals("00000000-0000-0001-0000-000000000002",rs.getObject(20));
    }

    @Test
    public void testUpdateIpaddr() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set ipaddr = ?");
        BasicIPAddr ipaddrs = new BasicIPAddr(1,2);
        s.setObject(1,ipaddrs);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject(21));
        rs.next();
        org.junit.Assert.assertEquals("0::1:0:0:0:2",rs.getObject(21));
    }

    @Test
    public void testUpdateInt128() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set int128 = ?");
        BasicInt128 int128 = new BasicInt128(1,2);
        s.setObject(1,int128);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject(22));
        rs.next();
        org.junit.Assert.assertEquals("00000000000000010000000000000002",rs.getObject(22));
    }

    @Test
    public void testUpdateBlob() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set blob = ?");
        s.setObject(1,"testest");
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals("testest",rs.getString(23));
        rs.next();
        org.junit.Assert.assertEquals("testest",rs.getString(23));
    }

    @Test
    public void testUpdateComplex() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set complex = ?");
        BasicComplex complexs = new BasicComplex(10,10);
        s.setObject(1,complexs);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        System.out.println();
        org.junit.Assert.assertEquals("10.0+10.0i",rs.getObject(24));
        rs.next();
        org.junit.Assert.assertEquals("10.0+10.0i",rs.getObject(24));
    }

    @Test
    public void testUpdateWhere() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set int = ? where string = ?");
        s.setObject(1,100);
        s.setObject(2,"Hello");
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals(100,rs.getInt(4));
    }


    @Test
    public void testUpdateWhereContext() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set int = ? where string = ? context by double");
        s.setObject(1,100);
        s.setObject(2,"Hello");
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals(100,rs.getInt(4));
    }

    @Test
    public void testUpdatePoint() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set point = ?");
        BasicPoint point = new BasicPoint(0,0);
        s.setObject(1,point);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject(25));
        rs.next();
        org.junit.Assert.assertEquals("(0.0, 0.0)",rs.getObject(25));
    }

    @Test
    public void testUpdateDecimal32() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set decimal32 = ?");
        s.setObject(1,21.23221,37,4);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals("21.2322",rs.getObject(26).toString());
        rs.next();
        org.junit.Assert.assertEquals("21.2322",rs.getObject(26).toString());
    }

    @Test
    public void testUpdateDecimal32_in_partition_table() throws SQLException {
        createPartitionTable("DECIMAL32(4)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        for(int i = 0;i<100;i++){
            ps.setInt(1, i);
            ps.setObject(2, i*10, 37, 4);
            ps.executeUpdate();
        }
        PreparedStatement s = conn.prepareStatement("update pt set dataType = ?");
        s.setObject(1,21.23221,37,4);
        s.execute();
        ResultSet rs = s.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("21.2322",rs.getObject(2).toString());
    }

    @Test
    public void testUpdateDecimal64() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set decimal64 = ?");
        s.setObject(1,276541.23221,38,8);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals("276541.23221000",rs.getObject(27).toString());
        rs.next();
        org.junit.Assert.assertEquals("276541.23221000",rs.getObject(27).toString());
    }

    @Test
    public void testUpdateDecimal64_in_partition_table() throws SQLException {
        createPartitionTable("DECIMAL64(8)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        for(int i = 0;i<100;i++){
            ps.setInt(1, i);
            ps.setObject(2, i*10, 38, 8);
            ps.executeUpdate();
        }
        PreparedStatement s = conn.prepareStatement("update pt set dataType = ?");
        s.setObject(1,21.2322,38,8);
        s.execute();
        ResultSet rs = s.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("21.23220000",rs.getObject(2).toString());
    }
    @Test
    public void testUpdateDecimal128() throws SQLException {
        createTable();
        PreparedStatement s = conn.prepareStatement("update trade set decimal128 = ?");
        s.setObject(1,276541.23221,39,16);
        s.execute();
        ResultSet rs = s.executeQuery("select * from trade");
        rs.next();
        org.junit.Assert.assertEquals("276541.2322100000063488",rs.getObject(28).toString());
        rs.next();
        org.junit.Assert.assertEquals("276541.2322100000063488",rs.getObject(28).toString());
    }
    @Test
    public void testUpdateDecimal128_in_partition_table() throws SQLException {
        createPartitionTable("DECIMAL128(18)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        for(int i = 0;i<100;i++){
            ps.setInt(1, i);
            ps.setObject(2, i*10, 39, 18);
            ps.executeUpdate();
        }
        PreparedStatement s = conn.prepareStatement("update pt set dataType = ?");
        s.setObject(1,21.232200000000010000,39,18);
        s.execute();
        ResultSet rs = s.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("21.232200000000008192",rs.getObject(2).toString());
    }
    @Test
    public void testUpdateDecimal128_in_partition_table_1() throws SQLException {
        createPartitionTable("DECIMAL128(37)");
        stm.execute("pt=loadTable('dfs://test_append_type','pt')");
        PreparedStatement ps = conn.prepareStatement("insert into pt values(?,?)");
        for(int i = 0;i<100;i++){
            ps.setInt(1, i);
            ps.setObject(2, i*10, 39, 37);
            ps.executeUpdate();
        }
        PreparedStatement s = conn.prepareStatement("update pt set dataType = ?");
        s.setObject(1,9.2322000000000100009999999999999999999,39,37);
        s.execute();
        ResultSet rs = s.executeQuery("select * from pt");
        rs.next();
        org.junit.Assert.assertEquals("9.2322000000000099283663761481033318400",rs.getObject(2).toString());
    }
}
