import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.*;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class JDBCTypeCastTest {
    Connection conn;
    Statement stm ;
    static String HOST = JDBCTestUtil.HOST;
    static int PORT = JDBCTestUtil.PORT ;
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

    public static boolean CreateMemTable(String host, Integer port) {
        boolean success = false;
        DBConnection db = null;
        try {
            //定义全类型内存表
            String script = "col_sym = symbol(`A`D`C`H)\n" +
                    "col_date = 2017.12.01 1954.12.03 2099.12.04 1970.12.07\n" +
                    "col_time =  08:50:34 09:08:01 09:59:05 10:08:21\n" +
                    "col_month = 2017.07M 2099.09M 1969.10M 1970.12M\n" +
                    "col_dt = 2016.06.13T13:30:10.008 2099.11.13T13:30:10.008 1969.11.13T13:30:10.001 1970.11.13T13:30:11.008\n" +
                    "col_str = `A `F `K `Z \n" +
                    "col_char = 'A' 'C'  'H' 'K'\n" +
                    "col_float = 10.252f  96.1f 100.2f 211.3f\n" +
                    "col_doub = 77.6 123.5 158.444 200.3 \n" +
                    "col_int = 1 2 3 4\n" +
                    "col_nanotime =  09:08:01.001234567 09:08:01.001765432 09:08:01.001987654 09:08:01.981987654\n" +
                    "col_nanotimestamp =  2017.12.01T09:08:01.001234567 2099.12.01T09:08:01.001765432 1969.12.01T09:08:01.001987654 1970.12.01T09:08:01.671987654\n" +
                    "col_bool = true true false false\n" +
                    "col_short = short(1 2 3 4)\n" +
                    "col_long = long(1 2 3 4)\n" +
                    "col_minute = 13:30m 13:30m 13:30m 13:30m\n" +
                    "col_second = 13:30:10 13:30:10 13:30:10 13:30:10\n"+
                    "col_datetime =  2016.06.13T13:30:10 2099.11.13T13:30:10 1969.11.13T13:30:10 1970.11.13T13:30:11\n" +
                    "col_timestamp =  2016.06.13T13:30:10.008 2099.11.13T13:30:10.008 1969.11.13T13:30:10.001 1970.11.13T13:30:11.008\n" +
                    "col_uuid =  uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\" \"5d212a78-cc48-e3b1-4235-b4d91473ee81\" \"5d212a78-cc48-e3b1-4235-b4d91473ee82\" \"5d212a78-cc48-e3b1-4235-b4d91473ee83\")\n" +
                    "col_ipaddr =  ipaddr(\"192.168.1.13\" \"192.168.1.13\" \"192.168.1.13\" \"0.0.0.0\")\n" +
                    "col_int128 =  int128(\"e1671797c52e15f763380b45e841ec33\" \"e1671797c52e15f763380b45e841ec33\" \"e1671797c52e15f763380b45e841ec33\" \"e1671797c52e15f763380b45e841ec33\")\n" +
                    "col_blob = blob(`A`D`C`H)\n" +
                    "col_complex = [complex(1,2),complex(2,3),complex(3,4),complex(4,5)]\n" +
                    "col_point = [point(1,2),point(3,4),point(5,6),point(7,8)]\n"+
                    "col_decimal32 = decimal32([1.321,4231,-1.321,0],4)\n"+
                    "col_decimal64 = decimal64([1.321,4231,-1.321,0],8)\n"+
                    "col_decimal128 = decimal128([1.321,4231,-1.321,0],16)\n"+
                    "col_datehour = [datehour(2021.06.13 13:30:10),datehour(1969.06.13 13:30:10),datehour(1970.06.13 13:30:10),datehour(2099.06.13 13:30:10)]\n"+
                    "share table(col_sym,col_date,col_time,col_month,col_dt,col_str,col_char,col_float,col_doub,col_int,col_nanotime,col_nanotimestamp,col_bool,col_short,col_long,col_minute,col_second,col_datetime,col_timestamp,col_uuid,col_ipaddr,col_int128,col_blob,col_complex,col_point,col_decimal32,col_decimal64,col_decimal128,col_datehour) as tb";
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
    public static boolean CreateMemTable_null(String host, Integer port) {
        boolean success = false;
        DBConnection db = null;
        try {
            //定义全类型内存表
            String script = "colNames=\"col\"+string(1..29);\n" +
                    "colNames=\"col\"+string(1..29);\n" +
                    "colTypes=[BOOL,CHAR,SHORT,INT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                    "t=table(1:0,colNames,colTypes);\n" +
                    "insert into t values(,,,1,,,,,,,,,,,,,,,,,,,,,,,,,)\n" +
                    "share t as tb_null";
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
    public void Test_AllTypeCast() throws SQLException {
            CreateMemTable(HOST,PORT);
            //读取内存表到RecordSet
            ResultSet rs = stm.executeQuery("select * from tb");
            while(rs.next()){
                    String sym =  rs.getString(1);
                    Date date =  rs.getDate(2);
                    Time tim = rs.getTime(3);
                    YearMonth mon = (YearMonth)rs.getObject(4);
                    Timestamp ts = rs.getTimestamp(5);
                    String str = rs.getString(6);
                    Byte chr = rs.getByte(7);
                    Float flt = rs.getFloat(8);
                    Double dbl = rs.getDouble(9);
                    Integer i = rs.getInt(10);
                    LocalTime  nt = (LocalTime)rs.getObject(11);
                    LocalDateTime ntp = (LocalDateTime) rs.getObject(12);
                    Boolean bool = rs.getBoolean(13);
                    Short shorts = rs.getShort(14);
                    Long longs = rs.getLong(15);
                    LocalTime min = (LocalTime)rs.getObject(16);
                    LocalTime sec = (LocalTime) rs.getObject(17);
                    LocalDateTime dt = (LocalDateTime) rs.getObject(18);
                    LocalDateTime times = (LocalDateTime) rs.getObject(19);
                    UUID uuids = (UUID) rs.getObject(20);
                    String ipaddrs = (String)rs.getObject(21);
                    String int128 = (String)rs.getObject(22);
                    String blobs = (String)rs.getObject(23);
                    String complexs = (String)rs.getObject(24);
                    String points = (String) rs.getObject(25);
                    BigDecimal decimal32 = (BigDecimal) rs.getObject(26);
                    BigDecimal decimal64 = (BigDecimal) rs.getObject(27);
                    BigDecimal decimal128 = (BigDecimal) rs.getObject(28);
            }
    }
    @Test
    public void Test_getObject_SYMBOL() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        String sym = rs.getString(1);
        String sym1 = (String)rs.getObject(1);
        assertEquals("A", sym);
        assertEquals("A", sym1);
        rs.next();
        String sym2 = rs.getString(1);
        String sym3 = (String)rs.getObject(1);
        assertEquals("D", sym2);
        assertEquals("D", sym3);
        rs.next();
        String sym4 = rs.getString(1);
        String sym5 = (String)rs.getObject(1);
        assertEquals("C", sym4);
        assertEquals("C", sym5);
        rs.next();
        String sym6 = rs.getString(1);
        String sym7 = (String)rs.getObject(1);
        assertEquals("H", sym6);
        assertEquals("H", sym7);
    }
    @Test
    public void Test_getObject_date() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        Date  date1 = (Date)rs.getObject(2);
        assertEquals(java.sql.Date.valueOf(LocalDate.of(2017,12,01)), date1);
        rs.next();
        Date date2 = (Date)rs.getObject(2);
        assertEquals(java.sql.Date.valueOf(LocalDate.of(1954,12,03)), date2);
        rs.next();
        Date date3 = (Date)rs.getObject(2);
        assertEquals(java.sql.Date.valueOf(LocalDate.of(2099,12,04)), date3);
        rs.next();
        Date date4 = (Date)rs.getObject(2);
        assertEquals(java.sql.Date.valueOf(LocalDate.of(1970,12,07)), date4);
    }
    @Test
    public void Test_getObject_time() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        LocalTime time1 = (LocalTime)rs.getObject(3);
        assertEquals(LocalTime.of(8,50,34), time1);
        rs.next();
        LocalTime time2 = (LocalTime)rs.getObject(3);
        assertEquals(LocalTime.of(9,8,01), time2);
        rs.next();
        LocalTime time3 = (LocalTime)rs.getObject(3);
        assertEquals(LocalTime.of(9,59,05), time3);
        rs.next();
        LocalTime time4 = (LocalTime)rs.getObject(3);
        assertEquals(LocalTime.of(10,8,21), time4);
    }
    @Test
    public void Test_getObject_month() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        YearMonth month1 = (YearMonth) rs.getObject(4);
        assertEquals(YearMonth.of(2017, 7), month1);
        rs.next();
        YearMonth month2 = (YearMonth) rs.getObject(4);
        assertEquals(YearMonth.of(2099, 9), month2);
        rs.next();
        YearMonth month3 = (YearMonth) rs.getObject(4);
        assertEquals(YearMonth.of(1969, 10), month3);
        rs.next();
        YearMonth month4 = (YearMonth) rs.getObject(4);
        assertEquals(YearMonth.of(1970, 12), month4);
    }
    @Test
    public void Test_getObject_timestamp() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        LocalDateTime timestamp1 = (LocalDateTime) rs.getObject(5);
        assertEquals(LocalDateTime.of(2016,06,13,13,30,10,8000000), timestamp1);
        rs.next();
        LocalDateTime timestamp2 = (LocalDateTime) rs.getObject(5);
        assertEquals(LocalDateTime.of(2099,11,13,13,30,10,8000000), timestamp2);
        rs.next();
        LocalDateTime timestamp3 = (LocalDateTime) rs.getObject(5);
        assertEquals(LocalDateTime.of(1969,11,13,13,30,10,1000000), timestamp3);
        rs.next();
        LocalDateTime timestamp4 = (LocalDateTime) rs.getObject(5);
        assertEquals(LocalDateTime.of(1970,11,13,13,30,11,8000000), timestamp4);
    }
    @Test
    public void Test_getObject_string() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        String str1 = (String)rs.getObject(6);
        assertEquals("A", str1);
        rs.next();
        String str2 = (String)rs.getObject(6);
        assertEquals("F", str2);
        rs.next();
        String str3 = (String)rs.getObject(6);
        assertEquals("K", str3);
        rs.next();
        String str4 = (String)rs.getObject(6);
        assertEquals("Z", str4);
    }
    @Test
    public void Test_getObject_char() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        Byte char1 = (Byte)rs.getObject(7);
        assertEquals(new Byte((byte) 65), char1);
        rs.next();
        Byte char2 = (Byte)rs.getObject(7);
        assertEquals(new Byte((byte) 67), char2);
        rs.next();
        Byte char3 = (Byte)rs.getObject(7);
        assertEquals(new Byte((byte) 72), char3);
        rs.next();
        Byte char4 = (Byte)rs.getObject(7);
        assertEquals(new Byte((byte) 75), char4);
    }
    @Test
    public void Test_getObject_float() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        Float float1 = (Float)rs.getObject(8);
        assertEquals(10.252f, float1,3);
        rs.next();
        Float float2 = (Float)rs.getObject(8);
        assertEquals(96.1f, float2,1);
        rs.next();
        Float float3 = (Float)rs.getObject(8);
        assertEquals(100.2f, float3,1);
        rs.next();
        Float float4 = (Float)rs.getObject(8);
        assertEquals(211.3f, float4,1);
    }
    @Test
    public void Test_getObject_double() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        Double double1 = (Double)rs.getObject(9);
        assertEquals(77.6, double1,1);
        rs.next();
        Double double2 = (Double)rs.getObject(9);
        assertEquals(123.5, double2,1);
        rs.next();
        Double double3 = (Double)rs.getObject(9);
        assertEquals(158.444, double3,3);
        rs.next();
        Double double4 = (Double)rs.getObject(9);
        assertEquals(200.3, double4,3);
    }
    @Test
    public void Test_getObject_int() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        Integer int1 = (Integer)rs.getObject(10);
        assertEquals(1, int1.intValue());
        rs.next();
        Integer int2 = (Integer)rs.getObject(10);
        assertEquals(2, int2.intValue());
        rs.next();
        Integer int3 = (Integer)rs.getObject(10);
        assertEquals(3, int3.intValue());
        rs.next();
        Integer int4 = (Integer)rs.getObject(10);
        assertEquals(4, int4.intValue());
    }
    @Test
    public void Test_getObject_nanotime() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        LocalTime nanotime1 = (LocalTime)rs.getObject(11);
        assertEquals(LocalTime.of(9,8,1,1234567), nanotime1);
        rs.next();
        LocalTime nanotime2 = (LocalTime)rs.getObject(11);
        assertEquals(LocalTime.of(9,8,01,1765432), nanotime2);
        rs.next();
        LocalTime nanotime3 = (LocalTime)rs.getObject(11);
        assertEquals(LocalTime.of(9,8,1,1987654), nanotime3);
        rs.next();
        LocalTime nanotime4 = (LocalTime)rs.getObject(11);
        assertEquals(LocalTime.of(9,8,1,981987654), nanotime4);
    }
    @Test
    public void Test_getObject_nanotimestamp() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        LocalDateTime nanotimestamp1 = (LocalDateTime)rs.getObject(12);
        assertEquals(LocalDateTime.of(2017,12,1,9,8,1,1234567), nanotimestamp1);
        rs.next();
        LocalDateTime nanotimestamp2 = (LocalDateTime)rs.getObject(12);
        assertEquals(LocalDateTime.of(2099,12,1,9,8,1,1765432), nanotimestamp2);
        rs.next();
        LocalDateTime nanotimestamp3 = (LocalDateTime)rs.getObject(12);
        assertEquals(LocalDateTime.of(1969,12,1,9,8,1,1987654), nanotimestamp3);
        rs.next();
        LocalDateTime nanotimestamp4 = (LocalDateTime)rs.getObject(12);
        assertEquals(LocalDateTime.of(1970,12,1,9,8,1,671987654), nanotimestamp4);
    }
    @Test
    public void Test_getObject_bool() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        Boolean bool1 = (Boolean)rs.getObject(13);
        assertEquals(true, bool1);
        rs.next();
        Boolean bool2 = (Boolean)rs.getObject(13);
        assertEquals(true, bool2);
        rs.next();
        Boolean bool3 = (Boolean)rs.getObject(13);
        assertEquals(false, bool3);
        rs.next();
        Boolean bool4 = (Boolean)rs.getObject(13);
        assertEquals(false, bool4);
    }
    @Test
    public void Test_getObject_short() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        Short short1 = (Short)rs.getObject(14);
        assertEquals((short)1, short1.shortValue());
        rs.next();
        Short short2 = (Short)rs.getObject(14);
        assertEquals((short)2, short2.shortValue());
        rs.next();
        Short short3 = (Short)rs.getObject(14);
        assertEquals((short)3, short3.shortValue());
        rs.next();
        Short short4 = (Short)rs.getObject(14);
        assertEquals((short)4, short4.shortValue());
    }
    @Test
    public void Test_getObject_long() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        Long long1 = (Long)rs.getObject(15);
        assertEquals(1, long1.longValue());
        rs.next();
        Long long2 = (Long)rs.getObject(15);
        assertEquals(2, long2.longValue());
        rs.next();
        Long long3 = (Long)rs.getObject(15);
        assertEquals(3, long3.longValue());
        rs.next();
        Long long4 = (Long)rs.getObject(15);
        assertEquals(4, long4.longValue());
    }
    @Test
    public void Test_getObject_minute() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        LocalTime time1 = (LocalTime)rs.getObject(16);
        assertEquals(LocalTime.of(13,30), time1);
        rs.next();
        LocalTime time2 = (LocalTime)rs.getObject(16);
        assertEquals(LocalTime.of(13,30), time2);
        rs.next();
        LocalTime time3 = (LocalTime)rs.getObject(16);
        assertEquals(LocalTime.of(13,30), time3);
        rs.next();
        LocalTime time4 = (LocalTime)rs.getObject(16);
        assertEquals(LocalTime.of(13,30), time4);
    }
    @Test
    public void Test_getObject_second() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        LocalTime second1 = (LocalTime)rs.getObject(17);
        assertEquals(LocalTime.of(13,30,10), second1);
        rs.next();
        LocalTime second2 = (LocalTime)rs.getObject(17);
        assertEquals(LocalTime.of(13,30,10), second2);
        rs.next();
        LocalTime second3 = (LocalTime)rs.getObject(17);
        assertEquals(LocalTime.of(13,30,10), second3);
        rs.next();
        LocalTime second4 = (LocalTime)rs.getObject(17);
        assertEquals(LocalTime.of(13,30,10), second4);
    }
    @Test
    public void Test_getObject_datetime() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        LocalDateTime datetime1 = (LocalDateTime) rs.getObject(18);
        assertEquals(LocalDateTime.of(2016,06,13,13,30,10), datetime1);
        rs.next();
        LocalDateTime datetime2 = (LocalDateTime) rs.getObject(18);
        assertEquals(LocalDateTime.of(2099,11,13,13,30,10), datetime2);
        rs.next();
        LocalDateTime datetime3 = (LocalDateTime) rs.getObject(18);
        assertEquals(LocalDateTime.of(1969,11,13,13,30,10), datetime3);
        rs.next();
        LocalDateTime datetime4 = (LocalDateTime) rs.getObject(18);
        assertEquals(LocalDateTime.of(1970,11,13,13,30,11), datetime4);
    }
    @Test
    public void Test_getObject_timestamp_1() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        LocalDateTime timestamp1 = (LocalDateTime) rs.getObject(19);
        assertEquals(LocalDateTime.of(2016,06,13,13,30,10,8000000), timestamp1);
        rs.next();
        LocalDateTime timestamp2 = (LocalDateTime) rs.getObject(19);
        assertEquals(LocalDateTime.of(2099,11,13,13,30,10,8000000), timestamp2);
        rs.next();
        LocalDateTime timestamp3 = (LocalDateTime) rs.getObject(19);
        assertEquals(LocalDateTime.of(1969,11,13,13,30,10,1000000), timestamp3);
        rs.next();
        LocalDateTime timestamp4 = (LocalDateTime) rs.getObject(19);
        assertEquals(LocalDateTime.of(1970,11,13,13,30,11,8000000), timestamp4);
    }
    @Test
    public void Test_getObject_uuid() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        UUID str1 = (UUID)rs.getObject(20);
        assertEquals(UUID.fromString("5d212a78-cc48-e3b1-4235-b4d91473ee87"), str1);
        rs.next();
        UUID str2 = (UUID)rs.getObject(20);
        assertEquals(UUID.fromString("5d212a78-cc48-e3b1-4235-b4d91473ee81"), str2);
        rs.next();
        UUID str3 = (UUID)rs.getObject(20);
        assertEquals(UUID.fromString("5d212a78-cc48-e3b1-4235-b4d91473ee82"), str3);
        rs.next();
        UUID str4 = (UUID)rs.getObject(20);
        assertEquals(UUID.fromString("5d212a78-cc48-e3b1-4235-b4d91473ee83"), str4);
    }
    @Test
    public void Test_getObject_ipaddr() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        String ipaddr1 = (String)rs.getObject(21);
        assertEquals("192.168.1.13", ipaddr1);
        rs.next();
        String ipaddr2 = (String)rs.getObject(21);
        assertEquals("192.168.1.13", ipaddr2);
        rs.next();
        String ipaddr3 = (String)rs.getObject(21);
        assertEquals("192.168.1.13", ipaddr3);
        rs.next();
        String ipaddr4 = (String)rs.getObject(21);
        assertEquals("0.0.0.0", ipaddr4);
    }
    @Test
    public void Test_getObject_int128() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        String int1281 = (String)rs.getObject(22);
        assertEquals("e1671797c52e15f763380b45e841ec33", int1281);
        rs.next();
        String int1282 = (String)rs.getObject(22);
        assertEquals("e1671797c52e15f763380b45e841ec33", int1282);
        rs.next();
        String int1283 = (String)rs.getObject(22);
        assertEquals("e1671797c52e15f763380b45e841ec33", int1283);
        rs.next();
        String int1284 = (String)rs.getObject(22);
        assertEquals("e1671797c52e15f763380b45e841ec33", int1284);
    }
    @Test
    public void Test_getObject_blob() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        String blob1 = (String)rs.getObject(23);
        assertEquals("A", blob1);
        rs.next();
        String blob2 = (String)rs.getObject(23);
        assertEquals("D", blob2);
        rs.next();
        String blob3 = (String)rs.getObject(23);
        assertEquals("C", blob3);
        rs.next();
        String blob4 = (String)rs.getObject(23);
        assertEquals("H", blob4);
    }
    @Test
    public void Test_getObject_complex() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        String complex1 = (String)rs.getObject(24);
        assertEquals("1.0+2.0i", complex1);
        rs.next();
        String complex2 = (String)rs.getObject(24);
        assertEquals("2.0+3.0i", complex2);
        rs.next();
        String complex3 = (String)rs.getObject(24);
        assertEquals("3.0+4.0i", complex3);
        rs.next();
        String complex4 = (String)rs.getObject(24);
        assertEquals("4.0+5.0i", complex4);
    }
    @Test
    public void Test_getObject_point() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        String point1 = (String)rs.getObject(25);
        assertEquals("(1.0, 2.0)", point1);
        rs.next();
        String point2 = (String)rs.getObject(25);
        assertEquals("(3.0, 4.0)", point2);
        rs.next();
        String point3 = (String)rs.getObject(25);
        assertEquals("(5.0, 6.0)", point3);
        rs.next();
        String point4 = (String)rs.getObject(25);
        assertEquals("(7.0, 8.0)", point4);
    }
    @Test
    public void Test_getObject_decimal32() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        BigDecimal decimal321 = (BigDecimal)rs.getObject(26);
        assertEquals(new BigDecimal("1.3210"), decimal321);
        rs.next();
        BigDecimal decimal322 = (BigDecimal)rs.getObject(26);
        assertEquals(new BigDecimal("4231.0000"), decimal322);
        rs.next();
        BigDecimal decimal323 = (BigDecimal)rs.getObject(26);
        assertEquals(new BigDecimal("-1.3210"), decimal323);
        rs.next();
        BigDecimal decimal324 = (BigDecimal)rs.getObject(26);
        assertEquals(new BigDecimal("0.0000"), decimal324);
    }
    @Test
    public void Test_getObject_decimal64() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        BigDecimal decimal641 = (BigDecimal)rs.getObject(27);
        assertEquals(new BigDecimal("1.32100000"), decimal641);
        rs.next();
        BigDecimal decimal642 = (BigDecimal)rs.getObject(27);
        assertEquals(new BigDecimal("4231.00000000"), decimal642);
        rs.next();
        BigDecimal decimal643 = (BigDecimal)rs.getObject(27);
        assertEquals(new BigDecimal("-1.32100000"), decimal643);
        rs.next();
        BigDecimal decimal644 = (BigDecimal)rs.getObject(27);
        assertEquals(new BigDecimal("0.00000000"), decimal644);
    }
    @Test
    public void Test_getObject_decimal128() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        BigDecimal decimal1281 = (BigDecimal)rs.getObject(28);
        assertEquals(new BigDecimal("1.3210000000000000"), decimal1281);
        rs.next();
        BigDecimal decimal1282 = (BigDecimal)rs.getObject(28);
        assertEquals(new BigDecimal("4231.0000000000000000"), decimal1282);
        rs.next();
        BigDecimal decimal1283 = (BigDecimal)rs.getObject(28);
        assertEquals(new BigDecimal("-1.3210000000000000"), decimal1283);
        rs.next();
        BigDecimal decimal1284 = (BigDecimal)rs.getObject(28);
        assertEquals(new BigDecimal("0.0000000000000000"), decimal1284);
    }

    @Test
    public void Test_getObject_datehour() throws SQLException {
        CreateMemTable(HOST,PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb");
        rs.next();
        LocalDateTime datehour1 = (LocalDateTime) rs.getObject(29);
        assertEquals(LocalDateTime.of(2021,6,13,13,0), datehour1);
        rs.next();
        LocalDateTime datehour2 = (LocalDateTime) rs.getObject(29);
        assertEquals(LocalDateTime.of(1969,6,13,13,0), datehour2);
        rs.next();
        LocalDateTime datehour3 = (LocalDateTime) rs.getObject(29);
        assertEquals(LocalDateTime.of(1970,6,13,13,0), datehour3);
        rs.next();
        LocalDateTime datehour4 = (LocalDateTime) rs.getObject(29);
        assertEquals(LocalDateTime.of(2099,6,13,13,0), datehour4);
    }
    @Test
    public void Test_getObject_bool_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        Boolean bool1 = (Boolean) rs.getObject(1);
        assertEquals(null, bool1);
    }
    @Test
    public void Test_getObject_char_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        Byte char1 =  (Byte)rs.getObject(2);
        assertEquals(null, char1);
    }
    @Test
    public void Test_getObject_short_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        Short  short1 = (Short)rs.getObject(3);
        assertEquals(null, short1);
    }
    @Test
    public void Test_getObject_int_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        Integer  int1 =  (Integer)rs.getObject(5);
        assertEquals(null, int1);
    }
    @Test
    public void Test_getObject_long_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        Long long1 = (Long)rs.getObject(6);
        assertEquals(null, long1);
    }
    @Test
    public void Test_getObject_date_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        LocalDate date1 = (LocalDate)rs.getObject(7);
        assertEquals(null, date1);
    }

    @Test
    public void Test_getObject_month_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        YearMonth month1 = (YearMonth) rs.getObject(8);
        assertEquals(null, month1);
    }
    @Test
    public void Test_getObject_time_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        LocalTime time1 = (LocalTime)rs.getObject(9);
        assertEquals(null, time1);
    }
    @Test
    public void Test_getObject_minute_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        LocalTime min1 = (LocalTime)rs.getObject(10);
        assertEquals(null, min1);
    }
    @Test
    public void Test_getObject_second_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        LocalTime second1 = (LocalTime)rs.getObject(11);
        assertEquals(null, second1);
    }
    @Test
    public void Test_getObject_datetime_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        LocalDateTime datetime1 = (LocalDateTime) rs.getObject(12);
        assertEquals(null, datetime1);
    }
    @Test
    public void Test_getObject_timestamp_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        LocalDateTime timestamp1 = (LocalDateTime) rs.getObject(13);
        assertEquals(null, timestamp1);
    }
    @Test
    public void Test_getObject_nanotime_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        LocalTime nanotime1 = (LocalTime)rs.getObject(14);
        assertEquals(null, nanotime1);
    }
    @Test
    public void Test_getObject_nanotimestamp_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        LocalDateTime nanotimestamp1 = (LocalDateTime)rs.getObject(15);
        assertEquals(null, nanotimestamp1);
    }
    @Test
    public void Test_getObject_float_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        Float float1 = (Float)rs.getObject(16);
        assertEquals(null, float1);
    }
    @Test
    public void Test_getObject_double_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        Double double1 = (Double)rs.getObject(17);
        assertEquals(null, double1);
    }
    @Test
    public void Test_getObject_symbol_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        String sym1 = (String)rs.getObject(18);
        assertEquals(null, sym1);
    }
    @Test
    public void Test_getObject_string_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        String str1 = (String)rs.getObject(19);
        assertEquals(null, str1);
    }
    @Test
    public void Test_getObject_uuid_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        UUID str1 = (UUID)rs.getObject(20);
        assertEquals(null, str1);
    }
    @Test
    public void Test_getObject_datehour_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        LocalDateTime datehour1 = (LocalDateTime) rs.getObject(21);
        assertEquals(null, datehour1);
    }
    @Test
    public void Test_getObject_ipaddr_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        String ipaddr1 = (String)rs.getObject(22);
        assertEquals("0.0.0.0", ipaddr1);
    }
    @Test
    public void Test_getObject_int128_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        String int1281 = (String)rs.getObject(23);
        assertEquals(null, int1281);
    }
    @Test
    public void Test_getObject_blob_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        String blob1 = (String)rs.getObject(24);
        assertEquals(null, blob1);
    }
    @Test
    public void Test_getObject_complex_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        String complex1 = (String)rs.getObject(25);
        assertEquals(null, complex1);
    }
    @Test
    public void Test_getObject_point_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        String point1 = (String)rs.getObject(26);
        assertEquals("(,)", point1);
    }
    @Test
    public void Test_getObject_decimal32_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        BigDecimal decimal321 = (BigDecimal)rs.getObject(27);
        assertEquals(null, decimal321);
    }

    @Test
    public void Test_getObject_decimal64_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        BigDecimal decimal641 = (BigDecimal)rs.getObject(28);
        assertEquals(null, decimal641);
    }
    @Test
    public void Test_getObject_decimal128_null() throws SQLException {
        CreateMemTable_null(HOST, PORT);
        //读取内存表到RecordSet
        ResultSet rs = stm.executeQuery("select * from tb_null");
        rs.next();
        BigDecimal decimal1281 = (BigDecimal)rs.getObject(29);
        assertEquals(null, decimal1281);
    }

    @After
    public void Destroy(){
        try {
            stm = conn.createStatement();
            stm.executeUpdate("undef(`tb, SHARED);");
            stm.executeUpdate("undef(`tb_null, SHARED);");
        } catch (SQLException e) {
            //throw new RuntimeException(e);
        }

    }

}
