
import com.xxdb.data.*;
import org.junit.Test;
import java.sql.SQLException;

import static com.dolphindb.jdbc.Utils.*;
import static org.junit.Assert.assertEquals;

public class JDBCUtilsTest {

    //@Before
    public void SetUp() throws SQLException {
    }

    @Test
    public void Test_checkServerVersionIfSupportCatalog() throws SQLException {
        String version1 ="1.30.23.1 2025.08.31 LINUX x86_64";
        String version2 ="2.00.10.1 2025.08.31 LINUX x86_64";
        String version3 ="3.00.1 2025.08.31 LINUX x86_64";
        String version4 ="4.00.0 2025.08.31 LINUX x86_64";
        checkServerVersionIfSupportCatalog(version1);
        assertEquals(false,checkServerVersionIfSupportCatalog(version1));
        assertEquals(false,checkServerVersionIfSupportCatalog(version2));
        assertEquals(true,checkServerVersionIfSupportCatalog(version3));
        assertEquals(true,checkServerVersionIfSupportCatalog(version4));
    }

    @Test
    public void Test_checkServerVersionIfSupportRunSql() throws SQLException {
        String version1 ="1.30.23.1 2025.08.31 LINUX x86_64";
        String version2 ="2.00.14.1 2025.08.31 LINUX x86_64";
        String version3 ="2.00.15.1 2025.08.31 LINUX x86_64";
        String version4 ="3.00.2 2025.08.31 LINUX x86_64";
        String version5 ="3.00.3.4 2025.08.31 LINUX x86_64";
        String version6 ="4.00.0 2025.08.31 LINUX x86_64";
        assertEquals(false,checkServerVersionIfSupportRunSql(version1));
        assertEquals(false,checkServerVersionIfSupportRunSql(version2));
        assertEquals(true,checkServerVersionIfSupportRunSql(version3));
        assertEquals(false,checkServerVersionIfSupportRunSql(version4));
        assertEquals(true,checkServerVersionIfSupportRunSql(version5));
        assertEquals(true,checkServerVersionIfSupportRunSql(version6));
    }

    @Test
    public void Test_checkServerVersionIfSupportRowCount() throws SQLException {
        String version1 ="1.30.23.1 2025.08.31 LINUX x86_64";
        String version2 ="2.00.17.1 2025.08.31 LINUX x86_64";
        String version3 ="2.00.18 2025.08.31 LINUX x86_64";
        String version4 ="3.00.4.3 2025.08.31 LINUX x86_64";
        String version5 ="3.00.5 2025.08.31 LINUX x86_64";
        String version6 ="4.00.0 2025.08.31 LINUX x86_64";
        checkServerVersionIfSupportCatalog(version1);
        assertEquals(false,checkServerVersionIfSupportRowCount(version1));
        assertEquals(false,checkServerVersionIfSupportRowCount(version2));
        assertEquals(false,checkServerVersionIfSupportRowCount(version3));
        assertEquals(false,checkServerVersionIfSupportRowCount(version4));
        assertEquals(true,checkServerVersionIfSupportRowCount(version5));
        assertEquals(true,checkServerVersionIfSupportRowCount(version6));
    }


    @Test
    public void test_BasicEntityFactory_createScalar_string_bool() throws Exception {
        BasicBoolean  re1 = (BasicBoolean)createScalar(Entity.DATA_TYPE.DT_BOOL, "true",0);
        assertEquals("true",re1.getString());
        BasicBoolean  re2 = (BasicBoolean)createScalar(Entity.DATA_TYPE.DT_BOOL, "false",0);
        assertEquals("false",re2.getString());

        BasicBoolean  re3 = (BasicBoolean)createScalar(Entity.DATA_TYPE.DT_BOOL, "true",0);
        assertEquals("true",re1.getString());
        BasicBoolean  re4 = (BasicBoolean)createScalar(Entity.DATA_TYPE.DT_BOOL, "false",0);
        assertEquals("false",re2.getString());
        BasicBoolean  re5 = (BasicBoolean)createScalar(Entity.DATA_TYPE.DT_BOOL, null,0);
        assertEquals("false",re2.getString());
    }

    @Test
    public void test_BasicEntityFactory_createScalar_string_byte() throws Exception {
        BasicByte  re1 = (BasicByte)createScalar(Entity.DATA_TYPE.DT_BYTE, "1",0);
        assertEquals("1",re1.getString());
        BasicByte  re2 = (BasicByte)createScalar(Entity.DATA_TYPE.DT_BYTE, "12",0);
        assertEquals("12",re2.getString());
        BasicByte  re3 = (BasicByte)createScalar(Entity.DATA_TYPE.DT_BYTE, null,0);
        assertEquals("",re3.getString());
    }

    @Test
    public void test_BasicEntityFactory_createScalar_string_short() throws Exception {
        BasicShort  re1 = (BasicShort)createScalar(Entity.DATA_TYPE.DT_SHORT, "-32768",0);
        assertEquals("",re1.getString());
        BasicShort  re2 = (BasicShort)createScalar(Entity.DATA_TYPE.DT_SHORT, "32767",0);
        assertEquals("32767",re2.getString());
        BasicShort  re3 = (BasicShort)createScalar(Entity.DATA_TYPE.DT_SHORT, null,0);
        assertEquals("",re3.getString());
        BasicShort  re4 = (BasicShort)createScalar(Entity.DATA_TYPE.DT_SHORT, "0",0);
        assertEquals("0",re4.getString());
    }

    @Test
    public void test_BasicEntityFactory_createScalar_string_int() throws Exception {
        BasicInt  re1 = (BasicInt)createScalar(Entity.DATA_TYPE.DT_INT, "-2147483648",0);
        assertEquals("",re1.getString());
        BasicInt  re2 = (BasicInt)createScalar(Entity.DATA_TYPE.DT_INT, "2147483647",0);
        assertEquals("2147483647",re2.getString());
        BasicInt  re3 = (BasicInt)createScalar(Entity.DATA_TYPE.DT_INT, null,0);
        assertEquals("",re3.getString());
        BasicInt  re4 = (BasicInt)createScalar(Entity.DATA_TYPE.DT_INT, "0",0);
        assertEquals("0",re4.getString());
    }

    @Test
    public void test_BasicEntityFactory_createScalar_string_long() throws Exception {
        BasicLong  re1 = (BasicLong)createScalar(Entity.DATA_TYPE.DT_LONG, "-9223372036854775808",0);
        assertEquals("",re1.getString());
        BasicLong  re2 = (BasicLong)createScalar(Entity.DATA_TYPE.DT_LONG, "9223372036854775807",0);
        assertEquals("9223372036854775807",re2.getString());
        BasicLong  re3 = (BasicLong)createScalar(Entity.DATA_TYPE.DT_LONG, null,0);
        assertEquals("",re3.getString());
        BasicLong  re4 = (BasicLong)createScalar(Entity.DATA_TYPE.DT_LONG, "0",0);
        assertEquals("0",re4.getString());
    }

    @Test
    public void test_BasicEntityFactory_createScalar_string_float() throws Exception {
        BasicFloat  re1 = (BasicFloat)createScalar(Entity.DATA_TYPE.DT_FLOAT, "-9.1",0);
        assertEquals("-9.10000038",re1.getString());
        BasicFloat  re2 = (BasicFloat)createScalar(Entity.DATA_TYPE.DT_FLOAT, "922.3372",0);
        assertEquals("922.33721924",re2.getString());
        BasicFloat  re3 = (BasicFloat)createScalar(Entity.DATA_TYPE.DT_FLOAT, null,0);
        assertEquals("",re3.getString());
        BasicFloat  re4 = (BasicFloat)createScalar(Entity.DATA_TYPE.DT_FLOAT, "0",0);
        assertEquals("0",re4.getString());
    }

    @Test
    public void test_BasicEntityFactory_createScalar_string_double() throws Exception {
        BasicDouble  re1 = (BasicDouble)createScalar(Entity.DATA_TYPE.DT_DOUBLE, "-9.99",0);
        assertEquals("-9.99",re1.getString());
        BasicDouble  re2 = (BasicDouble)createScalar(Entity.DATA_TYPE.DT_DOUBLE, "922.3372",0);
        assertEquals("922.3372",re2.getString());
        BasicDouble  re3 = (BasicDouble)createScalar(Entity.DATA_TYPE.DT_DOUBLE, null,0);
        assertEquals("",re3.getString());
        BasicDouble  re4 = (BasicDouble)createScalar(Entity.DATA_TYPE.DT_DOUBLE, "0",0);
        assertEquals("0",re4.getString());
    }

    @Test
    public void test_BasicEntityFactory_createScalar_string_date() throws Exception {
        BasicDate  re1 = (BasicDate)createScalar(Entity.DATA_TYPE.DT_DATE, "2024-06-02",0);
        assertEquals("2024.06.02",re1.getString());
        BasicDate  re2 = (BasicDate)createScalar(Entity.DATA_TYPE.DT_DATE, "1964-06-02",0);
        assertEquals("1964.06.02",re2.getString());
        BasicDate  re3 = (BasicDate)createScalar(Entity.DATA_TYPE.DT_DATE, null,0);
        assertEquals("",re3.getString());
        BasicDate  re4 = (BasicDate)createScalar(Entity.DATA_TYPE.DT_DATE, "2039-06-02",0);
        assertEquals("2039.06.02",re4.getString());
    }

    @Test
    public void test_BasicEntityFactory_createScalar_string_month() throws Exception {
        BasicMonth  re1 = (BasicMonth)createScalar(Entity.DATA_TYPE.DT_MONTH, "2024-06",0);
        assertEquals("2024.06M",re1.getString());
        BasicMonth  re2 = (BasicMonth)createScalar(Entity.DATA_TYPE.DT_MONTH, "1964-06",0);
        assertEquals("1964.06M",re2.getString());
        BasicMonth  re3 = (BasicMonth)createScalar(Entity.DATA_TYPE.DT_MONTH, null,0);
        assertEquals("",re3.getString());
        BasicMonth  re4 = (BasicMonth)createScalar(Entity.DATA_TYPE.DT_MONTH, "2039-06",0);
        assertEquals("2039.06M",re4.getString());
    }

    @Test
    public void test_BasicEntityFactory_createScalar_string_time() throws Exception {
        BasicTime  re1 = (BasicTime)createScalar(Entity.DATA_TYPE.DT_TIME, "10:00:00.999",0);
        assertEquals("10:00:00.999",re1.getString());
        BasicTime  re2 = (BasicTime)createScalar(Entity.DATA_TYPE.DT_TIME, "23:59:59.888",0);
        assertEquals("23:59:59.888",re2.getString());
        BasicTime  re3 = (BasicTime)createScalar(Entity.DATA_TYPE.DT_TIME, null,0);
        assertEquals("",re3.getString());
        BasicTime  re4 = (BasicTime)createScalar(Entity.DATA_TYPE.DT_TIME, "00:00:00.000",0);
        assertEquals("00:00:00.000",re4.getString());
    }
    @Test
    public void test_BasicEntityFactory_createScalar_string_minute() throws Exception {
        BasicMinute  re1 = (BasicMinute)createScalar(Entity.DATA_TYPE.DT_MINUTE, "10:00:00",0);
        assertEquals("10:00m",re1.getString());
        BasicMinute  re2 = (BasicMinute)createScalar(Entity.DATA_TYPE.DT_MINUTE, "23:59:59",0);
        assertEquals("23:59m",re2.getString());
        BasicMinute  re3 = (BasicMinute)createScalar(Entity.DATA_TYPE.DT_MINUTE, null,0);
        assertEquals("",re3.getString());
        BasicMinute  re4 = (BasicMinute)createScalar(Entity.DATA_TYPE.DT_MINUTE, "00:00:00",0);
        assertEquals("00:00m",re4.getString());
    }
    @Test
    public void test_BasicEntityFactory_createScalar_string_second() throws Exception {
        BasicSecond  re1 = (BasicSecond)createScalar(Entity.DATA_TYPE.DT_SECOND, "10:00:00",0);
        assertEquals("10:00:00",re1.getString());
        BasicSecond  re2 = (BasicSecond)createScalar(Entity.DATA_TYPE.DT_SECOND, "23:59:59",0);
        assertEquals("23:59:59",re2.getString());
        String obj = null;
        BasicSecond  re3 = (BasicSecond)createScalar(Entity.DATA_TYPE.DT_SECOND, obj,0);
        assertEquals("",re3.getString());
        BasicSecond  re4 = (BasicSecond)createScalar(Entity.DATA_TYPE.DT_SECOND, "00:00:00",0);
        assertEquals("00:00:00",re4.getString());
    }
    @Test
    public void test_BasicEntityFactory_createScalar_string_datetime() throws Exception {
        BasicDateTime  re1 = (BasicDateTime)createScalar(Entity.DATA_TYPE.DT_DATETIME, "2024-06-02T10:00:00.000",0);
        assertEquals("2024.06.02T10:00:00",re1.getString());
        BasicDateTime  re2 = (BasicDateTime)createScalar(Entity.DATA_TYPE.DT_DATETIME, "1964-06-02T23:59:59.999",0);
        assertEquals("1964.06.02T23:59:59",re2.getString());
        BasicDateTime  re3 = (BasicDateTime)createScalar(Entity.DATA_TYPE.DT_DATETIME, null,0);
        assertEquals("",re3.getString());
        BasicDateTime  re4 = (BasicDateTime)createScalar(Entity.DATA_TYPE.DT_DATETIME, "2036-06-02T10:00:00.888",0);
        assertEquals("2036.06.02T10:00:00",re4.getString());
    }
    @Test
    public void test_BasicEntityFactory_createScalar_string_timestamp() throws Exception {
        BasicTimestamp  re1 = (BasicTimestamp)createScalar(Entity.DATA_TYPE.DT_TIMESTAMP, "2024-06-02T10:00:00.000",0);
        assertEquals("2024.06.02T10:00:00.000",re1.getString());
        BasicTimestamp  re2 = (BasicTimestamp)createScalar(Entity.DATA_TYPE.DT_TIMESTAMP, "1964-06-02T23:59:59.999",0);
        assertEquals("1964.06.02T23:59:59.999",re2.getString());
        BasicTimestamp  re3 = (BasicTimestamp)createScalar(Entity.DATA_TYPE.DT_TIMESTAMP, null,0);
        assertEquals("",re3.getString());
        BasicTimestamp  re4 = (BasicTimestamp)createScalar(Entity.DATA_TYPE.DT_TIMESTAMP, "2039-06-02T10:00:00.888",0);
        assertEquals("2039.06.02T10:00:00.888",re4.getString());
    }

    @Test
    public void test_BasicEntityFactory_createScalar_string_nanotime() throws Exception {
        BasicNanoTime  re = (BasicNanoTime)createScalar(Entity.DATA_TYPE.DT_NANOTIME, "10:00:00.000",0);
        assertEquals("10:00:00.000000000",re.getString());
        BasicNanoTime  re1 = (BasicNanoTime)createScalar(Entity.DATA_TYPE.DT_NANOTIME, "10:00:00.0002222",0);
        assertEquals("10:00:00.000222200",re1.getString());
        BasicNanoTime  re2 = (BasicNanoTime)createScalar(Entity.DATA_TYPE.DT_NANOTIME, "23:59:59.999",0);
        assertEquals("23:59:59.999000000",re2.getString());
        BasicNanoTime  re3 = (BasicNanoTime)createScalar(Entity.DATA_TYPE.DT_NANOTIME, null,0);
        assertEquals("",re3.getString());
        BasicNanoTime  re4 = (BasicNanoTime)createScalar(Entity.DATA_TYPE.DT_NANOTIME, "10:00:00.888",0);
        assertEquals("10:00:00.888000000",re4.getString());
    }

    @Test
    public void test_BasicEntityFactory_createScalar_string_nanotimestamp() throws Exception {
        BasicNanoTimestamp  re1 = (BasicNanoTimestamp)createScalar(Entity.DATA_TYPE.DT_NANOTIMESTAMP, "2024-06-02T10:00:00.000",0);
        assertEquals("2024.06.02T10:00:00.000000000",re1.getString());
        BasicNanoTimestamp  re2 = (BasicNanoTimestamp)createScalar(Entity.DATA_TYPE.DT_NANOTIMESTAMP, "1964-06-02T23:59:59.999",0);
        assertEquals("1964.06.02T23:59:59.999000000",re2.getString());
        BasicNanoTimestamp  re3 = (BasicNanoTimestamp)createScalar(Entity.DATA_TYPE.DT_NANOTIMESTAMP, null,0);
        assertEquals("",re3.getString());
        BasicNanoTimestamp  re4 = (BasicNanoTimestamp)createScalar(Entity.DATA_TYPE.DT_NANOTIMESTAMP, "2039-06-02T10:00:00.888",0);
        assertEquals("2039.06.02T10:00:00.888000000",re4.getString());
    }

    @Test
    public void test_BasicEntityFactory_createScalar_string_datehour() throws Exception {
        BasicDateHour  re1 = (BasicDateHour)createScalar(Entity.DATA_TYPE.DT_DATEHOUR, "2024-06-02T10:00:00.000",0);
        assertEquals("2024.06.02T10",re1.getString());
        BasicDateHour  re2 = (BasicDateHour)createScalar(Entity.DATA_TYPE.DT_DATEHOUR, "1964-06-02T23:59:59.999",0);
        assertEquals("1964.06.02T23",re2.getString());
        BasicDateHour  re3 = (BasicDateHour)createScalar(Entity.DATA_TYPE.DT_DATEHOUR, null,0);
        assertEquals("",re3.getString());
        BasicDateHour  re4 = (BasicDateHour)createScalar(Entity.DATA_TYPE.DT_DATEHOUR, "2039-06-02T10:00:00.888",0);
        assertEquals("2039.06.02T10",re4.getString());
    }
}
