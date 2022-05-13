import com.xxdb.data.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.xxdb.data.BasicTimestamp;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.UUID;

public class JDBCTypeCastTest {
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
    @Test
    public void Test_AllTypeCast(){
        //定义全类型内存表
        try{
            String sqlDDL = "col_sym = symbol(`A`D`C`H)\n" +
                    "col_date = 2017.12.01 2017.12.03 2017.12.04 2017.12.07\n" +
                    "col_time =  08:50:34 09:08:01 09:59:05 10:08:21\n" +
                    "col_month = 2017.07M 2017.09M 2017.10M 2017.12M\n" +
                    "col_dt = 2016.06.13T13:30:10.008 2016.11.13T13:30:10.008 2017.11.13 13:30:10.001 2017.11.13 13:30:11.008\n" +
                    "col_str = `A `F `K `Z \n" +
                    "col_char = 'A' 'C'  'H' 'K'\n" +
                    "col_float = 10.252f  96.1f 100.2f 211.3f\n" +
                    "col_doub = 77.6 123.5 158.444 200.3 \n" +
                    "col_int = 1 2 3 4\n" +
                    "col_nanotime =  09:08:01.001234567 09:08:01.001765432 09:08:01.001987654 09:08:01.981987654\n" +
                    "col_nanotimestamp =  2017.12.01T09:08:01.001234567 2017.12.01T09:08:01.001765432 2017.12.01T09:08:01.001987654 2018.12.01T09:08:01.671987654\n" +
                    "col_bool = true true false false\n" +
                    "col_short = short(1 2 3 4)\n" +
                    "col_long = long(1 2 3 4)\n" +
                    "col_minute = 13:30m 13:30m 13:30m 13:30m\n" +
                    "col_second = 13:30:10 13:30:10 13:30:10 13:30:10\n"+
                    "col_datetime =  2017.12.01T09:08:01 2017.12.01T09:08:01 2017.12.01T09:08:01 2018.12.01T09:08:01\n" +
                    "col_timestamp =  2012.06.13T13:30:10.008 2012.06.13T13:30:10.008 2012.06.13T13:30:10.008 2012.06.13T13:30:10.008\n" +
                    "col_uuid =  rand(uuid(),4)\n" +
                    "col_ipaddr =  rand(ipaddr(),4)\n" +
                    "col_int128 =  rand(int128(),4)\n" +
                    "tb = table(col_sym,col_date,col_time,col_month,col_dt,col_str,col_char,col_float,col_doub,col_int,col_nanotime,col_nanotimestamp,col_bool,col_short,col_long,col_minute,col_second,col_datetime,col_timestamp,col_uuid,col_ipaddr,col_int128)";
            System.out.println(sqlDDL);
            stm.executeUpdate(sqlDDL);
            //读取内存表到RecordSet
            ResultSet rs = stm.executeQuery("select * from tb");
            while(rs.next()){
                try
                {
                    String sym =  rs.getString(1);
                    Date date =  rs.getDate(2);
                    Time tim = rs.getTime(3);
                    BasicMonth mon = (BasicMonth)rs.getObject(4);
                    Timestamp ts = rs.getTimestamp(5);
                    String str = rs.getString(6);
                    String chr = rs.getString(7);
                    float flt = rs.getFloat(8);
                    double dbl = rs.getDouble(9);
                    int i = rs.getInt(10);
                    BasicNanoTime  nt = (BasicNanoTime)rs.getObject(11);
                    BasicNanoTimestamp ntp = (BasicNanoTimestamp) rs.getObject(12);
                    Boolean bool = rs.getBoolean(13);
                    short shorts = rs.getShort(14);
                    long longs = rs.getLong(15);
                    BasicMinute min = (BasicMinute)rs.getObject(16);
                    BasicSecond sec = (BasicSecond) rs.getObject(17);
                    BasicDateTime dt = (BasicDateTime) rs.getObject(18);
                    BasicTimestamp times = (BasicTimestamp) rs.getObject(19);
                    BasicUuid uuids = (BasicUuid) rs.getObject(20);
                    BasicIPAddr ipaddrs = (BasicIPAddr)rs.getObject(21);
                    BasicInt128 int128 = (BasicInt128)rs.getObject(22);
                }catch(Exception ex){
                    Assert.fail(ex.getMessage());
                }
            }

        }catch(SQLException ex)
        {
            Assert.fail(ex.getMessage());
        }
    }

    @After
    public void Destroy(){

    }

}
