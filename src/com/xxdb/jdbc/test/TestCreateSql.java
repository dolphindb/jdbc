package com.xxdb.jdbc.test;

import com.xxdb.data.BasicDate;

import java.sql.Date;
import java.time.LocalDate;

public class TestCreateSql {
    public static void main(String[] args) throws Exception {
//        String sql = "select * from tab_student where s_number=? ,s_id = ? ";
//        String[] sqls = sql.trim().split("\\?");
//        Object[] values = new Object[sql.length()+1];
//        setObject(values,1,"hhh");
//        setObject(values,2,2);
//        System.out.println(createSql(sqls,values));

//        Date date1 = new Date();
//        long l = date1.getTime();
//        java.sql.Date date = new java.sql.Date(l);
//        Time time = new Time(l);
//        Timestamp timestamp = new Timestamp(l);
//        System.out.println(date);
//        System.out.println(time);
//        System.out.println(timestamp);

          java.util.Date date1 = new java.util.Date();
          long l = date1.getTime();
          Date date = new Date(l);
          BasicDate basicDate = new BasicDate(LocalDate.parse(date.toString()));
          System.out.println(date);
          System.out.println(basicDate);
    }

    private static String createSql(String[] sqls,Object[] values){
        StringBuilder sb = new StringBuilder();
        for(int i=1; i<=sqls.length; ++i){
            sb.append(sqls[i-1]).append(values[i]);
        }
        return sb.toString();
    }

    private static void setObject(Object[] values, int i,Object value){
        if(value instanceof String){
            values[i] = "`"+value;
        }else {
            values[i] = value;
        }
    }
}
