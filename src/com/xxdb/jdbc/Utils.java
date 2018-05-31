package com.xxdb.jdbc;

import com.xxdb.data.*;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.YearMonth;

public class Utils {
    public static Object java2db(Object o){
        System.out.println(o.getClass().getName());
        if(o instanceof BasicAnyVector){
            if(((BasicAnyVector) o).getEntity(0) instanceof BasicString){
                String s = ((BasicAnyVector) o).getString();
                String[] arrs = s.substring(1,s.length()-1).split(",");
                StringBuilder sb = new StringBuilder("(");
                for(String it : arrs){
                    sb.append("`").append(it).append(",");
                }
                sb.delete(sb.length()-1,sb.length());
                sb.append(")");
                return sb.toString();
            }else{
                return ((Vector) o).getString();
            }
        }else if(o instanceof String || o instanceof BasicString){
            return "`"+o;
        }else if(o instanceof BasicStringVector || o instanceof BasicAnyVector){
            String s = ((BasicStringVector) o).getString();
            String[] arrs = s.substring(1,s.length()-1).split(",");
            StringBuilder sb = new StringBuilder("(");
            for(String it : arrs){
                sb.append("`").append(it).append(",");
            }
            sb.delete(sb.length()-1,sb.length());
            sb.append(")");
            return sb.toString();
        }else if(o instanceof Character){
            return "'"+ o +"'";
        }else if(o instanceof Short || o instanceof BasicShort){
            return o + "h";
        }else if(o instanceof Float || o instanceof BasicFloat){
            return o + "f";
        }else if(o instanceof Date){
            return new BasicDate(((Date) o).toLocalDate());
        }else if(o instanceof Time){
            return new BasicTime(((Time) o).toLocalTime());
        }else if(o instanceof Timestamp){
            return new BasicTimestamp(((Timestamp) o).toLocalDateTime());
        }else if(o instanceof YearMonth){
            return new BasicMonth((YearMonth)o);
        }else if(o instanceof Vector){
            return ((Vector) o).getString();
        }else{
            return  null;
        }

    }
}
