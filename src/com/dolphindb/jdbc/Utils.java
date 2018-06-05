package com.dolphindb.jdbc;

import com.xxdb.data.*;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.YearMonth;
import java.util.Properties;

public class Utils {
    public static Object java2db(Object o){
        if(o instanceof BasicStringVector || o instanceof BasicAnyVector || o instanceof Vector){
            String s = ((Vector)o).getString();
            if(((Vector) o).get(0) instanceof BasicString){
                return dbVectorString(s);
            }else{
                return s;
            }
        }else if(o instanceof String || o instanceof BasicString){
            return "`"+o;
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
            return  o;
        }

    }

    public static String dbVectorString(String s){
        String[] strings = s.substring(1,s.length()-1).split(",");
        StringBuilder sb = new StringBuilder("(");
        for(String it : strings){
            sb.append("`").append(it).append(",");
        }
        sb.delete(sb.length()-1,sb.length());
        sb.append(")");
        return sb.toString();
    }

    public static void joinOrder(StringBuilder sb,String[] values,String join){
        for(String item : values){
            if (item != null && item.length()>0){
                sb.append(item).append(join);
            }else{
                break;
            }
        }
        sb.delete(sb.length() - join.length(),sb.length());
    }

    public static void parseProperties(String s, Properties prop, String split1, String split2){
        String[] strings1 = s.split(split1);
        String[] strings2;
        for (String item : strings1){
            strings2 = item.split(split2);
            prop.setProperty(strings2[0],strings2[1]);
        }
    }

    public static String[] getProperties(Properties prop,String[] keys){
        String[] properties = new String[keys.length];
        int index = 0;
        for(String key : keys){
            properties[index] = prop.getProperty(key);
            index++;
        }
        return properties;
    }

    public static String getTableName(String s){
        String s1 = s.trim().split(";")[0];

        int index = s1.indexOf("from");
        if(index != -1){
            return s1.substring(index+4).trim().split(" ")[0];
        }else{
            return s1;
        }
    }

    public static boolean isUpdateable(String s){
        String s1 = s.trim().split(";")[0];
        String regex = "full.*join|inner.*join|right.*join|left.*join|" +
                "join.*(.*)|ej.*(.*)|sej.*(.*)|lj.*(.*)|fj.*(.*)|aj.*(.*)|cj.*(.*)|" +
                "group.*by|context.*by|pivot.*by";
        return !s1.matches(regex);
    }
}
