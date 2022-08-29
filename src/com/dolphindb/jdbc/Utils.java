package com.dolphindb.jdbc;

import com.xxdb.data.*;
import com.xxdb.data.Vector;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.YearMonth;
import java.util.*;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    public static final int DML_OTHER = -1;
    public static final int DML_SELECT = 0;
    public static final int DML_INSERT = 1;
    public static final int DML_UPDATE = 2;
    public static final int DML_DELETE = 3;
    public static final int DML_EXEC = 4;

    public static final Pattern INSERT_PATTERN = Pattern.compile("insert\\sinto\\s[a-zA-Z]{1}[a-zA-Z\\d_]*\\svalues\\s*\\(.+\\)");
    public static final Pattern DELETE_PATTERN  = Pattern.compile("delete\\sfrom\\s[a-zA-Z]{1}[a-zA-Z\\d_]*\\s(where\\s(.+=.+)+)?");
    public static final Pattern UPDATE_PATTERN = Pattern.compile("update\\s[a-zA-Z]{1}[a-zA-Z\\d_]*\\sset\\s(.+=.+)+(\\swhere\\s(.+=.+)+)?");

    public static final Pattern ASSIGN_PATTERN = Pattern.compile("[a-zA-Z]{1}[a-zA-Z\\d_]*[\\s]*=");
    public static Set<String> sqlWareHouse = new HashSet<>();
    //public static HashMap<String, String> sqlWareHouse2 = new HashMap<>();

    private static void createHashSet(){
        sqlWareHouse.add("select");
        sqlWareHouse.add("from");
        sqlWareHouse.add("where");
        sqlWareHouse.add("as");
        sqlWareHouse.add("last");
        sqlWareHouse.add("exec");
        sqlWareHouse.add("or");
        sqlWareHouse.add("and");
        sqlWareHouse.add("order");
        sqlWareHouse.add("group");
        sqlWareHouse.add("by");
        sqlWareHouse.add("interval");
        sqlWareHouse.add("cgroup");
        sqlWareHouse.add("having");
        sqlWareHouse.add("update");
        sqlWareHouse.add("set");
        sqlWareHouse.add("insert");
        sqlWareHouse.add("into");
        sqlWareHouse.add("values");
        sqlWareHouse.add("delete");
        sqlWareHouse.add("limit");
        sqlWareHouse.add("top");
        sqlWareHouse.add("map");
        sqlWareHouse.add("pivot");
        sqlWareHouse.add("partition");
        sqlWareHouse.add("sample");
    }

    public static Object java2db(Object o){
        if(o instanceof BasicStringVector || o instanceof BasicAnyVector || o instanceof AbstractVector || o instanceof Vector){
            String s = ((Vector)o).getString();
            if(((Vector) o).get(0) instanceof BasicString){
                return dbVectorString(s);
            }else{
                return s;
            }
        }else if(o instanceof String || o instanceof BasicString){
            return "\""+o+"\"";
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
        }else {
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

    public static void parseProperties(String s, Properties prop, String split1, String split2) throws SQLException {
        String[] strings1 = s.split(split1);
        int index = 0;
        String[] var7 = strings1;
        int var8 = strings1.length;

        for(int var9 = 0; var9 < var8; ++var9) {
            String item = var7[var9];
            if (item.contains("tb_")) {
                int index1 = item.indexOf("_");
                int index2 = item.indexOf("=");
                int index3 = item.indexOf("+");
                String subString1 = item.substring(index1 + 1, index2);
                String subString2 = item.substring(index2 + 1, index3);
                String subString3 = item.substring(index3 + 1);
                prop.setProperty("script" + index, subString1 + "=loadTable(\"" + subString2 + "\",\"" + subString3 + "\")");
                ++index;
            } else if (item.length() > 0) {
                String[] strings2 = item.split(split2);
                if (strings2.length != 2) {
                    throw new SQLException(item + "     is error");
                }

                if (strings2[0].length() == 0) {
                    throw new SQLException(item + "     is error");
                }

                prop.setProperty(strings2[0], strings2[1]);
            }
        }

        prop.setProperty("length", String.valueOf(index));
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
    private static boolean startsWith(String sentence,String key){
        if(sentence.length()<key.length())
            return false;
        String substr=sentence.substring(0,key.length());
        return substr.compareToIgnoreCase(key)==0;
    }
    public static int getDml(String sql){
        String sqlBackup = new String(sql);
        if(startsWith(sqlBackup,"select")){
            return DML_SELECT;
        }else if(sqlBackup.startsWith("insert") || sqlBackup.startsWith("tableInsert")){
            return DML_INSERT;
        }else if(sqlBackup.startsWith("update")){
            return DML_UPDATE;
        }else if(sqlBackup.startsWith("delete")) {
            return DML_DELETE;
        }else if(sqlBackup.startsWith("exec")){
            return DML_EXEC;
        }else{
            return DML_OTHER;
        }
    }

    public static String getTableName(String sql) throws SQLException{
        String tableName = null;
        if(sql.startsWith("insert")){
            Matcher matcher = INSERT_PATTERN.matcher(sql);
            if(matcher.find()){
                tableName = sql.substring(sql.indexOf("into") + "into".length(), sql.indexOf("values"));
            }else {
                throw new SQLException("check the SQl " + sql);
            }
        }else if(sql.startsWith("tableInsert")){
            tableName = sql.substring(sql.indexOf("(") + "(".length(), sql.indexOf(","));
        }else if(sql.startsWith("append!")){
            tableName = sql.substring(sql.indexOf("(") + "(".length(), sql.indexOf(","));
        }else if(sql.contains(".append!")){
            tableName = sql.split("\\.")[0];
        }else if(sql.startsWith("update")){
            Matcher matcher = UPDATE_PATTERN.matcher(sql);
            if(matcher.find()){
                tableName = sql.substring(sql.indexOf("update") + "update".length(), sql.indexOf("set"));
            }else{
                throw new SQLException("check the SQl " + sql);
            }
        }else if(sql.contains(".update!")){
            tableName = sql.split("\\.")[0];
        }else if(sql.startsWith("delete")){
            Matcher matcher = DELETE_PATTERN.matcher(sql);
            if(matcher.find()){
                int index = sql.indexOf("where");
                if(index != -1) {
                    tableName = sql.substring(sql.indexOf("from") + "from".length(), sql.indexOf("where"));
                }else{
                    tableName = sql.substring(sql.indexOf("from") + "from".length()).replaceAll(";","");
                }
            }else{
                throw new SQLException("check the SQl " + sql);
            }
        }
        return tableName;
    }

    public static boolean isUpdateable(String s){
        String s1 = s.trim().split(";")[0];
        String regex = "full.*join|inner.*join|right.*join|left.*join|" +
                "join.*(.*)|ej.*(.*)|sej.*(.*)|lj.*(.*)|fj.*(.*)|aj.*(.*)|cj.*(.*)|" +
                "group.*by|context.*by|pivot.*by";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(s1);

        return !matcher.find();
    }

    public static String getRandomString(int length) {
        String character = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String base = character + "0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        int number = random.nextInt(character.length());
        sb.append(character.charAt(number));
        for (int i = 1; i < length; i++) {
            number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }

    public static BasicTable Vevtor2Table(Vector vector,String sql) {
        List<String> colNames = new ArrayList<>(1);
        sql = sql.trim();
        if (sql.contains("as")) {
            colNames.add(sql.split(" ")[3]);
        } else {
            colNames.add(sql.split(" ")[1]);
        }
        List<Vector> cols = new ArrayList<>(1);
        cols.add(vector);
        return new BasicTable(colNames, cols);
    }

    public static String changeCase(String sql){
        if (sql==null)
            return sql;
        createHashSet();
        StringBuilder sbSql=new StringBuilder();
        StringBuilder sbKey1=new StringBuilder();
        char chr = 0;
        for (int i = 0;i < sql.length();i++){
            chr=sql.charAt(i);
            if ((chr >='a'&&chr <= 'z')||(chr >= 'A'&&chr <= 'Z')||(chr >= '0'&&chr <= '9')){
                sbKey1.append(chr);
            }else {
                if (sbKey1.length()>0){
                    String key = sbKey1.toString();
                    String lowerKey=key.toLowerCase();
                    if (sqlWareHouse.contains(lowerKey))
                        sbSql.append(lowerKey);
                    else{
                        sbSql.append(key);
                    }
                }
                sbSql.append(chr);
                sbKey1.delete(0, sbKey1.length());
            }
            if (i==sql.length()-1&&sbKey1.length()>0){
                String key = sbKey1.toString();
                String lowerKey=key.toLowerCase();
                if (sqlWareHouse.contains(lowerKey))
                    sbSql.append(lowerKey);
                else{
                    sbSql.append(key);
                }
            }
        }
        return sbSql.toString();
    }

    public static String getSelectOneColName(String sql){
        String colName = sql.substring(sql.indexOf(" as ") + " as ".length());
        return colName;
    }
}
