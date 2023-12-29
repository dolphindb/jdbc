package com.dolphindb.jdbc;

import com.xxdb.data.*;
import com.xxdb.data.Vector;
import java.sql.*;
import java.sql.Date;
import java.time.YearMonth;
import java.util.*;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Utils {
    public static final int DML_OTHER = -1;
    public static final int DML_SELECT = 0;
    public static final int DML_INSERT = 1;
    public static final int DML_UPDATE = 2;
    public static final int DML_DELETE = 3;
    public static final int DML_EXEC = 4;

    static String INSERT_STRING = "(insert)\\s+(into)\\s+";

    static String MEM_TABLE_NAME = "([a-zA-Z]{1}[a-zA-Z\\d_]*)";

    static String LOAD_TABLE_NAME = "(loadTable\\(.+?\\))";

    static String TABLE_NAME_STRING = MEM_TABLE_NAME + "|" + LOAD_TABLE_NAME;

    static String VALUE_WITH_QUESTION_STRING= "\\s+(values)\\s*\\(([\\s?,]+)\\)";

    static String VALUE_STRING = "\\s+(values)\\s*\\((.+)\\)";

    static String COLNAME_STRING = "\\s*\\([a-zA-Z\\d_\\,\\s]+?\\)";

    static String DELETE_STRING = "(delete)|\\s+((?i)from)\\s+";

    static String DELETE_WHERE_STRING = "\\s+((where)\\s+(.+=.+)+)?";

    static String INSERT_TABLE_NAME_COLUMN_STRING = "(" + LOAD_TABLE_NAME + "*" + MEM_TABLE_NAME + "*" + ")\\s*(\\((.+?)\\))*";

    public static final Pattern DELETE_PATTERN  = Pattern.compile( DELETE_STRING + MEM_TABLE_NAME + DELETE_WHERE_STRING);

    public static final Pattern DELETE_LOADTABLE_PATTERN = Pattern.compile(DELETE_STRING + LOAD_TABLE_NAME + DELETE_WHERE_STRING);
    public static final Pattern UPDATE_PATTERN = Pattern.compile("update\\s+[a-zA-Z]{1}[a-zA-Z\\d_]*\\s+set\\s+(.+=.+)+(\\s+where\\s+(.+=.+)+)?");

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
        sqlWareHouse.add("desc");
        sqlWareHouse.add("asc");
        sqlWareHouse.add("sum");
        sqlWareHouse.add("max");
        sqlWareHouse.add("min");
        sqlWareHouse.add("avg");
        sqlWareHouse.add("count");
        sqlWareHouse.add("distinct");
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

    public static int getDml(String sql) {
        if (startsWith(sql,"select") || startsWith(sql,"SELECT"))
            return DML_SELECT;
        else if(sql.startsWith("insert") || sql.startsWith("INSERT"))
            return DML_INSERT;
        else if(sql.startsWith("update") || sql.startsWith("UPDATE"))
            return DML_UPDATE;
        else if(sql.startsWith("delete") || sql.startsWith("DELETE"))
            return DML_DELETE;
        else if(sql.startsWith("exec") || sql.startsWith("EXEC"))
            return DML_EXEC;
        else
            return DML_OTHER;
    }

    public static JDBCPrepareStatement.PrepareStatementDeleteStrategy getPrepareStmtDeleteSqlExecuteBatchStrategy(int sqlDmlType, String preProcessedSql, Map<Integer, Integer> deleteIndexSQLToDDB) {
        if (sqlDmlType == Utils.DML_DELETE) {
            String[] splitSqls = null;
            splitSqls = preProcessedSql.split("\\s*(?=[!=><]|(?<!\\w)between\\b|(?<!\\w)and\\b|(?<!\\w)or\\b|(?<!\\w)in\\b(?!\\()|(?<!=)=)\\s*|\\s*(?<=[!=><]|(?<!\\w)between\\b|(?<!\\w)and\\b|(?<!\\w)or\\b|(?<!\\w)in\\b(?!\\()|=(?!=))\\s*");
            // splitSqls = preProcessedSql.split("\\s*(?=[><=]|between|and|or|in)\\s*|\\s*(?<=[><=]|between|and|or|in)\\s*");
            List<String> partsList = Arrays.stream(splitSqls)
                    .filter(str -> !str.isEmpty())
                    .collect(Collectors.toList());

            if (partsList.contains(">") || partsList.contains("<")
                    || partsList.contains("between") || partsList.contains("in") || partsList.contains("or")) {
                return JDBCPrepareStatement.PrepareStatementDeleteStrategy.CONCAT_SQL_CONDITION_WITH_OR;
            } else {
                if (deleteIndexSQLToDDB.size() == 1)
                    return JDBCPrepareStatement.PrepareStatementDeleteStrategy.COMBINE_SQL_WITH_IN;
                else
                    return JDBCPrepareStatement.PrepareStatementDeleteStrategy.COMBINE_SQL_WITH_MAKEKEY;
            }
        } else {
            return null;
        }
    }

    public static String getTableName(String sql, boolean isPrepareStatement) throws SQLException{
        String tableName = null;
        if (sql.startsWith("insert") || sql.startsWith("INSERT")) {
            String checkString = INSERT_STRING + INSERT_TABLE_NAME_COLUMN_STRING + (isPrepareStatement ? VALUE_WITH_QUESTION_STRING : VALUE_STRING);
            Pattern pattern = Pattern.compile(checkString);
            Matcher matcher = pattern.matcher(sql);
            if (sql.matches(checkString) && matcher.find()) {
                tableName = matcher.group(3);
                if (tableName != null && !tableName.isEmpty())
                    return tableName;
                else
                    throw new SQLException("Please check your SQL format: " + sql);
            } else {
                throw new SQLException("Please check your SQL format: " + sql);
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
            Matcher matcher1 = DELETE_PATTERN.matcher(sql);
            Matcher matcher2 = DELETE_LOADTABLE_PATTERN.matcher(sql);
                int index = sql.indexOf("where");
                if(index != -1) {
                    tableName = sql.substring(sql.indexOf("from") + "from".length(), sql.indexOf("where")).trim();
                }else {
                    tableName = sql.substring(sql.indexOf("from") + "from".length()).replaceAll(";", "").trim();
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
    private static boolean isKeyChar(char chr){
        return (chr >='a'&&chr <= 'z')||(chr >= 'A'&&chr <= 'Z')||(chr >= '0'&&chr <= '9')||(chr == '_');
    }
    private static boolean isStringChar(char chr){
        return chr=='\''||chr=='"'||chr=='`';
    }

    public static String changeCase(String sql){
        if (sql==null)
            return sql;

        sql = sql.replaceAll("\r", "");

        createHashSet();
        StringBuilder sbSql=new StringBuilder();
        StringBuilder sbKey1=new StringBuilder();
        char chr = 0;
        char isInString = 0;
        int continueSplashCount=0;
        for (int i = 0;i < sql.length();i++){
            chr=sql.charAt(i);
            int prevContinueSplashCount=continueSplashCount;
            if(isInString != 0) {// is in string
                if(isInString=='`'){//check ` end flag
                    if(!isKeyChar(chr)){//end with no key char
                        isInString=0;
                        continueSplashCount=0;
                        if(isStringChar(chr)){
                            isInString=chr;
                        }
                    }
                }else{// string
                    if(chr=='\\')
                        continueSplashCount++;
                    else
                        continueSplashCount=0;
                    if(chr == isInString){// is end chr?
                        if(prevContinueSplashCount%2==0)// check not \, like \" or \'
                            isInString=0;
                    }
                }
                sbSql.append(chr);
                continue;
            }else{//not in string
                if(isStringChar(chr)){//start of string
                    isInString=chr;
                    sbSql.append(chr);
                    continueSplashCount=0;
                    continue;
                }
            }
            if (isKeyChar(chr)){
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

    public static String changeCase(String sql, JDBCConnection connection){
        if (sql==null)
            return sql;

        sql = sql.replaceAll("\r", "");

        createHashSet();
        StringBuilder sbSql=new StringBuilder();
        StringBuilder sbKey1=new StringBuilder();
        String tableAliasValue;
        try {
            tableAliasValue = connection.getClientInfo("tableAlias");
        } catch (SQLException e) {
            throw new RuntimeException("get tableAlias prop has error!");
        }
        char chr = 0;
        char isInString = 0;
        int continueSplashCount=0;
        for (int i = 0;i < sql.length();i++){
            chr=sql.charAt(i);
            int prevContinueSplashCount=continueSplashCount;
            if(isInString != 0) {// is in string
                if(isInString=='`'){//check ` end flag
                    if(!isKeyChar(chr)){//end with no key char
                        isInString=0;
                        continueSplashCount=0;
                        if(isStringChar(chr)){
                            isInString=chr;
                        }
                    }
                }else{// string
                    if(chr=='\\')
                        continueSplashCount++;
                    else
                        continueSplashCount=0;
                    if(chr == isInString){// is end chr?
                        if(prevContinueSplashCount%2==0)// check not \, like \" or \'
                            isInString=0;
                    }
                }
                sbSql.append(chr);
                continue;
            }else{//not in string
                if(isStringChar(chr)){//start of string
                    isInString=chr;
                    sbSql.append(chr);
                    continueSplashCount=0;
                    continue;
                }
            }
            if (isKeyChar(chr)){
                sbKey1.append(chr);
            }else {
                if (sbKey1.length()>0){
                    String key = sbKey1.toString();
                    String lowerKey=key.toLowerCase();
                    if (sqlWareHouse.contains(lowerKey)) {
                        if (Utils.isNotEmpty(tableAliasValue) && !tableAliasValue.contains(key)) {
                            sbSql.append(lowerKey);
                        } else {
                            if (Utils.isEmpty(tableAliasValue)) {
                                sbSql.append(lowerKey);
                            } else {
                                sbSql.append(key);
                            }
                        }
                    }
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
                if (sqlWareHouse.contains(lowerKey) && (!sqlWareHouse.contains(key))) {
                    if (Utils.isNotEmpty(tableAliasValue) && !tableAliasValue.contains(key)) {
                        sbSql.append(lowerKey);
                    } else {
                        sbSql.append(key);
                    }
                } else{
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

    public static String outerJoinToFullJoin(String sql){
        if(!sql.contains("outer join")){
            return sql;
        }
        if(sql.contains("left outer join")){
            sql = sql.replaceAll("left outer join","left join");
        }else if(sql.contains("right outer")){
            sql = sql.replaceAll("right outer join","right join");
        }else{
            sql = sql.replaceAll("outer join","full join");
        }
        return sql;
    }

    public static String oracleToDolphin(String sql){
        if(sql.contains("length")){
            sql = sql.replaceAll("length\\s*\\(","strlen(");
        }
        if(sql.contains("nvl")){
            sql = sql.replaceAll("nvl\\s*\\(","ifValid(");
        }
        if(sql.contains("replace")){
            sql = sql.replaceAll("replace\\s*\\(","strReplace(");
        }
        return sql;
    }

    public static boolean isEmpty(CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    public static boolean isNotEmpty(CharSequence cs) {
        return !isEmpty(cs);
    }

    public static String parseTableAliasPropToScript(String tableAliasValue) {
        Set<String> aliasSet = new HashSet<>();
        StringBuilder stringBuilder = new StringBuilder();

        try {
            String[] strs = tableAliasValue.split(",");
            for (String str : strs) {
                str = str.trim();
                // split by ':', not '://'
                String[] split = str.split("(?<!:)[:](?!/)");
                if (Utils.isEmpty(str)) {
                    throw new RuntimeException("tableAlias's value cannot be null!");
                }
                if (str.contains("dfs")) {
                    // 1、dfs table
                    if (split.length == 1) {
                        // 1）no contain alias:
                        String finalStr;
                        String[] pathSplit = str.split("(?<!/)/(?!/)");
                        String alias = pathSplit[1];
                        String dbPath = pathSplit[0];
                        if (aliasSet.contains(alias)) {
                            throw new RuntimeException("Duplicate table alias found in property tableAlias: " + alias);
                        }
                        aliasSet.add(alias);

                        finalStr = alias + "=loadTable(\"" + dbPath + "\",\"" + alias + "\");\n";
                        stringBuilder.append(finalStr);
                    } else if (split.length == 2) {
                        String finalStr;
                        if (split[0].contains("dfs") && !split[1].contains("dfs")) {
                            finalStr = parseOtherPath(str, aliasSet);
                            stringBuilder.append(finalStr);
                        } else {
                            // 2）contain alias:
                            String alias = split[0].replaceAll(":", "");
                            String path = split[1];
                            if (aliasSet.contains(alias)) {
                                throw new RuntimeException("Duplicate table alias found in property tableAlias: " + alias);
                            }
                            aliasSet.add(alias);
                            if (Utils.isEmpty(path)) {
                                throw new RuntimeException("The dfs path is empty!");
                            }

                            int lastIndex = path.lastIndexOf('/');
                            if (lastIndex != -1) {
                                String dbPath = path.substring(0, lastIndex);
                                String tbName = path.substring(lastIndex + 1);
                                finalStr = alias + "=loadTable(\"" + dbPath + "\"," + "\"" + tbName + "\");\n";
                                stringBuilder.append(finalStr);
                            }
                        }
                    }
                } else if (str.contains("mvcc")) {
                    // 2、mvcc table
                    if (str.startsWith("mvcc") && Pattern.matches(".*[a-zA-Z]:\\\\.*", str)) {
                        // win: no alias
                        String[] path = str.split("://");
                        int lastDoubleSlashIndex = path[1].lastIndexOf("\\");
                        String dropTableName = path[1].substring(0, lastDoubleSlashIndex - 1);
                        String tbNameAndAlias = path[1].substring(lastDoubleSlashIndex + 1);
                        if (aliasSet.contains(tbNameAndAlias)) {
                            throw new RuntimeException("Duplicate table alias found in property tableAlias: " + tbNameAndAlias);
                        }
                        aliasSet.add(tbNameAndAlias);

                        String finalStr = tbNameAndAlias + "=loadMvccTable(\"" + dropTableName + "\",\"" + tbNameAndAlias + "\");\n";
                        stringBuilder.append(finalStr);
                    } else if (split.length == 1) {
                        // no alias:
                        String finalStr;
                        List<String> mvccPathSplit = parseMvccPath(split[0]);
                        String mvccPath = mvccPathSplit.get(1);
                        String[] pathSplit = mvccPath.split("/");
                        String alias = pathSplit[pathSplit.length - 1];
                        if (aliasSet.contains(alias)) {
                            throw new RuntimeException("Duplicate table alias found in property tableAlias: " + alias);
                        }
                        aliasSet.add(alias);
                        String mvccFilePath = mvccPath.substring(0, mvccPath.lastIndexOf("/"));
                        if (mvccPath.startsWith("/")) {
                            finalStr = alias + "=loadMvccTable(\"" + mvccFilePath + "\",\"" + alias + "\");\n";
                        } else {
                            finalStr = alias + "=loadMvccTable(" + "\"" + mvccFilePath + "\",\"" + alias + "\");\n";
                        }

                        stringBuilder.append(finalStr);
                    } else {
                        String finalStr;
                        if (split[0].contains("mvcc") && (!split[1].contains("mvcc:"))) {
                            finalStr = parseOtherPath(str, aliasSet);
                        } else {
                            // contain alias:
                            String alias= split[0];
                            if (str.contains("\\\\")) {
                                // if: 'win \\'
                                String[] tempSplit = split[1].split("://"); // mvcc://C

                                if (aliasSet.contains(alias)) {
                                    throw new RuntimeException("Duplicate table alias found in property tableAlias: " + alias);
                                }
                                aliasSet.add(alias);

                                int lastDoubleSlashIndex = split[2].lastIndexOf("\\\\");
                                String dropTableName = split[2].substring(0, lastDoubleSlashIndex);
                                String tbName = split[2].substring(lastDoubleSlashIndex + 2);

                                finalStr = alias + "=loadMvccTable(\"" + tempSplit[1] + ":" + dropTableName + "\",\"" + tbName + "\");\n";
                            } else {
                                List<String> mvccPathSplit = parseMvccPath(split[1]);
                                String mvccPath = mvccPathSplit.get(1);
                                if (aliasSet.contains(alias)) {
                                    throw new RuntimeException("Duplicate table alias found in property tableAlias: " + alias);
                                }
                                aliasSet.add(alias);
                                String[] pathSplit = mvccPath.split("/");
                                String mvccFilePath = mvccPath.substring(0, mvccPath.lastIndexOf("/"));
                                if (mvccPath.startsWith("/")) {
                                    finalStr = alias + "=loadMvccTable(\"" + mvccFilePath + "\",\"" + pathSplit[pathSplit.length - 1] + "\");\n";
                                } else {
                                    finalStr = alias + "=loadMvccTable(" + "\"" + mvccFilePath + "\",\"" + pathSplit[pathSplit.length - 1] + "\");\n";
                                }
                            }
                        }

                        stringBuilder.append(finalStr);
                    }
                } else {
                    // 3、other
                    String finalStr = parseOtherPath(str, aliasSet);
                    stringBuilder.append(finalStr);
                }
            }
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to parse tableAlias: "+ e.getMessage());
        }

        return stringBuilder.toString();
    }

    public static String parseOtherPath(String str, Set<String> aliasSet) {
        String[] split = str.split(":");
        if (split.length == 0) {
            throw new RuntimeException("The split of str's length is 0.");
        }
        String alias = split[0];
        String memTableName = split[1];
        if (aliasSet.contains(alias)) {
            throw new RuntimeException("Duplicate table alias found in property tableAlias: " + alias);
        }
        aliasSet.add(alias);
        String finalStr = alias + "=" + memTableName + ";\n";

        return finalStr;
    }

    public static List<String> parseMvccPath(String str) {
        Pattern pattern = Pattern.compile("^(.*?://)(.*)");
        Matcher matcher = pattern.matcher(str);
        List<String> result = new ArrayList<>();

        if (matcher.find()) {
            result.add(matcher.group(1));
            result.add(matcher.group(2));
        }

        return result;
    }

    public static String getInsertColumnString(String sql) {
        Pattern pattern = Pattern.compile(Utils.INSERT_STRING + INSERT_TABLE_NAME_COLUMN_STRING + Utils.VALUE_WITH_QUESTION_STRING);
        Matcher matcher = pattern.matcher(sql);

        //(insert)\s+(into)\s+((loadTable\(.+?\))*([a-zA-Z]{1}[a-zA-Z\d_]*)*)\s*(\((.+?)\))*\s+(values)\s*\(([\s?,]+)\)
        //(1     )   (2   )   ((4               ) (5                      ) )    (7 (6  )  )    (8     )     (9      )
        if (matcher.find()) {
            String columnParam = matcher.group(7);
            if (columnParam != null){
                return columnParam;
            }
        }
        return "";
    }

    public static String getInsertValueQuestionString(String sql) {
        Pattern pattern = Pattern.compile(Utils.INSERT_STRING + INSERT_TABLE_NAME_COLUMN_STRING + Utils.VALUE_WITH_QUESTION_STRING);
        //(insert)\s+(into)\s+((loadTable\(.+?\))*([a-zA-Z]{1}[a-zA-Z\d_]*)*)\s*(\((.+?)\))*\s+(values)\s*\(([\s?,]+)\)
        //(1     )   (2)      ((4               ) (5                      ) )   (7 (6  )  )    (8     )     (9      )
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            String columnParam = matcher.group(9);
            if (columnParam != null){
                return columnParam;
            }
        }
        return "";
    }

    public static Map<String, Integer> getInsertColumnParamInSql(String sql){
        Map<String,Integer> map = new HashMap<>();
        String columnParam = getInsertColumnString(sql);
        if (!columnParam.isEmpty()) {
            String[] columnParams = columnParam.split(",");
            for (int i = 0; i < columnParams.length; i++)
                map.put(columnParams[i].trim().toLowerCase(), i);

            return map;
        } else {
            return map;
        }
    }

    public static Map<String, Integer> getDeleteColumnParamInSql(String preProcessedSql) {
        Map<String,Integer> map = new HashMap<>();
        // parse col names in delete sql
        String[] splitSqls = null;
//        splitSqls = preProcessedSql.split("\\s*(?=[!=><]|between|and|or|in|(?<!=)=)\\s*|\\s*(?<=[!=><]|between|and|or|in|=(?!=))\\s*");
        splitSqls = preProcessedSql.split("\\s*(?=[!=><]|(?<!\\w)between\\b|(?<!\\w)and\\b|(?<!\\w)or\\b|(?<!\\w)in\\b(?!\\()|(?<!=)=)\\s*|\\s*(?<=[!=><]|(?<!\\w)between\\b|(?<!\\w)and\\b|(?<!\\w)or\\b|(?<!\\w)in\\b(?!\\()|=(?!=))\\s*");
        List<String> partsList = Arrays.stream(splitSqls)
                .filter(str -> !str.isEmpty())
                .collect(Collectors.toList());

        int indexInDeleteSql = 0;
        for (int i = 0; i < partsList.size(); i++ ) {
            String sqlPart = partsList.get(i);
            sqlPart = sqlPart.trim();
            if (!sqlPart.equals("?") && !sqlPart.equals("=") && !sqlPart.equals("and") && !sqlPart.equals("or") && !sqlPart.equals(">") && !sqlPart.equals("<") && !sqlPart.equals("in") && !sqlPart.equals("!")) {
//                String[] words = sqlPart.split("\\s+");
                String[] words = sqlPart.split("[\\s,]+");
                String lastWord = words[words.length - 1];
//                if (words.length == 1 || lastWord.contains("?") || lastWord.equals("where"))
//                if (lastWord.contains("?") || lastWord.equals("where"))
                if ((!lastWord.equals("?") && (i > 0) && partsList.get(i-1).equals("=") && words.length == 1) || lastWord.contains("?") || lastWord.equals("where"))
                    continue;
                map.put(lastWord.toLowerCase(), indexInDeleteSql);
                indexInDeleteSql ++;
            }
        }

        return map;
    }

    public static void checkInsertSQLValid(String sql, int columnSize){
        String columnParam = getInsertColumnString(sql);
        String[] columnParams = columnParam.split(",");
        String QuestionMark = getInsertValueQuestionString(sql);
        String[] QuestionMarks = QuestionMark.split(",");
        if(columnParam.isEmpty()) {
            if(QuestionMarks.length != columnSize)
                throw new RuntimeException("The number of table columns and the number of values do not match! Please check the SQL!");
        }else {
            if (columnParams.length != QuestionMarks.length) {
                throw new RuntimeException("The number of columns and the number of values do not match! Please check the SQL!");
            }
        }
    }

    public static int transferColDefsTypesToSqlTypes(String type) {
        type = type.replaceAll("\\(.*?\\)", "");
        switch (type){
            case "BOOL":
                return Types.BOOLEAN;
            case "CHAR":
                return Types.CHAR;
            case "SHORT":
                return Types.TINYINT;
            case "INT":
                return Types.INTEGER;
            case "LONG":
                return Types.BIGINT;
            case "DATE":
                return Types.DATE;
            case "TIME":
                return Types.TIME;
            case "DATETIME":
            case "TIMESTAMP":
                return Types.TIMESTAMP;
            case "FLOAT":
                return Types.FLOAT;
            case "DOUBLE":
                return Types.DOUBLE;
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
                return Types.DECIMAL;
            case "STRING":
            case "SYMBOL":
                return Types.VARCHAR;
            case "BLOB":
                return Types.BLOB;
            default:
                return Types.OTHER;
        }
    }
}
