package com.dolphindb.jdbc;

import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;

import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;

public class JDBCStatement implements Statement {

    protected JDBCConnection connection;
    protected ResultSet resultSet;
    protected String[] sqlSplit;
    protected Object[] values;
    protected StringBuilder batch;
    protected Queue<Object> objectQueue;
    protected Object result;
    protected Deque<ResultSet> resultSets;
    protected HashMap<String,String> tableTypes;
    protected static final String IN_MEMORY_TABLE = "IN-MEMORY TABLE";
    protected boolean isClosed;


    public JDBCStatement(JDBCConnection cnn){
        this.connection = cnn;
        objectQueue = new LinkedList<>();
        resultSets = new LinkedList<>();
    }

    private String getTableType(String tableName) throws SQLException{
        if (tableTypes == null) {
            tableTypes = new LinkedHashMap<>();
        }
        String tableType = tableTypes.get(tableName);
        if (tableType == null) {
            try {
                tableType = connection.run("typestr " + tableName).getString();
            } catch (IOException e) {
                throw new SQLException(e);
            }
            tableTypes.put(tableName, tableType);
            return tableType;
        }else{
            return tableType;
        }
    }


    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        sql = sql.trim();
        String[] strings = sql.split(";");
        if(strings.length == 0){
            throw new SQLException("SQL was empty");
        }else if(strings.length == 2){
            throw new SQLException("check the SQL " + sql);
        }
        sql = strings[0];
        int dml = Utils.getDml(sql);
        Entity entity;
        switch (dml){
            case Utils.DML_INSERT:
            case Utils.DML_UPDATE:
            case Utils.DML_DELETE:
                throw new SQLException("the given SQL statement produces anything other than a single ResultSet object");
            case Utils.DML_SELECT:
                try {
                    entity = connection.run(sql);
                    if(entity instanceof BasicTable){
                        resultSet = new JDBCResultSet(connection, this, entity, sql);
                        return resultSet;
                    }else{
                        throw new SQLException("the given SQL statement produces anything other than a single ResultSet object");
                    }
                }catch (IOException e){
                    throw new SQLException(e);
                }
            default:
                throw new SQLException("the given SQL statement produces anything other than a single ResultSet object");
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        sql = sql.trim();
        String[] strings = sql.split(";");
        if(strings.length == 0){
            throw new SQLException("SQL was empty");
        }else if(strings.length == 2){
            throw new SQLException("check the SQL " + sql);
        }
        sql = strings[0];
        String tableName = Utils.getTableName(sql);
        int dml = Utils.getDml(sql);
        String tableType;
        switch (dml) {
            case Utils.DML_INSERT:
                if (tableName != null) {
                    tableType = getTableType(tableName);
                    if (tableType.equals(IN_MEMORY_TABLE)) {
                        try {
                            return tableInsert(tableName, sql).getInt();
                        } catch (IOException e) {
                            throw new SQLException(e);
                        }
                    } else {
                        String[] values;
                        int index = sql.indexOf(";");
                        if (index == -1) {
                            values = sql.substring(sql.indexOf("values") + "values".length()).replaceAll("\\(|\\)", "").split(",");
                        } else {
                            values = sql.substring(sql.indexOf("values") + "values".length(), index).replaceAll("\\(|\\)", "").split(",");
                        }
                        StringBuilder sqlSb = new StringBuilder("append!(").append(tableName).append(",").append("table(");

                        char name = 'A';
                        int colIndex = 0;
                        for (String value : values) {
                            sqlSb.append(value).append(" as ").append((char) (name + colIndex)).append("_,");
                            colIndex++;
                        }
                        sqlSb.delete(sqlSb.length() - ",".length(), sqlSb.length());
                        sqlSb.append("))");
                        try {
                            connection.run(sqlSb.toString());
                            return SUCCESS_NO_INFO;
                        } catch (IOException e) {
                            new SQLException(e);
                        }
                    }
                } else {
                    throw new SQLException("check the SQL " + sql);
                }

            case Utils.DML_UPDATE:
            case Utils.DML_DELETE:
                if (tableName != null) {
                    tableType = getTableType(tableName);
                    if (tableType.equals(IN_MEMORY_TABLE)) {
                        try {
                            connection.run(sql);
                            return SUCCESS_NO_INFO;
                        } catch (IOException e) {
                            throw new SQLException(e);
                        }
                    } else {
                        throw new SQLException("only local in-memory table can update");
                    }
                } else {
                    throw new SQLException("check the Query " + sql);
                }
            case Utils.DML_SELECT:
                throw new SQLException("Can not issue SELECT via executeUpdate()");
            default:
                Entity entity;
                try {
                    entity = connection.run(sql);
                }catch (IOException e){
                    throw new SQLException(e);
                }
                if(entity instanceof BasicTable){
                    throw new SQLException("Can not produces ResultSet");
                }
                return 0;
        }
    }

    @Override
    public void close() throws SQLException {
        checkClosed();
        isClosed = true;
        sqlSplit = null;
        values = null;
        batch = null;
        result = null;
        if(objectQueue != null){
            objectQueue.clear();
            objectQueue = null;
        }

        if(resultSet != null) {
            resultSet.close();
            objectQueue = null;
        }

        if(tableTypes != null){
            tableTypes.clear();
            tableTypes = null;
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int maxFieldSize) throws SQLException {
        Driver.unused();
    }

    @Override
    public int getMaxRows() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public void setMaxRows(int maxRows) throws SQLException {
        Driver.unused();
    }

    @Override
    public void setEscapeProcessing(boolean b) throws SQLException {
        Driver.unused();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public void setQueryTimeout(int queryTimeout) throws SQLException {
        Driver.unused();
    }

    @Override
    public void cancel() throws SQLException {
        Driver.unused();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        Driver.unused();
    }

    @Override
    public void setCursorName(String cursorName) throws SQLException {
        Driver.unused();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        sql = sql.trim();
        String[] strings = sql.split(";");
        if(strings.length == 0){
            throw new SQLException("SQL was empty");
        }else if(strings.length == 2){
            throw new SQLException("check the SQL " + sql);
        }
        sql = strings[0];
        int dml = Utils.getDml(sql);
        switch (dml){
            case Utils.DML_SELECT: {
                ResultSet resultSet_ = executeQuery(sql);
                resultSets.offerLast(resultSet_);
                objectQueue.offer(executeQuery(sql));
            }
            case Utils.DML_INSERT:
            case Utils.DML_UPDATE:
            case Utils.DML_DELETE:
                objectQueue.offer(executeUpdate(sql));
                break;
            default: {
                Entity entity;
                try {
                    entity = connection.run(sql);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
                if (entity instanceof BasicTable) {
                    ResultSet resultSet_ = new JDBCResultSet(connection, this, entity, sql);
                    resultSets.offerLast(resultSet_);
                    objectQueue.offer(resultSet_);
                }
            }
        }

        if(objectQueue.isEmpty()){
            return false;
        }else {
            result = objectQueue.poll();
            if(result instanceof ResultSet){
                return true;
            }else{
                return false;
            }
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        if(result == null) {
            resultSet = null;
        }else if(result instanceof ResultSet){
            resultSet = (JDBCResultSet) result;
        }else{
            resultSet = null;
        }
        result = null;
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        int updateCount;
        if(result == null) {
            updateCount = -1;
        }else if(result instanceof Integer){
            updateCount =  (int)result;
        }else{
            updateCount = -1;
        }
        result = null;
        return updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        while (!resultSets.isEmpty()){
            ResultSet resultSet_ = resultSets.pollFirst();
            if(resultSet_ != null){
                resultSet_.close();
            }
        }

        if(!objectQueue.isEmpty()){
            result = objectQueue.poll();
            if(result instanceof ResultSet){
                return true;
            }else{
                return false;
            }
        }else{
            return false;
        }
    }

    @Override
    public void setFetchDirection(int fetchDirection) throws SQLException {
        Driver.unused();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public void setFetchSize(int fetchSize) throws SQLException {
        Driver.unused();
    }

    @Override
    public int getFetchSize() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        batch.append(sql).append(";\n");
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        batch.delete(0,batch.length());
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        String[] strings = batch.toString().split(";");
        int[] arr_int = new int[strings.length];
        int index = 0;
        try {
            for(String item : strings){
                arr_int[index] = executeUpdate(item);
                ++index;
            }
            batch.delete(0,batch.length());
            return arr_int;
        }catch (Exception e){
            batch.delete(0,batch.length());
            throw new BatchUpdateException(e.getMessage(),Arrays.copyOf(arr_int,index));
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }


    @Override
    public boolean getMoreResults(int current) throws SQLException{
        checkClosed();
        switch (current){
            case Statement.CLOSE_ALL_RESULTS:
                while (!resultSets.isEmpty()){
                    ResultSet resultSet_ = resultSets.pollLast();
                    if(resultSet_ != null){
                        resultSet_.close();
                    }
                }
                break;
            case Statement.CLOSE_CURRENT_RESULT:
                if(resultSet != null){
                    resultSet.close();
                }
                break;
            case Statement.KEEP_CURRENT_RESULT:
                break;
                default:
                throw new SQLException("the argument supplied is not one of the following:\n" +
                        "Statement.CLOSE_CURRENT_RESULT,\n" +
                        "Statement.KEEP_CURRENT_RESULT or\n" +
                        " Statement.CLOSE_ALL_RESULTS");
        }
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException{
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException{
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException{
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException{
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException{
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException{
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        Driver.unused();
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public void setPoolable(boolean b) throws SQLException {
        Driver.unused();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        Driver.unused();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        Driver.unused();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        Driver.unused();
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        checkClosed();
        return aClass.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        checkClosed();
        return aClass.isInstance(this);
    }

    protected BasicInt tableInsert(String tableName, String sql) throws IOException{
        if(sql.startsWith("tableInsert")){
            return (BasicInt)connection.run(sql);
        }else {
            String values;
            int index = sql.indexOf(";");
            if (index == -1) {
                values = sql.substring(sql.indexOf("values") + "values".length());
            } else {
                values = sql.substring(sql.indexOf("values") + "values".length(), index);
            }

            String new_sql = MessageFormat.format("tableInsert({0},{1})", tableName, values);
            BasicInt n = (BasicInt) connection.run(new_sql);
            return n;
        }
    }

    protected void checkClosed() throws SQLException{
        if(isClosed()){
            throw new SQLException("Statement is closed");
        }
        if(connection == null||connection.isClosed()){
            throw new SQLException("Connection is closed");
        }
    }
}
