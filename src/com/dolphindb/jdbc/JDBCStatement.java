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
        System.out.println("JDBCStatement.executeQuery : " + sql);
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
        System.out.println("JDBCStatement.executeUpdate : " + sql);
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

                        String colName = "col";
                        int colIndex = 1;
                        for (String value : values) {
                            sqlSb.append(value).append(" as ").append(colName+colIndex).append(",");
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
        System.out.println("JDBCStatement.close");
        checkClosed();
        isClosed = true;
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
        System.out.println("JDBCStatement.execute : " + sql);
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
                objectQueue.offer(resultSet_);
            }
            break;
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
        System.out.println("JDBCStatement.getResultSet");
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
        System.out.println("JDBCStatement.getUpdateCount");
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
        System.out.println("JDBCStatement.getMoreResults");
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
        System.out.println("JDBCStatement.setFetchDirection");
        Driver.unused();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        System.out.println("JDBCStatement.getFetchDirection");
        Driver.unused();
        return 0;
    }

    @Override
    public void setFetchSize(int fetchSize) throws SQLException {
        System.out.println("JDBCStatement.setFetchSize");
        Driver.unused();
    }

    @Override
    public int getFetchSize() throws SQLException {
        System.out.println("JDBCStatement.getFetchSize");
        Driver.unused();
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        System.out.println("JDBCStatement.getResultSetConcurrency");
        Driver.unused();
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        System.out.println("JDBCStatement.getResultSetType");
        Driver.unused();
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        System.out.println("JDBCStatement.addBatch : " + sql);
        checkClosed();
        batch.append(sql).append(";\n");
    }

    @Override
    public void clearBatch() throws SQLException {
        System.out.println("JDBCStatement.clearBatch");
        checkClosed();
        batch.delete(0,batch.length());
    }

    @Override
    public int[] executeBatch() throws SQLException {
        System.out.println("JDBCStatement.executeBatch");
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
        }catch (SQLException e){
            batch.delete(0,batch.length());
            throw new BatchUpdateException(e.getMessage(),Arrays.copyOf(arr_int,index));
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        System.out.println("JDBCStatement.getConnection");
        checkClosed();
        return connection;
    }


    @Override
    public boolean getMoreResults(int current) throws SQLException{
        System.out.println("JDBCStatement.getMoreResults");
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
        System.out.println("JDBCStatement.getGeneratedKeys");
        Driver.unused();
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException{
        System.out.println("JDBCStatement.executeUpdate.autoGeneratedKeys");
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException{
        System.out.println("JDBCStatement.executeUpdate.columnIndexes");
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException{
        System.out.println("JDBCStatement.executeUpdate.columnNames");
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException{
        System.out.println("JDBCStatement.executeUpdate.autoGeneratedKeys");
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException{
        System.out.println("JDBCStatement.executeUpdate.columnIndexes");
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException{
        System.out.println("JDBCStatement.executeUpdate.execute");
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        System.out.println("JDBCStatement.executeUpdate.getResultSetHoldability");
        Driver.unused();
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        System.out.println("JDBCStatement.executeUpdate.isClosed : " + isClosed);
        return isClosed;
    }

    @Override
    public void setPoolable(boolean b) throws SQLException {
        System.out.println("JDBCStatement.executeUpdate.setPoolable");
        Driver.unused();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        System.out.println("JDBCStatement.executeUpdate.isPoolable");
        Driver.unused();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        System.out.println("JDBCStatement.executeUpdate.closeOnCompletion");
        Driver.unused();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        System.out.println("JDBCStatement.executeUpdate.isCloseOnCompletion");
        Driver.unused();
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        System.out.println("JDBCStatement.executeUpdate.unwrap");
        checkClosed();
        return aClass.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        System.out.println("JDBCStatement.executeUpdate.isWrapperFor");
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
        System.out.println("JDBCStatement.checkClosed");
        if(isClosed()){
            throw new SQLException("Statement is closed");
        }
        if(connection == null||connection.isClosed()){
            throw new SQLException("Connection is closed");
        }
        System.out.println("checkClosed pass");
    }
}
