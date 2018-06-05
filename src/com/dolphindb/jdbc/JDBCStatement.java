package com.dolphindb.jdbc;

import com.xxdb.data.BasicInt;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import com.xxdb.data.Void;

import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;

public class JDBCStatement implements Statement {

    protected JDBCConnection connection;

    protected ResultSet resultSet;

    protected String sql;
    protected String[] sqlSplit;
    protected Object[] values;
    protected StringBuilder batch;
    protected Queue<Object> objectQueue;
    protected  Object result;

    protected boolean isClosed;


    public JDBCStatement(JDBCConnection cnn){
        this.connection = cnn;
        objectQueue = new LinkedList<>();
    }


    @Override
    public ResultSet executeQuery(String s) throws SQLException {
        checkClosed();
        try {
            Entity entity = connection.getDbConnection().run(s);
            if(entity instanceof BasicTable){
                resultSet = new JDBCResultSet(connection, this, entity, s);
                return resultSet;
            }else{
                throw new SQLException("executeQuery can not create other than ResultSet");
            }
        }catch (IOException e){
            e.printStackTrace();
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public int executeUpdate(String s) throws SQLException {
        checkClosed();
        try {
             s = s.trim();
             if(s.startsWith("tableInsert")){
                 return tableInsert(s).getInt();
             }else{
                Entity entity = connection.getDbConnection().run(s);
                if(entity instanceof Void){
                    return 0;
                }else if(entity instanceof BasicTable){
                    throw new SQLException("executeUpdate can not create ResultSet");
                }else{
                    // todo 等待 update delete api 返回更新行数
                    return 0;
                }
            }
        }catch (Exception e){
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        checkClosed();
        isClosed = true;
        sql = null;
        sqlSplit = null;
        values = null;
        batch = null;
        result = null;
        if(objectQueue != null){
            objectQueue.clear();
        }
        objectQueue = null;
        if(resultSet != null) {
            resultSet.close();
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int maxFieldSize) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int maxRows) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean b) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int queryTimeout) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String cursorName) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        try {
            String[] strings = sql.split(";");
            System.out.println(strings.length);
            for(String item : strings){
                if(item.length()>0){
                    System.out.println(item);
                    if(item.startsWith("insert") || item.startsWith("tableInsert")){
                        objectQueue.offer(tableInsert(item).getInt());
                    }else if(item.startsWith("update")||item.startsWith("delete")){
                        //todo 获取更新计数
                        connection.getDbConnection().run(item);
                    }else{
                        Entity entity = connection.getDbConnection().run(item);
                        if(entity instanceof  BasicTable){
                            objectQueue.offer(new JDBCResultSet(connection,this,entity,item));
                        }
                    }
                }
            }
            if(objectQueue.isEmpty()){
                return false;
            }else {
                System.out.println(objectQueue.size());
                result = objectQueue.poll();
                if(result instanceof ResultSet){
                    return true;
                }else{
                    return false;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            throw new SQLException(e);
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
        if(resultSet != null) {
            resultSet.close();
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

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int fetchSize) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        batch.append(sql).append("\n");
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        batch.delete(0,batch.length());
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        List<Integer> int_list = new ArrayList<>();
        try {
            String[] strings = batch.toString().split(";");
            System.out.println(strings.length);
            for(String item : strings){
                if(item.length()>0){
                    System.out.println(item);
                    if(item.startsWith("insert") || item.startsWith("tableInsert")){
                        int_list.add(tableInsert(item).getInt());
                    }else if(item.startsWith("update")||item.startsWith("delete")){
                        //todo 获取更新计数
                        connection.getDbConnection().run(item);
                    }else{
                        Entity entity = connection.getDbConnection().run(item);
                        if(entity instanceof  BasicTable){
                            int size = int_list.size();
                            int[] arr_int = new int[size];
                            for(int i=0; i<size; ++i){
                                arr_int[i] = int_list.get(i);
                            }
                            throw new BatchUpdateException("can not return ResultSet",arr_int);
                        }
                    }
                }
            }
            int size = int_list.size();
            int[] arr_int = new int[size];
            for(int i=0; i<size; ++i){
                arr_int[i] = int_list.get(i);
            }
            return arr_int;
        }catch (Exception e){
            throw new SQLException(e);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }

    @Override
    public boolean getMoreResults(int i) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int i) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] ints) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] strings) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int i) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] ints) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] strings) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public void setPoolable(boolean b) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return false;
    }

    protected BasicInt tableInsert(String sql) throws Exception{
        if(sql.startsWith("tableInsert")){
            return (BasicInt)connection.getDbConnection().run(sql);
        }else {
            String tableName = sql.substring(sql.indexOf("into") + "into".length(), sql.indexOf("values"));
            String values;
            int index = sql.indexOf(";");
            if (index == -1) {
                values = sql.substring(sql.indexOf("values") + "values".length());
            } else {
                values = sql.substring(sql.indexOf("values") + "values".length(), sql.indexOf(";"));
            }

            String new_sql = MessageFormat.format("tableInsert({0},{1})", tableName, values);
            BasicInt n = (BasicInt) connection.getDbConnection().run(new_sql);
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
