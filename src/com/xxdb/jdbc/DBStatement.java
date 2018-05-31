package com.xxdb.jdbc;

import com.xxdb.data.Entity;
import com.xxdb.data.Table;

import java.io.IOException;
import java.sql.*;

public class DBStatement implements Statement {

    private _DBConnection connection;

    private ResultSet resultSet;

    private String select = "select * from ";


    public DBStatement(_DBConnection cnn){
        this.connection = cnn;
        select += cnn.getTableName();
    }


    @Override
    public ResultSet executeQuery(String s) throws SQLException {
        try {
            if(s.trim().startsWith("update")) {
                connection.getDb().run(s);
                resultSet = new DBResultSet(connection,connection.getDb().run(select),select);
            }else {
                resultSet = new DBResultSet(connection, connection.getDb().run(s), s);
            }
            return resultSet;
        }catch (IOException e){
            e.printStackTrace();
            throw new SQLException(e.getMessage());
        }

    }

    @Override
    public int executeUpdate(String s) throws SQLException {
        try {
             connection.getDb().run(s);
             resultSet = new DBResultSet(connection,connection.getDb().run(select),select);
             return 1;
        }catch (Exception e){
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int i) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int i) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean b) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int i) throws SQLException {

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
    public void setCursorName(String s) throws SQLException {

    }

    @Override
    public boolean execute(String s) throws SQLException {
        try {
            Entity entity = connection.getDb().run(s);
            if(entity instanceof Table){
                resultSet = new DBResultSet(connection,entity,s);
            }else {
                resultSet = new DBResultSet(connection,connection.getDb().run(select), select);
            }
            return true;
        }catch (IOException e){
            e.printStackTrace();
            throw new SQLException("can not execute "+ s);
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if(resultSet == null){
            throw new SQLException("resultSet is null");
        }
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int i) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int i) throws SQLException {

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
    public void addBatch(String s) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
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
    public int executeUpdate(String s, int i) throws SQLException {
        return executeUpdate(s);
    }

    @Override
    public int executeUpdate(String s, int[] ints) throws SQLException {
        return executeUpdate(s);
    }

    @Override
    public int executeUpdate(String s, String[] strings) throws SQLException {
        return executeUpdate(s);
    }

    @Override
    public boolean execute(String s, int i) throws SQLException {
        return execute(s);
    }

    @Override
    public boolean execute(String s, int[] ints) throws SQLException {
        return execute(s);
    }

    @Override
    public boolean execute(String s, String[] strings) throws SQLException {
        return execute(s);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connection.isClosed();
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
}
