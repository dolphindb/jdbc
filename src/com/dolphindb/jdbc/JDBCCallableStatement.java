package com.dolphindb.jdbc;

import com.xxdb.data.BasicString;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import org.w3c.dom.ls.LSOutput;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class JDBCCallableStatement extends JDBCPrepareStatement implements CallableStatement {

    private String[] strings;
    private String preSql;

    private Deque<ResultSet> resultSets;

    private ResultSet resultSet;


    public JDBCCallableStatement(JDBCConnection connection, String sql) throws SQLException {
        super(connection,sql);
        sql = Utils.changeCase(sql);
        this.connection = connection;
        this.preSql = sql.trim();
        while (preSql.endsWith(";"))
            preSql = preSql.substring(0, preSql.length() - 1);
        String[] strings = preSql.split(";");
        this.strings = strings;
        resultSets = new LinkedList<>();
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {

    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {

    }

    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        return false;
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        return 0;
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        return 0;
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        return 0;
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        return 0;
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        return 0;
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return null;
    }


    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        return new byte[0];
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {

    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {

    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {

    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {

    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {

    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {

    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {

    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {

    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {

    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {

    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {

    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {

    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {

    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {

    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {

    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {

    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {

    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {

    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {

    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {

    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {

    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {

    }

    @Override
    public ResultSet executeQuery() throws SQLException{
        resultSets.offerLast(super.executeQuery());
        return super.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException{
        return super.executeUpdate();
    }

    @Override
    public boolean execute() throws SQLException{
        boolean allowMultiQueries = Boolean.valueOf(connection.getClientInfo("allowMultiQueries"));
        boolean returnResult = false;
        boolean hasResultSet;
        if(allowMultiQueries == false){
            if(strings.length >= 2){
               throw new SQLException("check the SQl " + preSql);
            }else{
                returnResult = execute(strings[0]);
                if(returnResult){
                    resultSets.offerLast(super.getResultSet());
                }
                return returnResult;
            }
        } else{
            for(int i = 0; i < strings.length; i++){
                if(i == 0){
                    returnResult = execute(strings[i]);
                    hasResultSet = returnResult;
                }else {
                    hasResultSet = execute(strings[i]);
                }
                if(hasResultSet){
                    resultSets.offerLast(super.getResultSet());
                }
            }
            return returnResult;
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if(!resultSets.isEmpty()){
            return resultSets.poll();
        }
        return null;

    }


    @Override
    public boolean getMoreResults() throws SQLException{
        if (!resultSets.isEmpty()){
            return true;
        }
        return false;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException{
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
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {

    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {

    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {

    }

    @Override
    public String getString(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return false;
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        return 0;
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        return 0;
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        return 0;
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        return 0;
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        return 0;
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        return 0;
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return new byte[0];
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {

    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {

    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {

    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {

    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {

    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        return null;
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {

    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {

    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {

    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {

    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {

    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {

    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        return null;
    }
}
