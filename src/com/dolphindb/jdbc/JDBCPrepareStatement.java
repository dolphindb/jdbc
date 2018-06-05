package com.dolphindb.jdbc;

import com.xxdb.data.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.Calendar;

public class JDBCPrepareStatement extends JDBCStatement implements PreparedStatement {

    public JDBCPrepareStatement(JDBCConnection connection, String sql){
        super(connection);
        this.connection = connection;
        this.sql = sql.trim();
        sqlSplit = this.sql.split("\\?");
        values = new Object[sqlSplit.length+1];
        batch = new StringBuilder();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(createSql());
    }

    @Override
    public int executeUpdate() throws SQLException {
         return super.executeUpdate(createSql());
    }

    @Override
    public void setNull(int parameterIndex, int x) throws SQLException {
        super.checkClosed();
        setObject(parameterIndex,x);
    }


    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        super.checkClosed();
        setObject(parameterIndex,x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal bigDecimal) throws SQLException {
        super.checkClosed();
        setObject(parameterIndex,bigDecimal);
    }

    @Override
    public void setString(int parameterIndex, String s) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,s);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] bytes) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,bytes);
    }

    @Override
    public void setDate(int parameterIndex, Date date) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,date);
    }

    @Override
    public void setTime(int parameterIndex, Time time) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,time);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp timestamp) throws SQLException{
        super.checkClosed();
        setObject(parameterIndex,timestamp);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream inputStream, int length) throws SQLException{

}

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream inputStream, int length) throws SQLException{

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream inputStream, int length) throws SQLException{

    }

    @Override
    public void clearParameters() throws SQLException {
        super.clearBatch();
        for(Object item : values){
            item = null;
        }
    }



    @Override
    public void setObject(int parameterIndex, Object object) throws SQLException{
        super.checkClosed();
        if(object instanceof Date){
            values[parameterIndex] = new BasicDate(LocalDate.parse(object.toString()));
        }else if(object instanceof Time){
            values[parameterIndex] = new BasicTime(LocalTime.parse(object.toString()));
        }else if(object instanceof Timestamp){
            values[parameterIndex] = new BasicTimestamp(LocalDateTime.parse(object.toString()));
        }else{
            values[parameterIndex] = object;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object object, int targetSqlType) throws SQLException {
        setObject(parameterIndex,object);
    }

    @Override
    public void setObject(int parameterIndex, Object object, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex,object);
    }


    @Override
    public boolean execute() throws SQLException {
        super.checkClosed();
        String s = createSql();
        return super.execute(s);
    }

    @Override
    public void addBatch() throws SQLException {
        super.checkClosed();
        batch.append(createSql()).append(";\n");

    }

    @Override
    public void clearBatch() throws SQLException {
        super.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return super.executeBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

    }

    @Override
    public void setRef(int parameterIndex, Ref ref) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, Blob blob) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Clob clob) throws SQLException {

    }

    @Override
    public void setArray(int parameterIndex, Array array) throws SQLException {

    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return resultSet.getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date date, Calendar cal) throws SQLException{
        setObject(parameterIndex,date);
    }

    @Override
    public void setTime(int parameterIndex, Time time, Calendar cal) throws SQLException{
        setObject(parameterIndex,time);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp timestamp, Calendar cal) throws SQLException{
        setObject(parameterIndex,timestamp);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException{

    }

    @Override
    public void setURL(int parameterIndex, URL url) throws SQLException{

    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId rowId) throws SQLException{

    }

    @Override
    public void setNString(int parameterIndex, String s) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader, long l) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, NClob nClob) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long l) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long l) throws SQLException {

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML sqlxml) throws SQLException {

    }


    @Override
    public void setAsciiStream(int parameterIndex, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long l) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

    }


    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

    }

    public void setBasicDate(int parameterIndex, LocalDate date) throws SQLException{
        setObject(parameterIndex, date);
    }

    public void setBasicMonth(int parameterIndex, YearMonth yearMonth) throws SQLException{
        setObject(parameterIndex,new BasicMonth(yearMonth));
    }

    public void setBasicMinute(int parameterIndex, LocalTime time) throws SQLException{
        setObject(parameterIndex,new BasicMinute(time));
    }

    public void setBasicSecond(int parameterIndex, LocalTime time) throws SQLException{
        setObject(parameterIndex,new BasicSecond(time));
    }

    public void setBasicDateTime(int parameterIndex, LocalDateTime time) throws SQLException{
        setObject(parameterIndex,new BasicDateTime(time));
    }

    public void setBasicTimestamp(int parameterIndex, LocalDateTime time) throws SQLException{
        setObject(parameterIndex, new BasicTimestamp(time));
    }

    public void setBasicNanotime(int parameterIndex, LocalTime time) throws SQLException{
        setObject(parameterIndex, new BasicNanoTime(time));
    }

    public void setBasicNanotimestamp(int parameterIndex, LocalDateTime time) throws SQLException{
        setObject(parameterIndex, new BasicNanoTimestamp(time));
    }





    private String createSql(){
        StringBuilder sb = new StringBuilder();
        for(int i = 1; i< sqlSplit.length; ++i){
            sb.append(sqlSplit[i-1]).append(Utils.java2db(values[i]));
        }
        sb.append(sqlSplit[sqlSplit.length-1]);
        return sb.toString();
    }
}
