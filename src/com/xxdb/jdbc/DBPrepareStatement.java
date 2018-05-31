package com.xxdb.jdbc;

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

public class DBPrepareStatement extends DBStatement implements PreparedStatement {

    private _DBConnection conn;
    private String sql;
    private String[] sqls;
    private Object[] values;
    private StringBuilder batch;

    public DBPrepareStatement(_DBConnection conn, String sql){
        super(conn);
        this.conn = conn;
        this.sql = sql.trim();
        if(!this.sql.endsWith(";")){
            this.sql = this.sql + ";";
        }
        sqls = this.sql.split("\\?");
        values = new Object[sqls.length+1];
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
    public void setNull(int i, int i1) throws SQLException {
        setObject(i,i1);
    }


    @Override
    public void setBoolean(int parameterIndex, boolean x) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setShort(int parameterIndex, short x) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setInt(int parameterIndex, int x) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setLong(int parameterIndex, long x) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setString(int parameterIndex, String x) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) {

}

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) {

    }

    @Override
    public void clearParameters() throws SQLException {
        for(Object item : values){
            item = null;
        }
    }



    @Override
    public void setObject(int parameterIndex, Object x) {
        if(x instanceof Date){
            values[parameterIndex] = new BasicDate(LocalDate.parse(x.toString()));
        }else if(x instanceof Time){
            values[parameterIndex] = new BasicTime(LocalTime.parse(x.toString()));
        }else if(x instanceof Timestamp){
            values[parameterIndex] = new BasicTimestamp(LocalDateTime.parse(x.toString()));
        }else{
            values[parameterIndex] = x;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {

    }


    @Override
    public boolean execute() throws SQLException {
        String s = createSql();
        return super.execute(s);
    }

    @Override
    public void addBatch() throws SQLException {
        // todo
        batch.append(createSql());

    }

    @Override
    public void clearBatch() throws SQLException {
        batch.delete(0,batch.length());
    }

    @Override
    public int[] executeBatch() throws SQLException {
        //todo 需要修改
        if(super.isClosed()) throw new SQLException("Connection is closed");
        try {
            conn.getDb().run(batch.toString());
            return new int[0];
        }catch (Exception e){
            return new int[0];
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {

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
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) {
        setObject(parameterIndex,x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) {

    }

    @Override
    public void setURL(int parameterIndex, URL x) {

    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) {

    }

    @Override
    public void setNString(int i, String s) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void setNClob(int i, NClob nClob) throws SQLException {

    }

    @Override
    public void setClob(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void setBlob(int i, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void setNClob(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void setSQLXML(int i, SQLXML sqlxml) throws SQLException {

    }


    @Override
    public void setAsciiStream(int i, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void setCharacterStream(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setCharacterStream(int i, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int i, Reader reader) throws SQLException {

    }

    @Override
    public void setClob(int i, Reader reader) throws SQLException {

    }


    @Override
    public void setBlob(int i, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(int i, Reader reader) throws SQLException {

    }

    public void setBasicDate(int parameterIndex, LocalDate date){
        setObject(parameterIndex, date);
    }

    public void setBasicMonth(int parameterIndex, YearMonth yearMonth) {
        setObject(parameterIndex,new BasicMonth(yearMonth));
    }

    public void setBasicMinute(int parameterIndex, LocalTime time){
        setObject(parameterIndex,new BasicMinute(time));
    }

    public void setBasicSecond(int parameterIndex, LocalTime time){
        setObject(parameterIndex,new BasicSecond(time));
    }

    public void setBasicDateTime(int parameterIndex, LocalDateTime time){
        setObject(parameterIndex,new BasicDateTime(time));
    }

    public void setBasicTimestamp(int parameterIndex, LocalDateTime time){
        setObject(parameterIndex, new BasicTimestamp(time));
    }

    public void setBasicNanotime(int parameterIndex, LocalTime time){
        setObject(parameterIndex, new BasicNanoTime(time));
    }

    public void setBasicNanotimestamp(int parameterIndex, LocalDateTime time){
        setObject(parameterIndex, new BasicNanoTimestamp(time));
    }





    private String createSql(){
        StringBuilder sb = new StringBuilder();
        for(int i=1; i<sqls.length; ++i){
            sb.append(sqls[i-1]).append(Utils.java2db(values[i]));
        }
        sb.append(sqls[sqls.length-1]);
        return sb.toString();
    }
}
