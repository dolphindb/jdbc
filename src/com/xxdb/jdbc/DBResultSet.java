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
import java.util.Calendar;
import java.util.Map;

public class DBResultSet implements ResultSet {

    private BasicTable table;
    private Vector vector;
    private int row = -1;
    private int rows;

    private boolean isClosed = false;

    public DBResultSet(Entity Entity){
        this.table = (BasicTable) Entity;
        rows = table.rows();
        System.out.println(table.rows() + "  "+ table.columns());
    }

    @Override
    public boolean next() throws SQLException {
        checkedClose();
        row++;
        return row <= table.rows()-1;
    }

    @Override
    public void close() throws SQLException {
        isClosed = true;
        table = null;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException{
        checkedClose();
        return table.getColumn(columnIndex).get(row).getString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException{
        checkedClose();
        return ((BasicBooleanVector)table.getColumn(columnIndex)).getBoolean(row);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException{
        checkedClose();
        return ((BasicByteVector)table.getColumn(columnIndex)).getByte(row);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkedClose();
        return ((BasicShortVector)table.getColumn(columnIndex)).getShort(row);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkedClose();
        return ((BasicIntVector)table.getColumn(columnIndex)).getInt(row);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkedClose();
        return ((BasicLongVector)table.getColumn(columnIndex)).getLong(row);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkedClose();
        return ((BasicFloatVector)table.getColumn(columnIndex)).getFloat(row);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkedClose();
        return ((BasicDoubleVector)table.getColumn(columnIndex)).getDouble(row);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException{
        checkedClose();
        return null;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkedClose();
        return new byte[0];
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(table.getColumnName(columnIndex));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(table.getColumnName(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(table.getColumnName(columnIndex));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        checkedClose();
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        checkedClose();
        return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        checkedClose();
        return null;
    }

    @Override
    public String getString(String columnLabel) throws SQLException{
        checkedClose();
        return ((BasicStringVector)table.getColumn(columnLabel)).get(row).getString();
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        checkedClose();
        return ((BasicBooleanVector)table.getColumn(columnLabel)).getBoolean(row);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        checkedClose();
        return ((BasicByteVector)table.getColumn(columnLabel)).getByte(row);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        checkedClose();
        return ((BasicShortVector)table.getColumn(columnLabel)).getShort(row);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        checkedClose();
        return ((BasicIntVector)table.getColumn(columnLabel)).getInt(row);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        checkedClose();
        return ((BasicLongVector)table.getColumn(columnLabel)).getLong(row);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        checkedClose();
        return ((BasicFloatVector)table.getColumn(columnLabel)).getFloat(row);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        checkedClose();
        return ((BasicDoubleVector)table.getColumn(columnLabel)).getDouble(row);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException{
        //todo
        checkedClose();
        return null;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        //todo
        checkedClose();
        return new byte[0];
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        checkedClose();
        Scalar scalar = table.getColumn(columnLabel).get(row);
        LocalDate date = null;
        if(scalar instanceof BasicDate){
            date = ((BasicDate) scalar).getDate();
        }
        if (date==null) return null;
        return new Date(date.getYear(),date.getMonthValue(),date.getDayOfMonth());
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {

        checkedClose();
        Scalar scalar = table.getColumn(columnLabel).get(row);
        LocalTime time = null;
        if(scalar instanceof BasicMinute){
            time = ((BasicMinute) scalar).getMinute();
        }else if(scalar instanceof BasicSecond){
            time = ((BasicSecond) scalar).getSecond();
        }else if(scalar instanceof BasicNanoTime){
            time = ((BasicNanoTime) scalar).getNanoTime();
        }
        if (time==null) return null;
        return new Time(time.getHour(),time.getMinute(),time.getSecond());
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        checkedClose();
        Scalar scalar = table.getColumn(columnLabel).get(row);
        LocalDateTime dateTime = null;
        if(scalar instanceof BasicDateTime){
            dateTime = ((BasicDateTime) scalar).getDateTime();
        }else if(scalar instanceof BasicTimestamp){
            dateTime = ((BasicTimestamp) scalar).getTimestamp();
        }else if(scalar instanceof BasicNanoTimestamp){
            dateTime = ((BasicNanoTimestamp) scalar).getNanoTimestamp();
        }
        if (dateTime==null) return null;
        return new Timestamp(dateTime.getYear(),dateTime.getMonthValue(),dateTime.getDayOfMonth(),
                dateTime.getHour(), dateTime.getMinute(),dateTime.getSecond(),dateTime.getNano());
    }

    @Override
    public InputStream getAsciiStream(String s) throws SQLException {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(String s) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(String s) throws SQLException {
        return null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public String getCursorName() throws SQLException {
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int i) throws SQLException {
        checkedClose();
        return table.getColumn(i).get(row);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        checkedClose();
        return table.getColumn(columnLabel).get(row);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkedClose();
        return 0;
    }

    @Override
    public Reader getCharacterStream(int i) throws SQLException {
        checkedClose();
        return null;
    }

    @Override
    public Reader getCharacterStream(String s) throws SQLException {
        checkedClose();
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int i) throws SQLException {
        checkedClose();
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String s) throws SQLException {
        checkedClose();
        return null;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return row < 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return row >= rows;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return row == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        return row == rows-1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        row = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        row = rows;
    }

    @Override
    public boolean first() throws SQLException {
        row =0;
        return rows > 0;
    }

    @Override
    public boolean last() throws SQLException {
        row = rows -1;
        return  rows > 0;
    }

    @Override
    public int getRow() throws SQLException {
        return row+1;
    }

    @Override
    public boolean absolute(int i) throws SQLException {
        row = i-1;
        return row < rows;
    }

    @Override
    public boolean relative(int i) throws SQLException {
        row += i;
        return  row >= 0 && row < rows;
    }

    @Override
    public boolean previous() throws SQLException {
        --row;
        return row>=0;
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
    public int getType() throws SQLException {
        return 0;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int i) throws SQLException {

    }

    @Override
    public void updateBoolean(int i, boolean b) throws SQLException {
        checkedClose();
        ((BasicBooleanVector)table.getColumn(i)).setBoolean(row,b);
    }

    @Override
    public void updateByte(int i, byte b) throws SQLException {
        checkedClose();
        ((BasicByteVector)table.getColumn(i)).setByte(row,b);
    }

    @Override
    public void updateShort(int i, short i1) throws SQLException {
        checkedClose();
        ((BasicShortVector)table.getColumn(i)).setShort(row,i1);
    }

    @Override
    public void updateInt(int i, int i1) throws SQLException {
        checkedClose();
        ((BasicIntVector)table.getColumn(i)).setInt(row,i1);
    }

    @Override
    public void updateLong(int i, long l) throws SQLException {
        checkedClose();
        ((BasicLongVector)table.getColumn(i)).setLong(row,l);
    }

    @Override
    public void updateFloat(int i, float v) throws SQLException {
        checkedClose();
        ((BasicFloatVector)table.getColumn(i)).setFloat(row,v);
    }

    @Override
    public void updateDouble(int i, double v) throws SQLException {
        checkedClose();
        ((BasicDoubleVector)table.getColumn(i)).setDouble(row,v);
    }

    @Override
    public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
        //((BasicShortVector)table.getColumn(i)).setShort(row,i1);
    }

    @Override
    public void updateString(int i, String s) throws SQLException {
        checkedClose();
        ((BasicStringVector)table.getColumn(i)).setString(row,s);
    }

    @Override
    public void updateBytes(int i, byte[] bytes) throws SQLException {

    }

    @Override
    public void updateDate(int i, Date date) throws SQLException {

    }

    @Override
    public void updateTime(int i, Time time) throws SQLException {

    }

    @Override
    public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream, int i1) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream, int i1) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int i, Reader reader, int i1) throws SQLException {

    }

    @Override
    public void updateObject(int i, Object o, int i1) throws SQLException {

    }

    @Override
    public void updateObject(int i, Object o) throws SQLException {

    }

    @Override
    public void updateNull(String s) throws SQLException {

    }

    @Override
    public void updateBoolean(String s, boolean b) throws SQLException {
        checkedClose();
        ((BasicBooleanVector)table.getColumn(s)).setBoolean(row,b);
    }

    @Override
    public void updateByte(String s, byte b) throws SQLException {
        checkedClose();
        ((BasicByteVector)table.getColumn(s)).setByte(row,b);
    }

    @Override
    public void updateShort(String s, short i) throws SQLException {
        checkedClose();
        ((BasicShortVector)table.getColumn(s)).setShort(row,i);
    }

    @Override
    public void updateInt(String s, int i) throws SQLException {
        checkedClose();
        ((BasicIntVector)table.getColumn(s)).setInt(row,i);
    }

    @Override
    public void updateLong(String s, long l) throws SQLException {
        checkedClose();
        ((BasicLongVector)table.getColumn(s)).setLong(row,l);
    }

    @Override
    public void updateFloat(String s, float v) throws SQLException {
        checkedClose();
        ((BasicFloatVector)table.getColumn(s)).setFloat(row,v);
    }

    @Override
    public void updateDouble(String s, double v) throws SQLException {
        checkedClose();
        ((BasicDoubleVector)table.getColumn(s)).setDouble(row,v);
    }

    @Override
    public void updateBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {

    }

    @Override
    public void updateString(String s, String s1) throws SQLException {
        checkedClose();
        ((BasicStringVector)table.getColumn(s)).setString(row,s1);
    }

    @Override
    public void updateBytes(String s, byte[] bytes) throws SQLException {

    }

    @Override
    public void updateDate(String s, Date date) throws SQLException {

    }

    @Override
    public void updateTime(String s, Time time) throws SQLException {

    }

    @Override
    public void updateTimestamp(String s, Timestamp timestamp) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream, int i) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream, int i) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String s, Reader reader, int i) throws SQLException {

    }

    @Override
    public void updateObject(String s, Object o, int i) throws SQLException {

    }

    @Override
    public void updateObject(String s, Object o) throws SQLException {

    }

    @Override
    public void insertRow() throws SQLException {

    }

    @Override
    public void updateRow() throws SQLException {

    }

    @Override
    public void deleteRow() throws SQLException {

    }

    @Override
    public void refreshRow() throws SQLException {

    }

    @Override
    public void cancelRowUpdates() throws SQLException {

    }

    @Override
    public void moveToInsertRow() throws SQLException {

    }

    @Override
    public void moveToCurrentRow() throws SQLException {

    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(int i) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int i) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(int i) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(int i) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String s, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(String s) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String s) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(String s) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(String s) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int i, Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(String s, Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int i, Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String s, Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String s, Calendar calendar) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(int i) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(String s) throws SQLException {
        return null;
    }

    @Override
    public void updateRef(int i, Ref ref) throws SQLException {

    }

    @Override
    public void updateRef(String s, Ref ref) throws SQLException {

    }

    @Override
    public void updateBlob(int i, Blob blob) throws SQLException {

    }

    @Override
    public void updateBlob(String s, Blob blob) throws SQLException {

    }

    @Override
    public void updateClob(int i, Clob clob) throws SQLException {

    }

    @Override
    public void updateClob(String s, Clob clob) throws SQLException {

    }

    @Override
    public void updateArray(int i, Array array) throws SQLException {

    }

    @Override
    public void updateArray(String s, Array array) throws SQLException {

    }

    @Override
    public RowId getRowId(int i) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(String s) throws SQLException {
        return null;
    }

    @Override
    public void updateRowId(int i, RowId rowId) throws SQLException {

    }

    @Override
    public void updateRowId(String s, RowId rowId) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void updateNString(int i, String s) throws SQLException {

    }

    @Override
    public void updateNString(String s, String s1) throws SQLException {

    }

    @Override
    public void updateNClob(int i, NClob nClob) throws SQLException {

    }

    @Override
    public void updateNClob(String s, NClob nClob) throws SQLException {

    }

    @Override
    public NClob getNClob(int i) throws SQLException {
        return null;
    }

    @Override
    public NClob getNClob(String s) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int i) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String s) throws SQLException {
        return null;
    }

    @Override
    public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {

    }

    @Override
    public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {

    }

    @Override
    public String getNString(int i) throws SQLException {
        return null;
    }

    @Override
    public String getNString(String s) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int i) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String s) throws SQLException {
        return null;
    }

    @Override
    public void updateNCharacterStream(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String s, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String s, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateBlob(int i, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void updateBlob(String s, InputStream inputStream, long l) throws SQLException {

    }

    @Override
    public void updateClob(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateClob(String s, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateNClob(int i, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateNClob(String s, Reader reader, long l) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(int i, Reader reader) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String s, Reader reader) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int i, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int i, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int i, Reader reader) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String s, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String s, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String s, Reader reader) throws SQLException {

    }

    @Override
    public void updateBlob(int i, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBlob(String s, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateClob(int i, Reader reader) throws SQLException {

    }

    @Override
    public void updateClob(String s, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(int i, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(String s, Reader reader) throws SQLException {

    }

    @Override
    public <T> T getObject(int i, Class<T> aClass) throws SQLException {
        return null;
    }

    @Override
    public <T> T getObject(String s, Class<T> aClass) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return false;
    }

    public BasicDate getBasicDate(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicDate)table.getColumn(columnLabel).get(row);
    }

    public BasicMonth getBasicMonth(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicMonth)table.getColumn(columnLabel).get(row);
    }

    public BasicTime getBasicTime(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicTime)table.getColumn(columnLabel).get(row);
    }

    public BasicMinute getBasicMinute(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicMinute)table.getColumn(columnLabel).get(row);
    }

    public BasicSecond getBasicSecond(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicSecond)table.getColumn(columnLabel).get(row);
    }

    public BasicDateTime getBasicDateTime(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicDateTime)table.getColumn(columnLabel).get(row);
    }

    public BasicNanoTime getBasicNanoTime(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicNanoTime) table.getColumn(columnLabel).get(row);
    }

    public BasicNanoTimestamp getBasicNanoTimestamp(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicNanoTimestamp) table.getColumn(columnLabel).get(row);
    }

    public BasicDate getBasicDate(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicDate)table.getColumn(columnIndex).get(row);
    }

    public BasicMonth getBasicMonth(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicMonth)table.getColumn(columnIndex).get(row);
    }

    public BasicTime getBasicTime(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicTime)table.getColumn(columnIndex).get(row);
    }

    public BasicMinute getBasicMinute(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicMinute)table.getColumn(columnIndex).get(row);
    }

    public BasicSecond getBasicSecond(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicSecond)table.getColumn(columnIndex).get(row);
    }

    public BasicDateTime getBasicDateTime(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicDateTime)table.getColumn(columnIndex).get(row);
    }

    public BasicNanoTime getBasicNanoTime(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicNanoTime) table.getColumn(columnIndex).get(row);
    }

    public BasicNanoTimestamp getBasicNanoTimestamp(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicNanoTimestamp) table.getColumn(columnIndex).get(row);
    }




//    private <T> T get(int index, Class<T> tClass) throws SQLException{
//        checkedClose();
//        if(tClass.isInstance(Boolean.class)){
//            return ((BasicBooleanVector)table.getColumn(index)).getBoolean(row);
//        }else if(tClass.isInstance(Byte.class)){
//            return ((BasicByteVector)table.getColumn(index)).getByte(row);
//        }else if(tClass.isInstance(Integer.class)){
//            return ((BasicIntVector)table.getColumn(index)).getInt(row);
//        }else if(tClass.isInstance(Short.class)){
//            return ((BasicShortVector)table.getColumn(index)).getShort(row);
//        }else if(tClass.isInstance(Long.class)){
//            return ((BasicLongVector)table.getColumn(index)).getLong(row);
//        }else if(tClass.isInstance(Float.class)){
//            return ((BasicFloatVector)table.getColumn(index)).getFloat(row);
//        }else if(tClass.isInstance(Double.class)){
//            return ((BasicDoubleVector)table.getColumn(index)).getDouble(row);
//        }else if(tClass.isInstance(String.class)){
//            return ((BasicStringVector)table.getColumn(index)).getString(row);
//        }
//    }


    private void update(int index, Object value) throws SQLException{
        checkedClose();
        if(value instanceof Boolean){
            ((BasicBooleanVector)table.getColumn(index)).setBoolean(row,(boolean)value);
        }else if(value instanceof Byte){
            ((BasicByteVector)table.getColumn(index)).setByte(row,(byte)value);
        }else if(value instanceof Integer){
            ((BasicIntVector)table.getColumn(index)).setInt(row,(int)value);
        }else if(value instanceof Short){
            ((BasicShortVector)table.getColumn(index)).setShort(row,(short) value);
        }else if(value instanceof Long){
            ((BasicLongVector)table.getColumn(index)).setLong(row,(long) value);
        }else if(value instanceof Float){
            ((BasicFloatVector)table.getColumn(index)).setFloat(row,(float) value);
        }else if(value instanceof Double){
            ((BasicDoubleVector)table.getColumn(index)).setDouble(row,(double) value);
        }else if(value instanceof String){
            ((BasicStringVector)table.getColumn(index)).setString(row,(String) value);
        }
    }

    private void update(String name, Object value) throws SQLException{
        update(findColumn(name),value);
    }


    private void checkedClose() throws SQLException{
        if(table == null && isClosed){
            throw new SQLException("ResultSet is closed");
        }
    }
}
