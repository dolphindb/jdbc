package com.xxdb.jdbc;

import com.xxdb.data.*;
import com.xxdb.data.Vector;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.*;

/**
 * DBResultSet 的操作只适合 Vector 和 table, 否则操作会报空指针
 * 如果是其他数据结构，本类提供 getEntity() 提供给开发者自行操作
 * columnIndex 从1开始计数
 */

public class DBResultSet implements ResultSet{
    private Entity entity;
    private BasicTable table;
    private _DBConnection conn;
    private int row = -1;
    private int rows;
    private String tableName;
    private String filePath;

    private HashMap<String,Integer> findColumnHashMap;

    private int updateRow;

    private int insertRow;

    private HashMap<Integer,Entity> insertRowMap; //插入数据的特殊行
    
    private HashMap<Integer,Entity> new_updateRowMap; // 新数据的缓存行
    
    private HashMap<Integer,Entity> old_updateRowMap; // 旧数据的缓存行

    private boolean isInsert;

    private boolean isClosed = false;

    public DBResultSet(_DBConnection conn,Entity entity,String sql) throws SQLException{
        if(entity == null) throw new SQLException("ResultSet get data is null");
        this.conn = conn;
        this.tableName = conn.getTableName();
        this.filePath = conn.getFilePath();
        this.entity = entity;
        if(entity.isVector()){
            List<String> colNames = new ArrayList<>(1);
            sql = sql.trim();
            if(sql.contains("as")){
                colNames.add(sql.split(" ")[3]);
            }else{
                colNames.add(sql.split(" ")[1]);
            }
            List<Vector> cols = new ArrayList<>(1);
            cols.add((Vector)entity);
            this.table = new BasicTable(colNames,cols);
        }else if(entity.isTable()){
            this.table = (BasicTable) entity;
        }else{
            throw new SQLException("ResultSet get data is null");
        }
        rows = this.table.rows();

        findColumnHashMap = new HashMap<>(this.table.columns());

        insertRowMap = new HashMap<>(this.table.columns()+1);
        new_updateRowMap = new HashMap<>(this.table.columns()+1);
        old_updateRowMap = new HashMap<>(this.table.columns()+1);

        for(int i=0; i<this.table.columns(); ++i){
            findColumnHashMap.put(this.table.getColumnName(i),i+1);
        }

        System.out.println(table.rows() + "  "+ table.columns());
//        if(entity.isMatrix()){
//            Matrix matrix = (BasicIntMatrix) entity;
//            int col = matrix.getColumnLabels().rows();
//            int row = matrix.getRowLabels().rows();
//            List<Vector> cols = new ArrayList<>(col+1);
//            for(int i=0; i<col; ++i){
//                for(int j=0; j<row; ++j){
//
//                }
//            }
    }

    public Entity getEntity() {
        return entity;
    }

    /**
     * 数据持久化
     * @throws SQLException
     */
    public void saveTable() throws SQLException{
//        try {
//            conn.getDb().run(MessageFormat.format("saveTable(\"{0}\",{1},`{1})",filePath,tableName));
//        }catch (IOException e){
//            new SQLException(e);
//        }
    }

    /**
     * 加载表
     * @throws SQLException
     */
    public Entity loadTable() throws SQLException{
        return run(MessageFormat.format("{1} = loadTable(\"{0}\",`{1});{1}",filePath,tableName));
    }

    @Override
    public boolean next() throws SQLException {
        checkedClose();
        row++;
        return row <= rows-1;
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

    /**
     * 这里获取的是数据结构的String表示形式
     * @param columnIndex
     * @return
     * @throws SQLException
     */

    @Override
    public String getString(int columnIndex) throws SQLException{
        return ((Entity)getObject(columnIndex)).getString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException{
        return ((BasicBoolean)getObject(columnIndex)).getBoolean();
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException{
        return ((BasicByte)getObject(columnIndex)).getByte();
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return ((BasicShort)getObject(columnIndex)).getShort();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return ((BasicInt)getObject(columnIndex)).getInt();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return ((BasicLong)getObject(columnIndex)).getLong();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return ((BasicFloat)getObject(columnIndex)).getFloat();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return ((BasicDouble)getObject(columnIndex)).getDouble();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException{
        checkedClose();
        return null;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return  toByteArray(getObject(columnIndex));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkedClose();
        Scalar scalar = (Scalar) getObject(columnIndex);
        LocalDate date = null;
        if(scalar instanceof BasicDate){
            date = ((BasicDate) scalar).getDate();
        }
        if (date==null) return null;
        return new Date(date.getYear(),date.getMonthValue(),date.getDayOfMonth());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkedClose();
        Scalar scalar = (Scalar) getObject(columnIndex);
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
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkedClose();
        Scalar scalar = (Scalar) getObject(columnIndex);
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
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException{
        //todo
        checkedClose();
        return null;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
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
        Driver.unused();
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new DBResultSetMetaData(table);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkedClose();
        //throw new SQLException(""+columnIndex);
        return table.getColumn(adjustColumnIndex(columnIndex)).get(row);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        checkedClose();
        return table.getColumn(columnLabel).get(row);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkedClose();
        return findColumnHashMap.get(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Driver.unused();
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        Driver.unused();
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
    public void updateNull(int columnIndex) throws SQLException {

    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        checkedClose();
        update(columnIndex,x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        checkedClose();
        update(columnIndex,x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        checkedClose();
        update(columnIndex,x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        checkedClose();
        update(columnIndex,x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        checkedClose();
        update(columnIndex,x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        checkedClose();
        update(columnIndex,x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        checkedClose();
        update(columnIndex,x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal bigDecimal) throws SQLException {
        //((BasicShortVector)table.getColumn(i)).setShort(row,i1);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        checkedClose();
        update(columnIndex,x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] bytes) throws SQLException {
        update(columnIndex,toObject(bytes));
    }

    @Override
    public void updateDate(int columnIndex, Date date) throws SQLException {
        update(columnIndex,date);
    }

    @Override
    public void updateTime(int columnIndex, Time time) throws SQLException {
        update(columnIndex,time);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp timestamp) throws SQLException {
        update(columnIndex,timestamp);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream inputStream, int i1) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream inputStream, int i1) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader reader, int i1) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object o, int i1) throws SQLException {
        update(columnIndex,o);
    }

    @Override
    public void updateObject(int columnIndex, Object o) throws SQLException {
        update(columnIndex,o);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {

    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {

    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        update(columnLabel,x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] bytes) throws SQLException {
        update(columnLabel,bytes);
    }

    @Override
    public void updateDate(String columnLabel, Date date) throws SQLException {
        update(columnLabel,date);
    }

    @Override
    public void updateTime(String columnLabel, Time time) throws SQLException {
        update(columnLabel,time);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp timestamp) throws SQLException {
        update(columnLabel,timestamp);
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
    public void updateObject(String columnLabel, Object o, int i) throws SQLException {
        update(columnLabel,o);
        
    }

    @Override
    public void updateObject(String columnLabel, Object o) throws SQLException {
        update(columnLabel,o);
        
    }

    @Override
    public void insertRow() throws SQLException {

        if(insertRow == row){
            System.out.println("here");
            insertRun();
            saveTable();
            table = (BasicTable) run(tableName);
            rows = table.rows();
        }
        insertRowMap.clear();
        isInsert = false;
    }

    @Override
    public void updateRow() throws SQLException {
        if(updateRow == row){
            updateRun();
            saveTable();
        }
        old_updateRowMap.clear();
        new_updateRowMap.clear();
    }

    @Override
    public void deleteRow() throws SQLException {
        StringBuilder sb = new StringBuilder("delete from ").append(tableName).append(" where ");
        for(int i=1; i<=table.columns(); ++i){
            sb.append(getColumnName(i)).append(" = ").append(Utils.java2db(getObject(i))).append(", ");
        }
        sb.delete(sb.length()-2,sb.length());
        String sql = sb.toString();
        System.out.println(sql);
        run(sql);
        table = (BasicTable) run(tableName);
        rows = table.rows();
        saveTable();
    }

    @Override
    public void refreshRow() throws SQLException {
        BasicTable newTable = (BasicTable) loadTable();
        try {
            for(int i=0; i<newTable.columns(); ++i){
                table.getColumn(i).set(row,newTable.getColumn(i).get(row));
            }
        }catch (Exception e){
            throw new SQLException(e);
        }

    }

    @Override
    public void cancelRowUpdates() throws SQLException {

    }

    @Override
    public void moveToInsertRow() throws SQLException {
        isInsert = true;
    }

    @Override
    public void moveToCurrentRow() throws SQLException {

    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(adjustColumnIndex(columnIndex));
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
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException{
        return getObject(columnLabel);
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
    public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
        return getDate(adjustColumnIndex(columnIndex));
    }

    @Override
    public Date getDate(String columnLabel, Calendar calendar) throws SQLException {
        return getDate(columnLabel);
    }

    @Override
    public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
        return getTime(adjustColumnIndex(columnIndex));
    }

    @Override
    public Time getTime(String columnLabel, Calendar calendar) throws SQLException {
        return getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar calendar) throws SQLException {
        return getTimestamp(adjustColumnIndex(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar calendar) throws SQLException {
        return getTimestamp(columnLabel);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
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
        return (BasicDate)getObject(columnLabel);
    }

    public BasicMonth getBasicMonth(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicMonth)getObject(columnLabel);
    }

    public BasicTime getBasicTime(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicTime)getObject(columnLabel);
    }

    public BasicMinute getBasicMinute(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicMinute)getObject(columnLabel);
    }

    public BasicSecond getBasicSecond(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicSecond)getObject(columnLabel);
    }

    public BasicDateTime getBasicDateTime(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicDateTime)getObject(columnLabel);
    }

    public BasicNanoTime getBasicNanoTime(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicNanoTime)getObject(columnLabel);
    }

    public BasicNanoTimestamp getBasicNanoTimestamp(String columnLabel) throws SQLException{
        checkedClose();
        return (BasicNanoTimestamp)getObject(columnLabel);
    }

    public BasicDate getBasicDate(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicDate)getObject(columnIndex);
    }

    public BasicMonth getBasicMonth(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicMonth)getObject(columnIndex);
    }

    public BasicTime getBasicTime(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicTime)getObject(columnIndex);
    }

    public BasicMinute getBasicMinute(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicMinute)getObject(columnIndex);
    }

    public BasicSecond getBasicSecond(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicSecond)getObject(columnIndex);
    }

    public BasicDateTime getBasicDateTime(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicDateTime)getObject(columnIndex);
    }

    public BasicNanoTime getBasicNanoTime(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicNanoTime)getObject(columnIndex);
    }

    public BasicNanoTimestamp getBasicNanoTimestamp(int columnIndex) throws SQLException{
        checkedClose();
        return (BasicNanoTimestamp)getObject(columnIndex);
    }

    private void update(String name, Object value) throws SQLException{
        update(findColumn(name),value);
    }

    private void update(int index, Object value) throws SQLException{
        if(isInsert){
            insert(index, value);
        }else{
            checkedClose();
            updateRow = row;
            old_updateRowMap.put(index,table.getColumn(adjustColumnIndex(index)).get(row));
            Vector vector = table.getColumn(adjustColumnIndex(index));
            if(value instanceof Scalar){
                try {
                    table.getColumn(adjustColumnIndex(index)).set(row, (Scalar)value);
                }catch (Exception e){
                    throw new SQLException(e);
                }
            }if (value instanceof Boolean) {
                ((BasicBooleanVector) table.getColumn(adjustColumnIndex(index))).setBoolean(row, (boolean) value);
            } else if (value instanceof Byte) {
                ((BasicByteVector) table.getColumn(adjustColumnIndex(index))).setByte(row, (byte) value);
            } else if (value instanceof Character){
                System.out.println(table.getColumn(adjustColumnIndex(index)).getClass().getName());
                ((BasicByteVector) table.getColumn(adjustColumnIndex(index))).setByte(row, (byte) ((char)value & 0xFF));
            }
            else if (value instanceof Integer) {
                if(vector instanceof BasicLongVector){
                    ((BasicLongVector) vector).setLong(row, (int) value);
                }else if(vector instanceof BasicIntVector){
                    ((BasicIntVector) vector).setInt(row,(int) value);
                }else if(vector instanceof BasicShortVector){
                    ((BasicShortVector) vector).setShort(row,Short.valueOf(value.toString()));
                }
            } else if (value instanceof Short) {
                if(vector instanceof BasicLongVector){
                    ((BasicLongVector) vector).setLong(row, (short) value);
                }else if(vector instanceof BasicIntVector){
                    ((BasicIntVector) vector).setInt(row,(short) value);
                }else if(vector instanceof BasicShortVector){
                    ((BasicShortVector) vector).setShort(row,(short) value);
                }
            } else if (value instanceof Long) {
                if(vector instanceof BasicLongVector){
                    ((BasicLongVector) vector).setLong(row, (long) value);
                }else if(vector instanceof BasicIntVector){
                    ((BasicIntVector) vector).setInt(row,Integer.valueOf(value.toString()));
                }else if(vector instanceof BasicShortVector){
                    ((BasicShortVector) vector).setShort(row,Short.valueOf(value.toString()));
                }
            } else if (value instanceof Float) {
                if(vector instanceof BasicFloatVector){
                    ((BasicFloatVector) vector).setFloat(row,(float) value);
                }else if (vector instanceof BasicDoubleVector){
                    ((BasicDoubleVector) vector).setDouble(row,(float) value);
                }
            } else if (value instanceof Double) {
                if(vector instanceof BasicFloatVector){
                    ((BasicFloatVector) vector).setFloat(row,(float) value);
                }else if (vector instanceof BasicDoubleVector){
                    ((BasicDoubleVector) vector).setDouble(row,(double) value);
                }
            } else if (value instanceof String) {
                ((BasicStringVector) table.getColumn(adjustColumnIndex(index))).setString(row, (String) value);
            } else if (value instanceof Date) {
                updateDateTime(index, ((Date) value).toLocalDate());
            } else if (value instanceof Time) {
                updateDateTime(index, ((Time) value).toLocalTime());
            } else if (value instanceof Timestamp) {
                updateDateTime(index, ((Timestamp) value).toLocalDateTime());
            } else {
                updateDateTime(index,value);
            }
            new_updateRowMap.put(index,table.getColumn(adjustColumnIndex(index)).get(row));
        }
    }

    private void insert(String name, Object value) throws SQLException{
        insert(findColumn(name),value);
    }


    private void insert(int index, Object value) throws SQLException{
        checkedClose();
        insertRow = row;
        if(value instanceof Scalar){
            insertRowMap.put(index,(Scalar) value);
        }else if(value instanceof Vector) {
            insertRowMap.put(index,(Vector) value);
        }else if(value instanceof Boolean){
            insertRowMap.put(index,new BasicBoolean((boolean)value));
        }else if(value instanceof Byte){
            insertRowMap.put(index,new BasicByte((byte) value));
        }else if(value instanceof Integer){
            insertRowMap.put(index,new BasicInt((int) value));
        }else if(value instanceof Short){
            insertRowMap.put(index,new BasicShort((short) value));
        }else if(value instanceof Long){
            insertRowMap.put(index,new BasicLong((long) value));
        }else if(value instanceof Float){
            insertRowMap.put(index,new BasicFloat((float) value));
        }else if(value instanceof Double){
            insertRowMap.put(index,new BasicDouble((double) value));
        }else if(value instanceof String){
            insertRowMap.put(index,new BasicString((String) value));
        }else if(value instanceof Date){
            insertDateTime(index,((Date) value).toLocalDate());
        }else if(value instanceof Time){
            insertDateTime(index,((Time) value).toLocalTime());
        }else if(value instanceof Timestamp){
            insertDateTime(index,((Timestamp) value).toLocalDateTime());
        }else {
            insertDateTime(index,value);
        }
    }


    private void updateDateTime(String name, Object value) throws SQLException{
        updateDateTime(findColumn(name),value);
    }

    private void updateDateTime(int index, Object value) throws SQLException{
        Vector vector = table.getColumn(adjustColumnIndex(index));
        if(value instanceof LocalDate){
            if(vector instanceof BasicDateVector){
                ((BasicDateVector) vector).setDate(row,(LocalDate) value);
            }
        }else if(value instanceof LocalTime){
            if(vector instanceof BasicTimeVector){
                ((BasicTimeVector) vector).setTime(row,(LocalTime) value);
            }else if(vector instanceof BasicMinuteVector){
                ((BasicMinuteVector) vector).setMinute(row,(LocalTime) value);
            }else if(vector instanceof BasicSecondVector){
                ((BasicSecondVector) vector).setSecond(row,(LocalTime) value);
            }else if(vector instanceof BasicNanoTime){
                ((BasicNanoTimeVector) vector).setNanoTime(row,(LocalTime) value);
            }
        }else if(value instanceof LocalDateTime){
            if(vector instanceof BasicDateTimeVector){
                ((BasicDateTimeVector) vector).setDateTime(row,(LocalDateTime) value);
            }else if(vector instanceof BasicTimestampVector){
                ((BasicTimestampVector) vector).setTimestamp(row,(LocalDateTime) value);
            }else if(vector instanceof BasicNanoTimestampVector){
                ((BasicNanoTimestampVector) vector).setNanoTimestamp(row,(LocalDateTime) value);
            }
        }else if(value instanceof YearMonth){
            if(vector instanceof BasicMonthVector){
                ((BasicMonthVector) vector).setMonth(row,(YearMonth) value);
            }
        }
    }

    private void insertDateTime(String name, Object value) throws SQLException{
        insertDateTime(findColumn(name),value);
    }

    private void insertDateTime(int index, Object value) throws SQLException{
        Vector vector = table.getColumn(adjustColumnIndex(index));
        if(value instanceof LocalDate){
            if(vector instanceof BasicDateVector){
                insertRowMap.put(index,new BasicDate((LocalDate) value));
            }
        }else if(value instanceof LocalTime){
            if(vector instanceof BasicTimeVector){
                insertRowMap.put(index,new BasicTime((LocalTime) value));
            } else if(vector instanceof BasicMinuteVector){
                insertRowMap.put(index,new BasicMinute((LocalTime) value));
            }else if(vector instanceof BasicSecondVector){
                insertRowMap.put(index,new BasicSecond((LocalTime) value));
            }else if(vector instanceof BasicNanoTime){
                insertRowMap.put(index,new BasicNanoTime((LocalTime) value));
            }
        }else if(value instanceof LocalDateTime){
            if(vector instanceof BasicTimestampVector){
                insertRowMap.put(index,new BasicTimestamp((LocalDateTime) value));
            }else if(vector instanceof BasicDateTimeVector){
                insertRowMap.put(index,new BasicDateTime((LocalDateTime) value));
            }else if(vector instanceof BasicNanoTimestampVector){
                insertRowMap.put(index,new BasicNanoTimestamp((LocalDateTime) value));
            }
        }else if(value instanceof YearMonth){
            if(vector instanceof BasicMonthVector){
                insertRowMap.put(index,new BasicMonth((YearMonth) value));
            }
        }
    }

    
    private void updateRun() throws SQLException{
        Entity value;
        StringBuilder sb = new StringBuilder();
        StringBuilder where = new StringBuilder(" where ");
        sb.append("update ").append(tableName).append(" set ");
        for (int i = 1; i <= table.columns(); ++i) {
            if((value = new_updateRowMap.get(i)) != null){
                sb.append(getColumnName(i)).append(" = ").append(Utils.java2db(value)).append(", ");
                where.append(getColumnName(i)).append(" = ").append(Utils.java2db(old_updateRowMap.get(i))).append(" ,");
            }else{
                where.append(getColumnName(i)).append(" = ").append(Utils.java2db(table.getColumn(adjustColumnIndex(i)).get(row))).append(" ,");
            }
            
        }
        sb.delete(sb.length()-2,sb.length());
        where.delete(where.length()-2,where.length());
        sb.append(where);
                
        String sql = sb.toString();
        System.out.println(sql);
        run(sql);
    }


    private void insertRun() throws SQLException{
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(tableName).append(" values ( ");
        for (int i = 1; i <= table.columns(); ++i) {
            sb.append(Utils.java2db(insertRowMap.get(i))).append(", ");

        }
        sb.delete(sb.length()-2,sb.length());
        sb.append(")");
        String sql = sb.toString();
        System.out.println(sql);
        run(sql);
    }

    private Entity run(String sql) throws SQLException {
        try {
            return conn.getDb().run(sql);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }
    
    private String JAVA2DBString(Entity entity, Object value){
        if(value instanceof Boolean){
            return new BasicBoolean((boolean) value).getString();
        }else if(value instanceof Byte){
            return new BasicByte((byte) value).getString();
        }else if(value instanceof Integer){
            return new BasicInt((int) value).getString();
        }else if(value instanceof Short){
            return new BasicShort((short) value).getString();
        }else if(value instanceof Long){
            return new BasicLong((long) value).getString();
        }else if(value instanceof Float){
            return new BasicFloat((float) value).getString();
        }else if(value instanceof Double){
            return new BasicDouble((double) value).getString();
        }else if(value instanceof String){
            return "`" + new BasicString((String) value).getString();
        }else if(value instanceof Date){
            return JAVA2DBString4DataTime(entity,((Date) value).toLocalDate());
        }else if(value instanceof Time){
            return JAVA2DBString4DataTime(entity,((Time) value).toLocalTime());
        }else if(value instanceof Timestamp){
            return JAVA2DBString4DataTime(entity,((Timestamp) value).toLocalDateTime());
        }else{
            return JAVA2DBString4DataTime(entity,value);
        }
    }

    private String JAVA2DBString4DataTime(Entity entity, Object value){
        if(value instanceof LocalDate){
            if(entity instanceof BasicDate){
                return new BasicDate((LocalDate) value).getString();
            }
        }else if(value instanceof LocalTime){
            if(entity instanceof BasicTime){
                return new BasicTime((LocalTime) value).getString();
            } else if(entity instanceof BasicMinuteVector){
                return new BasicMinute((LocalTime) value).getString();
            }else if(entity instanceof BasicSecondVector){
                return new BasicSecond((LocalTime) value).getString();
            }else if(entity instanceof BasicNanoTime){
                return new BasicNanoTime((LocalTime) value).getString();
            }
        }else if(value instanceof LocalDateTime){
            if(entity instanceof BasicTimestamp){
                return new BasicTimestamp((LocalDateTime) value).getString();
            }else if(entity instanceof BasicDateTime){
                return new BasicDateTime((LocalDateTime) value).getString();
            }else if(entity instanceof BasicNanoTimestampVector){
                return new BasicNanoTimestamp((LocalDateTime) value).getString();
            }
        }else if(value instanceof YearMonth){
            if(entity instanceof BasicMonth){
                new BasicMonth((YearMonth) value).getString();
            }
        }
        
        return null;
    }
    
    private String DB2String(Entity e){
        if(e == null) return "";
        if(e instanceof BasicString){
            return "`" + e.getString();
        }else{
            return e.getString();
        }
    }

    private String getColumnName(int columnIndex){
        return table.getColumnName(adjustColumnIndex(columnIndex));
    }

    private int adjustColumnIndex(int columnIndex){
        return columnIndex-1;
    }


    private void checkedClose() throws SQLException{
        if(table == null && isClosed){
            throw new SQLException("ResultSet is closed");
        }
    }

    /**
     * 对象转数组
     * @param obj
     * @return
     */
    public byte[] toByteArray (Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray ();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return bytes;
    }

    /**
     * 数组转对象
     * @param bytes
     * @return
     */
    public Object toObject (byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream (bytes);
            ObjectInputStream ois = new ObjectInputStream (bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return obj;
    }


}
