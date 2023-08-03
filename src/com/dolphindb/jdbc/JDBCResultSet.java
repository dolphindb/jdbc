package com.dolphindb.jdbc;

import com.xxdb.data.*;
import com.xxdb.data.Vector;
import javax.sql.rowset.serial.SerialBlob;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.YearMonth;
import java.util.*;


public class JDBCResultSet implements ResultSet{
    private final JDBCConnection conn;
    private final JDBCStatement statement;
    private BasicTable table;
    private int row = -1;
    private int rows;
    private String tableName;
    private Entity tableNameArg;
    private List<Entity> arguments;
    private HashMap<String,Integer> findColumnHashMap;
    private int updateRow;
    private int insertRow;
    private HashMap<Integer,Entity> insertRowMap;
    private boolean isInsert;
    private boolean isClosed = false;
    private boolean isUpdatable = false;
    private Object o;
    private Entity entity;

    private EntityBlockReader reader;

    public JDBCResultSet(JDBCConnection conn, JDBCStatement statement, Entity entity, String sql) throws SQLException {
        this.conn = conn;
        this.statement = statement;
        if(entity.isTable()){
            this.table = (BasicTable) entity;
            rows = this.table.rows();
            findColumnHashMap = new HashMap<>(this.table.columns());
            for(int i=0; i<this.table.columns(); ++i){
                findColumnHashMap.put(this.table.getColumnName(i),i+1);
            }
            this.isUpdatable = false;
            if (this.isUpdatable){
                insertRowMap = new HashMap<>(this.table.columns()+1);
            }
        }else{
            this.entity = entity;
            if(sql!=null&&sql.contains("select 1 as ")){
                List<String> colNames = new ArrayList<>(1);
                colNames.add(Utils.getSelectOneColName(sql));
                List<Vector> cols = new ArrayList<>(1);
                Vector vector = new BasicIntVector(1);
                Scalar scalar = new BasicInt(1);
                try {
                    vector.set(0,scalar);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                cols.add(vector);
                this.table = new BasicTable(colNames,cols);
                rows = this.table.rows();
                findColumnHashMap = new HashMap<>(this.table.columns());
                for(int i=0; i<this.table.columns(); ++i){
                    findColumnHashMap.put(this.table.getColumnName(i),i+1);
                }
                this.isUpdatable = false;
                if (this.isUpdatable){
                    insertRowMap = new HashMap<>(this.table.columns()+1);
                }
            }
        }
//        if(sql == null || sql.length() == 0){
//            this.isUpdatable = false;
//        }else{
//            this.isUpdatable = Utils.isUpdatable(sql);
//
//            if(this.isUpdatable){
//                this.tableName = Utils.getTableName(sql);
//                if(Utils.isUpdatable(this.tableName)){
//                    String s = run("typestr " + tableName).getString();
//                    if(!s.equals("IN-MEMORY TABLE")){
//                        this.isUpdatable = false;
//                    }else{
//                        tableNameArg = new BasicString(tableName);
//                    }
//                }
//            }else{
//                this.tableName = "";
//            }
//        }
    }

    public JDBCResultSet(JDBCConnection conn, JDBCStatement statement, EntityBlockReader reader, String sql) throws SQLException{
        this.conn = conn;
        this.statement = statement;
        this.reader = reader;
        if(!reader.hasNext()) {
            throw new SQLException("ResultSet data is null");
        }
        BasicTable entity = null;
        try {
            entity = (BasicTable)reader.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if(entity.isTable()){
            this.table = entity;
        }else{
            throw new SQLException("ResultSet data is null");
        }
        rows = this.table.rows();
        findColumnHashMap = new HashMap<>(this.table.columns());
        for(int i=0; i<this.table.columns(); ++i){
            findColumnHashMap.put(this.table.getColumnName(i),i+1);
        }
        this.isUpdatable = false;
        if (this.isUpdatable){
            insertRowMap = new HashMap<>(this.table.columns()+1);
        }
    }


    private BasicTable loadTable() throws SQLException{
        return (BasicTable) run(tableName);
    }

    @Override
    public boolean next() throws SQLException {
        if(this.getFetchSize() != 0) {
            // 当未读取到大表分段或者读取分段的行数超过限定时，尝试读取下一分段
            if(this.table == null || row >= rows - 1) {
                try {
                    if(!this.reader.hasNext()) return false;
                    this.table = (BasicTable) this.reader.read();
                    rows = this.table.rows();
                    row = -1;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        checkClosed();
        row++;
        return row <= rows-1;
    }

    @Override
    public void close() throws SQLException {
        isClosed = true;
        if(findColumnHashMap != null){
            findColumnHashMap.clear();
            findColumnHashMap = null;
        }
        if(insertRowMap != null){
            insertRowMap.clear();
            insertRowMap = null;
        }
        if(arguments != null){
            arguments.clear();
            arguments = null;
        }
        table = null;
    }

    @Override
    public boolean wasNull() throws SQLException {
    	return ((Scalar) o).isNull();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        Vector column = table.getColumn(adjustColumnIndex(columnIndex));
        Entity entity = column.get(row);
        Entity.DATA_TYPE x = column.getDataType();
        switch (x){
            case DT_BOOL:
                try {
                    return ((BasicBoolean) entity).booleanValue();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            case DT_BYTE:
                return ((BasicByte) entity).byteValue();
            case DT_SHORT:
                try {
                    return ((BasicShort) entity).shortValue();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            case DT_INT:
                try {
                    return ((BasicInt) entity).intValue();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            case DT_LONG:
                try {
                    return ((BasicLong) entity).longValue();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            case DT_DATE:
                return java.sql.Date.valueOf(((BasicDate) entity).getDate());
            case DT_TIME:
                return java.sql.Time.valueOf(((BasicTime) entity).getTime());
            case DT_DATETIME:
                return ((BasicDateTime) entity).getDateTime();
            case DT_TIMESTAMP:
                return ((BasicTimestamp) entity).getTimestamp();
            case DT_FLOAT:
                try {
                    return ((BasicFloat) entity).floatValue();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            case DT_DOUBLE:
                try {
                    return ((BasicDouble) entity).doubleValue();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            case DT_STRING:
            case DT_BLOB:
            case DT_IPADDR:
            case DT_INT128:
            case DT_COMPLEX:
            case DT_POINT:
                return entity.getString();
            case DT_MONTH:
                return ((BasicMonth) entity).getMonth();
            case DT_MINUTE:
                return ((BasicMinute) entity).getMinute();
            case DT_SECOND:
                return ((BasicSecond) entity).getSecond();
            case DT_NANOTIME:
                return ((BasicNanoTime) entity).getNanoTime();
            case DT_NANOTIMESTAMP:
                return ((BasicNanoTimestamp) entity).getNanoTimestamp();
            case DT_SYMBOL:
                return entity.getString();
            case DT_UUID:
                String string = entity.getString();
                if (string.isEmpty()) {
                    return null;
                } else {
                    return UUID.fromString(string);
                }
            case DT_DATEHOUR:
                return ((BasicDateHour) entity).getDateHour();
            case DT_DECIMAL32:
            case DT_DECIMAL64:
            case DT_DECIMAL128:
                try {
                    return new BigDecimal(entity.getString());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            default:
                return entity;
        }
    }

    @Override
    public String getString(int columnIndex) throws SQLException{
        return getObject(columnIndex).toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException{
        Boolean x = (Boolean) getObject(columnIndex);
        if (Objects.isNull(x))
            return false;
        else
            return x;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException{
        Byte x = (Byte) getObject(columnIndex);
        if (Objects.isNull(x))
            return 0;
        else
            return x;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Short x = (Short) getObject(columnIndex);
        if (Objects.isNull(x))
            return 0;
        else
            return x;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Integer x = (Integer) getObject(columnIndex);
        if (Objects.isNull(x))
            return 0;
        else
			return x;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Long x = (Long) getObject(columnIndex);
        if (Objects.isNull(x))
            return 0;
        else
			return x;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Float x = (Float) getObject(columnIndex);
        if (Objects.isNull(x))
            return 0;
        else
            return x;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Double x = (Double) getObject(columnIndex);
        if (Objects.isNull(x))
            return 0;
        else
            return x;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return BigDecimal.valueOf(getDouble(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException{
        Driver.unused("getBigDecimal not implemented");
        return null;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Driver.unused("getBytes not implemented");
        return null;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Object x = getObject(columnIndex);
        LocalDate localdate = null;
        if (x instanceof Date) {
            return (Date) x;
        }
        else if (x instanceof BasicMonth) {
            YearMonth dt = ((BasicMonth) x).getMonth();
            if (dt != null)
                localdate = LocalDate.of(dt.getYear(), dt.getMonth(),1);
        }
        else if (x instanceof LocalDateTime) {
            localdate = LocalDate.of(((LocalDateTime)x).getYear(), ((LocalDateTime)x).getMonth(), ((LocalDateTime)x).getDayOfMonth());
        }
        else if (x instanceof BasicTimestamp) {
        	LocalDateTime dt = ((BasicTimestamp) x).getTimestamp();
            if (dt != null)
                localdate = LocalDate.of(dt.getYear(), dt.getMonth(), dt.getDayOfMonth());
        }
        else if (x instanceof BasicNanoTimestamp) {
        	LocalDateTime dt = ((BasicNanoTimestamp) x).getNanoTimestamp();
            if (dt != null)
                localdate = LocalDate.of(dt.getYear(), dt.getMonth(), dt.getDayOfMonth());
        }
        if (localdate == null) return null;
        return java.sql.Date.valueOf(localdate);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Object x = getObject(columnIndex);
        LocalTime time = null;
        if (x instanceof BasicMinute){
            time = ((BasicMinute) x).getMinute();
        }
        else if (x instanceof BasicSecond){
            time = ((BasicSecond) x).getSecond();
        }
        else if (x instanceof Time) {
            return (Time) x;
        }
        else if (x instanceof LocalDateTime) {
            time = LocalTime.of(((LocalDateTime) x).getHour(), ((LocalDateTime) x).getMinute(), ((LocalDateTime) x).getSecond());
        }
        else if (x instanceof BasicNanoTime){
            time = ((BasicNanoTime) x).getNanoTime();
        }
        else if (x instanceof BasicNanoTimestamp) {
        	LocalDateTime dt = ((BasicNanoTimestamp) x).getNanoTimestamp();
        	if (dt != null)
        		time = LocalTime.of(dt.getHour(), dt.getMinute(), dt.getSecond(), dt.getNano());
        }
        if (time == null) return null;

        return java.sql.Time.valueOf(time);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object x = getObject(columnIndex);
        LocalDateTime dateTime = null;
        if (x instanceof LocalDateTime){
            dateTime = (LocalDateTime) x;
        }
        else if (x instanceof BasicTimestamp){
            dateTime = ((BasicTimestamp) x).getTimestamp();
        }
        else if (x instanceof BasicNanoTimestamp){
            dateTime = ((BasicNanoTimestamp) x).getNanoTimestamp();
        }
        if (dateTime == null) return null;
        return java.sql.Timestamp.valueOf(dateTime);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
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
    public String getString(String columnLabel) throws SQLException{
        return getString(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException{
        return getBigDecimal(findColumn(columnLabel), scale);
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
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
    	// TODO: implement warnings
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    	// TODO: implement warnings
    }

    @Override
    public String getCursorName() throws SQLException {
    	throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new JDBCResultSetMetaData(table);
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    	throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException{
    	throw new SQLFeatureNotSupportedException();
    }

    @SuppressWarnings("unchecked")
	@Override
    public <T> T getObject(int columnIndex, Class<T> aClass) throws SQLException {
        try {
            return (T) TypeCast.entity2java((Entity) getObject(columnIndex), aClass.getName());
        }catch (Exception e){
            throw new SQLException(e);
        }
    }

    @SuppressWarnings("unchecked")
	@Override
    public <T> T getObject(String columnLabel, Class<T> aClass) throws SQLException {
        return getObject(findColumn(columnLabel),aClass);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return findColumnHashMap.get(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return null;
    }


    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
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
        row = 0;
        return rows > 0;
    }

    @Override
    public boolean last() throws SQLException {
        row = rows - 1;
        return rows > 0;
    }

    @Override
    public int getRow() throws SQLException {
        return row + 1;
    }

    @Override
    public boolean absolute(int columnIndex) throws SQLException {
        if (columnIndex >= 0)
            row = columnIndex - 1;
        else
            row = rows + columnIndex;
        return row < rows;
    }

    @Override
    public boolean relative(int columnIndex) throws SQLException {
        row += columnIndex;
        return  row >= 0 && row < rows;
    }

    @Override
    public boolean previous() throws SQLException {
        --row;
        return row >= 0;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
    }

    @Override
    public int getFetchSize() throws SQLException {
        return statement != null ? statement.getFetchSize() : 0;
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_SCROLL_SENSITIVE;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    public EntityBlockReader getReader() {
        return this.reader;
    }

    public void setReader(EntityBlockReader reader) {
        this.reader = reader;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return  false;
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
        update(columnIndex, x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        update(columnIndex, x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        update(columnIndex, x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        update(columnIndex, x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        update(columnIndex, x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        update(columnIndex, x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        update(columnIndex, x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal bigDecimal) throws SQLException {
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        update(columnIndex,x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] bytes) throws SQLException {
        Driver.unused();
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
    public void updateAsciiStream(int columnIndex, InputStream inputStream, int columnIndex1) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream inputStream, int columnIndex1) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader reader, int columnIndex1) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateObject(int columnIndex, Object o, int columnIndex1) throws SQLException {
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
    public void updateBigDecimal(String columnLabel, BigDecimal bigDecimal) throws SQLException {
        Driver.unused();
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
    public void updateAsciiStream(String columnLabel, InputStream inputStream, int columnIndex) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream inputStream, int columnIndex) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int columnIndex) throws SQLException {
        Driver.unused();
    }

    @Override
    public void updateObject(String columnLabel, Object o, int columnIndex) throws SQLException {
        update(columnLabel,o);

    }

    @Override
    public void updateObject(String columnLabel, Object o) throws SQLException {
        update(columnLabel,o);
    }

    @Override
    public void insertRow() throws SQLException {
        isUpdatable();
        try {
            if(insertRow == row){
                createArguments();
                conn.run("tableInsert",arguments);
                table = loadTable();
                rows = table.rows();
            }
            arguments.clear();
            insertRowMap.clear();
            isInsert = false;
        }catch (Exception e){
            throw new SQLException(e.getMessage());
        }

    }

    @Override
    public void updateRow() throws SQLException {
        isUpdatable();
        if(updateRow == row){
            updateRun();
            table = loadTable();
            rows = table.rows();
        }
        insertRowMap.clear();
    }

    @Override
    public void deleteRow() throws SQLException {
        isUpdatable();
        StringBuilder sb = new StringBuilder("delete from ").append(tableName).append(" where ");
        for(int i=1; i<=table.columns(); ++i){
            sb.append(getColumnName(i)).append(" = ").append(Utils.java2db(getObject(i))).append(", ");
        }
        sb.delete(sb.length()-2,sb.length());
        String sql = sb.toString();
        run(sql);
        table = loadTable();
        rows = table.rows();
    }

    @Override
    public void refreshRow() throws SQLException {
        isUpdatable();
        BasicTable newTable = loadTable();
        try {
            for(int i=0; i<newTable.columns(); ++i){
                table.getColumn(i).set(row,newTable.getColumn(i).get(row));
            }
        }catch (Exception e){
            throw new SQLException(e.getMessage());
        }

    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        isUpdatable();
        if(isInsert){
            throw new SQLException("cursor is on the insert row");
        }
        insertRowMap.clear();
        isInsert = false;
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        isUpdatable();
        isInsert = true;
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        isInsert = false;
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        Blob blob = new SerialBlob(getObject(columnIndex).toString().getBytes());
        return blob;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
    	return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar calendar) throws SQLException {
    	return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
    	return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar calendar) throws SQLException {
    	return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar calendar) throws SQLException {
    	return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar calendar) throws SQLException {
    	return null;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref ref) throws SQLException {
    }

    @Override
    public void updateRef(String columnLabel, Ref ref) throws SQLException {
    }

    @Override
    public void updateBlob(int columnIndex, Blob blob) throws SQLException {
    	update(columnIndex,blob);
    }

    @Override
    public void updateBlob(String columnLabel, Blob blob) throws SQLException {
        update(columnLabel,blob);
    }

    @Override
    public void updateClob(int columnIndex, Clob clob) throws SQLException {
    }

    @Override
    public void updateClob(String columnLabel, Clob clob) throws SQLException {
    }

    @Override
    public void updateArray(int columnIndex, Array array) throws SQLException {
    }

    @Override
    public void updateArray(String columnLabel, Array array) throws SQLException {
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId rowId) throws SQLException {
    }

    @Override
    public void updateRowId(String columnLabel, RowId rowId) throws SQLException {
    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public void updateNString(int columnIndex, String columnLabel) throws SQLException {
    }

    @Override
    public void updateNString(String columnLabel, String columnLabel1) throws SQLException {
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML sqlxml) throws SQLException {
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML sqlxml) throws SQLException {
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader reader, long l) throws SQLException {
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long l) throws SQLException {
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream inputStream, long l) throws SQLException {
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream inputStream, long l) throws SQLException {
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader reader, long l) throws SQLException {
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream inputStream, long l) throws SQLException {
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream inputStream, long l) throws SQLException {
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long l) throws SQLException {
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long l) throws SQLException {
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long l) throws SQLException {
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long l) throws SQLException {
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long l) throws SQLException {
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long l) throws SQLException {
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long l) throws SQLException {
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader reader) throws SQLException {
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream inputStream) throws SQLException {
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream inputStream) throws SQLException {
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader reader) throws SQLException {
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream inputStream) throws SQLException {
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream inputStream) throws SQLException {
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        return aClass.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return aClass.isInstance(this);
    }

    public Entity getResult() throws SQLException{
        if (table == null)
            return entity;
        else
            return table;
    }

    public BasicDate getBasicDate(String columnLabel) throws SQLException{
        return (BasicDate)getObject(columnLabel);
    }

    public BasicMonth getBasicMonth(String columnLabel) throws SQLException{
        return (BasicMonth)getObject(columnLabel);
    }

    public BasicTime getBasicTime(String columnLabel) throws SQLException{
        return (BasicTime)getObject(columnLabel);
    }

    public BasicMinute getBasicMinute(String columnLabel) throws SQLException{
        return (BasicMinute)getObject(columnLabel);
    }

    public BasicSecond getBasicSecond(String columnLabel) throws SQLException{
        return (BasicSecond)getObject(columnLabel);
    }

    public BasicDateTime getBasicDateTime(String columnLabel) throws SQLException{
        return (BasicDateTime)getObject(columnLabel);
    }

    public BasicNanoTime getBasicNanoTime(String columnLabel) throws SQLException{
        return (BasicNanoTime)getObject(columnLabel);
    }

    public BasicNanoTimestamp getBasicNanoTimestamp(String columnLabel) throws SQLException{
        return (BasicNanoTimestamp)getObject(columnLabel);
    }

    public BasicDate getBasicDate(int columnIndex) throws SQLException{
        return (BasicDate)getObject(columnIndex);
    }

    public BasicMonth getBasicMonth(int columnIndex) throws SQLException{
        return (BasicMonth)getObject(columnIndex);
    }

    public BasicTime getBasicTime(int columnIndex) throws SQLException{
        return (BasicTime)getObject(columnIndex);
    }

    public BasicMinute getBasicMinute(int columnIndex) throws SQLException{
        return (BasicMinute)getObject(columnIndex);
    }

    public BasicSecond getBasicSecond(int columnIndex) throws SQLException{
        return (BasicSecond)getObject(columnIndex);
    }

    public BasicDateTime getBasicDateTime(int columnIndex) throws SQLException{
        return (BasicDateTime)getObject(columnIndex);
    }

    public BasicNanoTime getBasicNanoTime(int columnIndex) throws SQLException{
        return (BasicNanoTime)getObject(columnIndex);
    }

    public BasicNanoTimestamp getBasicNanoTimestamp(int columnIndex) throws SQLException{
        return (BasicNanoTimestamp)getObject(columnIndex);
    }

    public BasicTable getTable() {
        return table;
    }

    private void update(String name, Object value) throws SQLException{
        update(findColumn(name),value);
    }

    private void update(int columnIndex, Object value) throws SQLException{
        isUpdatable();
        if (isInsert){
            insertRow = row;
        }
        else {
            updateRow = row;
        }
        insert(columnIndex, value);
    }

    private void insert(int columnIndex, Object value) throws SQLException {
        try {
            Entity targetEntity = table.getColumn(adjustColumnIndex(columnIndex)).get(row);
            Entity.DATA_TYPE dataType = targetEntity.getDataType();
            int size = 0;
            if(dataType.getName().equals(Entity.DATA_TYPE.DT_DECIMAL.getName()) || dataType.getName().equals(Entity.DATA_TYPE.DT_DECIMAL32.getName()) ||
            dataType.getName().equals(Entity.DATA_TYPE.DT_DECIMAL64.getName()) || dataType.getName().equals(Entity.DATA_TYPE.DT_DECIMAL128.getName())){
                String value2 = String.valueOf(value);
                String[] values = value2.split("\\.");
                size = values[1].length();
            }
            Entity entity = BasicEntityFactory.createScalar(dataType, value, size);
            insertRowMap.put(columnIndex, entity);
        }catch (Exception e){
            throw new SQLException(e.getMessage());
        }

    }

    private void updateRun() throws SQLException{
        Entity value;
        StringBuilder sb = new StringBuilder();
        StringBuilder where = new StringBuilder(" where ");
        sb.append("update ").append(tableName).append(" set ");
        for (int i = 1; i <= table.columns(); ++i) {
            if((value = insertRowMap.get(i)) != null){
                sb.append(getColumnName(i)).append(" = ").append(Utils.java2db(value)).append(", ");
            }
            where.append(getColumnName(i)).append(" = ").append(Utils.java2db(table.getColumn(adjustColumnIndex(i)).get(row))).append(" ,");
        }
        sb.delete(sb.length()-2,sb.length());
        where.delete(where.length()-2,where.length());
        sb.append(where);
        String sql = sb.toString();
        run(sql);
    }

    private Entity run(String sql) throws SQLException {
        try {
            return conn.run(sql);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    private String getColumnName(int columnIndex){
        return table.getColumnName(adjustColumnIndex(columnIndex));
    }

    private int adjustColumnIndex(int columnIndex){
        return columnIndex-1;
    }

    private void checkClosed() throws SQLException{
        if (table == null && isClosed){
            throw new SQLException("ResultSet is closed");
        }
    }

    public void isUpdatable() throws SQLException{
        if (!isUpdatable) {
            throw new SQLException("Updating the table of ResultSet is not currently supported");
        }
    }

    private void createArguments(){
        int col = table.columns();
        if(arguments == null){
            arguments = new ArrayList<>(col+1);
        }
        arguments.add(tableNameArg);
        for(int i=1; i<= col; ++i){
            arguments.add(insertRowMap.get(i));
        }
    }

}
