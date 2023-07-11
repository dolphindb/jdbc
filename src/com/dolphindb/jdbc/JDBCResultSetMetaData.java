package com.dolphindb.jdbc;

import com.xxdb.data.BasicTable;
import com.xxdb.data.Entity;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class JDBCResultSetMetaData implements ResultSetMetaData{

    private final BasicTable table;

    public JDBCResultSetMetaData(BasicTable table){
        this.table = table;
    }

    @Override
    public String getCatalogName(int columnIndex) throws SQLException {
        return table.getColumn(adjustColumnIndex(columnIndex)).getDataCategory().name();
    }

    @Override
    public String getColumnClassName(int columnIndex) throws SQLException {
        return table.getColumn(adjustColumnIndex(columnIndex)).getClass().getName();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return table.columns();
    }

    @Override
    public String getColumnLabel(int columnIndex) throws SQLException {
        return table.getColumnName(adjustColumnIndex(columnIndex));
    }

    @Override
    public int getColumnDisplaySize(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnName(int columnIndex) throws SQLException {
        return table.getColumnName(adjustColumnIndex(columnIndex));
    }


    @Override
//    public int getColumnType(int columnIndex) throws SQLException {
//        return table.getColumn(adjustColumnIndex(columnIndex)).getDataType().ordinal();
//    }
    public int getColumnType(int columnIndex) throws SQLException {
        Entity.DATA_TYPE x = table.getColumn(adjustColumnIndex(columnIndex)).getDataType();
        switch (x){
            case DT_BOOL:
                return Types.BOOLEAN;
            case DT_BYTE:
                return Types.CHAR;
            case DT_SHORT:
                return Types.TINYINT;
            case DT_INT:
                return Types.INTEGER;
            case DT_LONG:
                return Types.BIGINT;
            case DT_DATE:
                return Types.DATE;
            case DT_TIME:
                return Types.TIME;
            case DT_DATETIME:
            case DT_TIMESTAMP:
                return Types.TIMESTAMP;
            case DT_FLOAT:
                return Types.FLOAT;
            case DT_DOUBLE:
                return Types.DOUBLE;
            case DT_STRING:
                return Types.VARCHAR;
            case DT_BLOB:
                return Types.VARCHAR;
            default:
                return Types.VARCHAR;
        }
    }

    @Override
    public String getColumnTypeName(int columnIndex) throws SQLException {
        return table.getColumn(adjustColumnIndex(columnIndex)).getDataType().name();
    }

    @Override
    public int getPrecision(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public String getSchemaName(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public String getTableName(int columnIndex) throws SQLException {
        return table.getColumnName(adjustColumnIndex(columnIndex));
    }

    @Override
    public boolean isAutoIncrement(int columnIndex) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int columnIndex) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int columnIndex) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int columnIndex) throws SQLException {
        return false;
    }

    @Override
    public boolean isReadOnly(int columnIndex) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int columnIndex) throws SQLException {
        return true;
    }

    @Override
    public boolean isSigned(int columnIndex) throws SQLException {
        return false;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int columnIndex) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int columnIndex) throws SQLException {
        return columnNullableUnknown;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    private int adjustColumnIndex(int columnIndex){
        return columnIndex-1;
    }
}
