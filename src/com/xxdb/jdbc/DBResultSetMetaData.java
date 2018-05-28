package com.xxdb.jdbc;

import com.xxdb.data.BasicTable;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class DBResultSetMetaData implements ResultSetMetaData{

    private BasicTable table;

    public DBResultSetMetaData(BasicTable table){
        this.table = table;
    }


    @Override
    public String getCatalogName(int columnIndex) throws SQLException {
        return table.getColumn(adjustcolumnIndex(columnIndex)).getDataCategory().name();
    }

    @Override
    public String getColumnClassName(int columnIndex) throws SQLException {
        return table.getColumn(adjustcolumnIndex(columnIndex)).getClass().getName();
    }

    @Override
    public int getColumnCount() throws SQLException {
        //throw new SQLException(""+table.columnIndexs());
        return table.columns();
    }

    @Override
    public String getColumnLabel(int columnIndex) throws SQLException {
        //throw new SQLException(""+columnIndex);
        return table.getColumnName(adjustcolumnIndex(columnIndex));
    }

    @Override
    public int getColumnDisplaySize(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnName(int columnIndex) throws SQLException {
        return table.getColumnName(adjustcolumnIndex(columnIndex));
    }


    @Override
    public int getColumnType(int columnIndex) throws SQLException {
        return table.getColumn(adjustcolumnIndex(columnIndex)).getDataType().ordinal();
    }

    @Override
    public String getColumnTypeName(int columnIndex) throws SQLException {
        return table.getColumn(adjustcolumnIndex(columnIndex)).getDataType().name();
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
        return table.getColumnName(adjustcolumnIndex(columnIndex));
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
        return false;
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

    private int adjustcolumnIndex(int columnIndexIndex){
        return columnIndexIndex-1;
    }
}
