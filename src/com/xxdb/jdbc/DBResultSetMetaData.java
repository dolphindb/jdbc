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
    public String getCatalogName(int column) throws SQLException {
        return table.getColumn(column).getDataCategory().name();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return table.getColumn(column).getClass().getName();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return table.columns();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return table.getColumnName(column);
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return table.getColumnName(column);
    }


    @Override
    public int getColumnType(int column) throws SQLException {
        return table.getColumn(column).getDataType().ordinal();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return table.getColumn(column).getDataType().name();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return table.getColumnName(column);
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullableUnknown;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }
}
