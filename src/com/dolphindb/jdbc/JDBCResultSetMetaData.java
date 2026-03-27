package com.dolphindb.jdbc;

import com.xxdb.data.BasicArrayVector;
import com.xxdb.data.BasicDecimal128Vector;
import com.xxdb.data.BasicDecimal32Vector;
import com.xxdb.data.BasicDecimal64Vector;
import com.xxdb.data.BasicTable;
import com.xxdb.data.Vector;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;

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
        if (Objects.nonNull(table))
            return table.columns();
        else
            return -1;
    }

    @Override
    public String getColumnLabel(int columnIndex) throws SQLException {
        return table.getColumnName(adjustColumnIndex(columnIndex));
    }

    @Override
    public int getColumnDisplaySize(int columnIndex) throws SQLException {
        return getPrecision(columnIndex);
    }

    @Override
    public String getColumnName(int columnIndex) throws SQLException {
        return table.getColumnName(adjustColumnIndex(columnIndex));
    }


    @Override
    public int getColumnType(int columnIndex) throws SQLException {
        Vector column = table.getColumn(adjustColumnIndex(columnIndex));
        if (column instanceof BasicArrayVector) {
            return Types.ARRAY;
        }
        switch (column.getDataType()) {
            case DT_BOOL:
                return Types.BOOLEAN;
            case DT_BYTE:
                return Types.CHAR;
            case DT_SHORT:
                return Types.SMALLINT;
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
            case DT_NANOTIMESTAMP:
                return Types.TIMESTAMP;
            case DT_FLOAT:
                return Types.FLOAT;
            case DT_DOUBLE:
                return Types.DOUBLE;
            case DT_DECIMAL32:
            case DT_DECIMAL64:
            case DT_DECIMAL128:
                return Types.DECIMAL;
            case DT_STRING:
                return Types.VARCHAR;
            case DT_BLOB:
                return Types.CLOB;
            default:
                return Types.OTHER;
        }
    }

    @Override
    public String getColumnTypeName(int columnIndex) throws SQLException {
        return table.getColumn(adjustColumnIndex(columnIndex)).getDataType().name();
    }

    @Override
    public int getPrecision(int columnIndex) throws SQLException {
        Vector column = table.getColumn(adjustColumnIndex(columnIndex));
        int temporalScale = getTemporalScale(column);
        switch (column.getDataType()) {
            case DT_DATE:
                return 10;
            case DT_MONTH:
                return 7;
            case DT_MINUTE:
                return 5;
            case DT_SECOND:
                return 8;
            case DT_DATEHOUR:
                return 13;
            case DT_DATETIME:
                return 19;
            case DT_TIME:
            case DT_NANOTIME:
                return 8 + (temporalScale > 0 ? 1 + temporalScale : 0);
            case DT_TIMESTAMP:
            case DT_NANOTIMESTAMP:
                return 19 + (temporalScale > 0 ? 1 + temporalScale : 0);
            case DT_DECIMAL32:
            case DT_DECIMAL64:
            case DT_DECIMAL128:
                return getDecimalScale(column);
            default:
                return 0;
        }
    }

    @Override
    public int getScale(int columnIndex) throws SQLException {
        Vector column = table.getColumn(adjustColumnIndex(columnIndex));
        if (column instanceof BasicArrayVector) {
            return 0;
        }
        switch (column.getDataType()) {
            case DT_TIME:
            case DT_TIMESTAMP:
                return 3;
            case DT_NANOTIME:
            case DT_NANOTIMESTAMP:
                return 9;
            case DT_DECIMAL32:
            case DT_DECIMAL64:
            case DT_DECIMAL128:
                return getDecimalScale(column);
            default:
                return 0;
        }
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

    private int getDecimalScale(Vector column) {
        if (column instanceof BasicDecimal32Vector) {
            return ((BasicDecimal32Vector) column).getScale();
        }
        if (column instanceof BasicDecimal64Vector) {
            return ((BasicDecimal64Vector) column).getScale();
        }
        if (column instanceof BasicDecimal128Vector) {
            return ((BasicDecimal128Vector) column).getScale();
        }
        return 0;
    }

    private int getTemporalScale(Vector column) {
        switch (column.getDataType()) {
            case DT_TIME:
            case DT_TIMESTAMP:
                return 3;
            case DT_NANOTIME:
            case DT_NANOTIMESTAMP:
                return 9;
            default:
                return 0;
        }
    }
}
