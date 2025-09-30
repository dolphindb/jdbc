package com.dolphindb.jdbc;

import com.xxdb.data.Vector;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;

public class DolphinDBArray implements Array {

    private final Vector vector;
    private boolean freed = false;

    public DolphinDBArray(Vector vector) {
        this.vector = vector;
    }

    private void checkFreed() throws SQLException {
        if (freed) {
            throw new SQLException("This DolphinDBArray object has been freed.");
        }
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        checkFreed();
        return vector.getDataType().getName();
    }

    @Override
    public int getBaseType() throws SQLException {
        checkFreed();
        return Utils.transferColDefsTypesToSqlTypes(vector.getDataType().getName());
    }

    @Override
    public Object getArray() throws SQLException {
        checkFreed();
        return Utils.convertVectorToJavaObjectArray(vector);
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        if (map != null && !map.isEmpty()) {
            throw new SQLFeatureNotSupportedException("Type mapping is not supported in getArray(Map)");
        }
        return getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        checkFreed();
        if (index < 1 || index > vector.rows()) {
            throw new SQLException("Index out of bounds: " + index);
        }
        if (count < 0) {
            throw new SQLException("Count cannot be negative: " + count);
        }
        
        int startIndex = (int) (index - 1);
        int endIndex = Math.min(startIndex + count, vector.rows());
        
        if (vector.rows() > 0) {
            return Utils.convertVectorToJavaObjectArray(vector, startIndex, endIndex);
        }
        
        return new Object[0];
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        if (map != null && !map.isEmpty()) {
            throw new SQLFeatureNotSupportedException("Type mapping is not supported in getArray(long, int, Map)");
        }
        return getArray(index, count);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLFeatureNotSupportedException("The current method is not supported.");

    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("The current method is not supported.");
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException("The current method is not supported.");
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("The current method is not supported.");
    }

    @Override
    public void free() throws SQLException {
        freed = true;
    }

    @Override
    public String toString() {
        return vector.getString();
    }
}
