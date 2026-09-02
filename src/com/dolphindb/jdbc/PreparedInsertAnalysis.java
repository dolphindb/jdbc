package com.dolphindb.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * {@link Utils#analyzePreparedInsert} 的不可变结果：三态分类、表名、列、行形与真实 {@code ?} 位置。
 */
public final class PreparedInsertAnalysis {
    private final PreparedInsertKind kind;
    private final String tableName;
    private final List<String> columnNames;
    private final int valueCountPerRow;
    private final int rowsPerExecution;
    private final int[] parameterPositions;
    private final int[] parameterSqlColumnIndexes;
    private final String errorMessage;

    private PreparedInsertAnalysis(PreparedInsertKind kind, String tableName, List<String> columnNames,
                                   int valueCountPerRow, int rowsPerExecution, int[] parameterPositions,
                                   int[] parameterSqlColumnIndexes, String errorMessage) {
        this.kind = kind;
        this.tableName = tableName;
        this.columnNames = columnNames == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(columnNames));
        this.valueCountPerRow = valueCountPerRow;
        this.rowsPerExecution = rowsPerExecution;
        this.parameterPositions = parameterPositions == null
                ? new int[0]
                : Arrays.copyOf(parameterPositions, parameterPositions.length);
        this.parameterSqlColumnIndexes = parameterSqlColumnIndexes == null
                ? new int[0]
                : Arrays.copyOf(parameterSqlColumnIndexes, parameterSqlColumnIndexes.length);
        this.errorMessage = errorMessage;
    }

    static PreparedInsertAnalysis invalid(String sql, String detail) {
        String message = detail == null || detail.isEmpty()
                ? ("Please check your SQL format: " + sql)
                : detail;
        return new PreparedInsertAnalysis(PreparedInsertKind.INVALID_SQL, null, null, 0, 0,
                null, null, message);
    }

    static PreparedInsertAnalysis of(PreparedInsertKind kind, String tableName, List<String> columnNames,
                                     int valueCountPerRow, int rowsPerExecution, int[] parameterPositions,
                                     int[] parameterSqlColumnIndexes) {
        return new PreparedInsertAnalysis(kind, tableName, columnNames, valueCountPerRow, rowsPerExecution,
                parameterPositions, parameterSqlColumnIndexes, null);
    }

    public PreparedInsertKind getKind() {
        return kind;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public boolean hasExplicitColumns() {
        return !columnNames.isEmpty();
    }

    public int getValueCountPerRow() {
        return valueCountPerRow;
    }

    public int getRowsPerExecution() {
        return rowsPerExecution;
    }

    public int[] getParameterPositions() {
        return Arrays.copyOf(parameterPositions, parameterPositions.length);
    }

    public int getParameterCount() {
        return parameterPositions.length;
    }

    /**
     * JDBC 参数（0-based）对应的 VALUES 列序号；表达式内部参数记为 {@code -1}。
     */
    public int[] getParameterSqlColumnIndexes() {
        return Arrays.copyOf(parameterSqlColumnIndexes, parameterSqlColumnIndexes.length);
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
