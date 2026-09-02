package com.dolphindb.jdbc;

/**
 * Prepared INSERT VALUES 槽位类型（JAVAOS-1913）。
 * 本期仅支持占位符与 {@code now()}。
 */
public enum InsertValueSlot {
    PLACEHOLDER,
    NOW
}
