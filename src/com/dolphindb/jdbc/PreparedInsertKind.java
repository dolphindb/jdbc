package com.dolphindb.jdbc;

/**
 * Prepared INSERT 三态分类（JAVAOS-1916）。
 * {@link Utils#parsePreparedInsertInfo} 仍只服务 tableInsert 物化路径；本枚举由
 * {@link Utils#analyzePreparedInsert} 返回，用于在构造期选择执行路径。
 */
public enum PreparedInsertKind {
    /** 全部槽位为 {@code ?} / 零参 {@code now()}，且多行形状一致 → BasicTable + tableInsert */
    TABLE_INSERT_COMPATIBLE,
    /** 结构合法但含函数、运算等表达式，或行间槽位形状不同 → 原生 INSERT SQL */
    SERVER_SQL_REQUIRED,
    /** 括号/行宽/列数/多语句等结构非法 → JDBC 直接报错，不透传 */
    INVALID_SQL
}
