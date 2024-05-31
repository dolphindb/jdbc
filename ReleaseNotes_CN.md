# JDBC 发行说明

> 注意：该文档不再进行维护。用户可移步至 DolphinDB 官方文档中心 [JDBC 版本说明](https://docs.dolphindb.cn/zh/rn/api/rn_jdbc.html)。 

**注意：为提升用户体验，DolphinDB JDBC 连接器的版本号现已调整。新的版本号与 DolphinDB Server 200 系列对齐。**

## 2.00.11.1

### 新增功能

- `JDBCConnection` 类的 `connect` 方法新增配置参数 *enableLoadBalance*，支持开启或关闭高可用模式下的负载均衡功能。

### 功能优化

-  `JDBCResultSet` 类的 `getInt`、`getLong`、`getDouble` 、`getString`、`getBigDecimal` 方法支持隐式类型转换。
- `JDBCPrepareStatement` 类执行 `insert` 操作时，兼容列名大小写不一致的场景。
- 重构新建 `JDBCConnection` 对象时获取连接信息的逻辑。
- 优化 `JDBCStatement` 类的 `executeUpdate` 方法在执行 `insert into` 内存表时的内部逻辑。

### 故障修复

- 修复在如输错端口号、IP 等情况下，连接服务器失败时出现无限重连的问题。
- 修复 `JDBCPrepareStatement` 执行 SQL 语句时，若 where 语句中包含括号时报错的问题。
- 修复 `JDBCPrepareStatement` 类的 `executeBatch` 方法在执行时，*batchsize* 参数未重置的问题。

## 1.30.22.5

### 新增功能

* `JDBCDataBaseMetaData` 类新增支持 `setCatalog` 方法，用来设置给定的数据库名称，以便选择此 Connection 对象所连接中的指定数据库来进行操作。
* `JDBCDataBaseMetaData` 类新增支持 `getCatalogs` 方法，用来获取数据库列表。
* `JDBCDataBaseMetaData` 类新增支持 `getTables` 方法，用来获取指定的表信息。
* `JDBCDataBaseMetaData` 类新增支持 `getColumns` 方法，用来获取指定的列信息。
* `JDBCResultSet` 类新增支持 `getBigDecimal` 方法，用来检索当前对象的指定列中类型为 `BigDecimal` 的数据。现提供如下两种方法：

  ```java
  BigDecimal getBigDecimal(int columnIndex) throws SQLException;
  BigDecimal getBigDecimal(String columnLabel) throws SQLException;
  ```

* `JDBCStatement` 类新增支持 `setMaxRows` 方法，用来设置由 `JDBCStatement` 生成的任何 `ResultSet` 对象所能包含的最大行数限制。
* `JDBCStatement` 类新增支持 `getMaxRows` 方法，用来获取由 `JDBCStatement` 生成的任何 `ResultSet` 对象可以包含的最大行数。
* `JDBCPrepareStatement` 类的 insert into 语句支持插入部分字段，未填写的字段则插入空值。

### 功能优化

* `JDBCConnection` 类的 `commit()`、`rollback()` 方法不支持用户级别事务操作，默认返回 null。
* `JDBCPrepareStatement` 类的 insert into 语句从逐条插入变为批量插入。

## 1.30.22.4

### 新增功能

* 增支持返回结果为标准 Java 数据类型。
* DolphinDB JDBC 新增支持 JDBC 4.0版本，用户在创建连接时无需再设置Class.forName("com.dolphindb.jdbc.Driver")。

### 功能优化

* 轻量化 DolphinDB JDBC 依赖的 jar 包。

## 1.30.22.3

### 功能优化

* 接口 JDBCResultSetData 优化了方法 getColumnType(int columnIndex) 的实现原理，以使其通用化。

## 1.30.22.2

### 新增功能

* 新功能：新增配置参数 *tableAlias*，支持通过别名访问数据库表。
* 新功能：配置参数 *highAvailablitySites* 新增支持通过逗号“,”分隔输入值。

## 1.30.22.1

### 新增功能

* 新增支持 DECIMAL128 数据类型。
* 新增支持连接属性 sqlStd，用于指定解析 SQL 语句的语法。

## 1.30.21.4

### 新增功能

* 方法 getObject 新增支持日期时间类型 LocalDate, LocalTime, LocalDateTime, java.util.Date, Timestamp。

### 故障修复

* 修复连接 DolphinDB 时若指定 database 和 tableName，在断开重连后出现查询数据异常的问题。
* 修复 JDBC PrepareStatement 使用 setString() 时存在字符串类型转换的问题。

## 1.30.21.3
  
### 故障修复

* 修复了执行 SQL 语句时，语句中与关键字相同的字符串被转成小写的问题。 

## 1.30.21.1

### 新增功能

* 新增连接属性 tableName，可以加载指定的表。
* 新增高可用配置参数 *enableHighAvailability*，与 *highAvailability* 功能相同。使用时只需设置其中一个参数即可（推荐使用 *enableHighAvailability*），若配置冲突则会报错。

### 功能优化

* 只连接一个节点时，若未设置高可用参数，则默认在连接失败或因网络问题导致连接断开时，都会自动进行重连。

### 故障修复

* 修复若 URL 设置的 databasePath 中包含无权限的表，则出现报错且无法连接的问题。
