# JDBC 发行说明

## 新功能

* 方法 getObject 新增支持日期时间类型 LocalDate, LocalTime, LocalDateTime, java.util.Date, Timestamp。（**1.30.21.4**）
* 新增连接属性 tableName，可以加载指定的表。（**1.30.21.1**）
* 新增高可用配置参数 *enableHighAvailability*，与 *highAvailability* 功能相同。使用时只需设置其中一个参数即可（推荐使用 *enableHighAvailability*），若配置冲突则会报错。（**1.30.21.1**）

## 改进

* 只连接一个节点时，若未设置高可用参数，则默认在连接失败或因网络问题导致连接断开时，都会自动进行重连。（**1.30.21.1**）

## 故障修复

* 修复连接 DolphinDB 时若指定 database 和 tableName，在断开重连后出现查询数据异常的问题。（**1.30.21.4**）
* 修复 JDBC PrepareStatement 使用 setString() 时存在字符串类型转换的问题。 （**1.30.21.4**）
* 修复了执行 SQL 语句时，语句中与关键字相同的字符串被转成小写的问题。 （**1.30.21.3**）
* 修复若 URL 设置的 databasePath 中包含无权限的表，则出现报错且无法连接的问题。（**1.30.21.1**）