# DolphinDB JDBC API

DolphinDB 提供 JDBC 的接口的实现，可以让支持 JDBC 接口的客户端程序直接接入 DolphinDB。DolphinDB 的 JDBC 接口基于 DolphinDB Java API 实现，所以 JDBC 包内置了 DolphinDB Java API 的包。

JDBC 接口主要通过 `JDBCStatement`, `JDBCPrepareStatement` 与 `JDBCCallableStatement` 提供直接执行和预编译执行三种方式的接口。

|接口|介绍|
|----|-------|
|JDBCStatement|可以正常访问数据库，适用于运行静态 SQL 语句。 Statement 接口不接受参数。|
|JDBCPrepareStatement|继承了 JDBCStatement。可多次使用 SQL 语句， PreparedStatement 接口运行时接受输入的参数。|
|JDBCCallableStatement|继承了 JDBCPrepareStatement（不调用存储过程）。支持通过分号（;）分隔多个 SQL 语句。|

<!-- 待讨论包含方法不发是否需要放上去
|接口|介绍|包含方法|
|----|-------|-----|
|JDBCStatement|可以正常访问数据库，适用于运行静态 SQL 语句。 Statement 接口不接受参数。|execute, executeUpdate, executeQuery|
|JDBCPrepareStatement|继承了 JDBCStatement。可多次使用 SQL 语句， PreparedStatement 接口运行时接受输入的参数。|setInt, setFloat, setString, setDate, addBatch, setCharacterStream, setBinaryStream|
|JDBCCallableStatement|继承了 JDBCPrepareStatement（不调用存储过程）。支持通过分号（;）分隔多个 SQL 语句，实现多条语句串行执行。|registerOutParameter, setNull, setString, wasNull, getlnt|
-->

下面通过几个示例程序来展示以上三个对象的使用方法。

使用前，可以通过 maven 引入 JDBC：以 1.30.17.1 为例

```xml
<dependency>
    <groupId>com.dolphindb</groupId>
    <artifactId>jdbc</artifactId>
    <version>1.30.17.1</version>
</dependency>
```

## 1. 内存表的增删改查

使用 Java API 将 demo 需要的模板表保存到磁盘。在 demo 中通过 loadTable 可以快速创建内存表。请注意，变量名及表名不可与 DolphinDB 的关键字同名。脚本代码如下：

```java
public static boolean CreateTable(String database, String tableName, String host, int port) {
    boolean success = false;
    DBConnection db = null;
    try {
        String sb = "bool = [1b, 0b];\n" +
            "char = [97c,'A'];\n" +
            "short = [122h, 123h];\n" +
            "int = [21, 22];\n" +
            "long = [22l, 23l];\n" +
            "float  = [2.1f, 2.2f];\n" +
            "double = [2.1, 2.2];\n" +
            "string= [`Hello, `world];\n" +
            "date = [2013.06.13, 2013.06.14];\n" +
            "month = [2016.06M, 2016.07M];\n" +
            "time = [13:30:10.008, 13:30:10.009];\n" +
            "minute = [13:30m, 13:31m];\n" +
            "second = [13:30:10, 13:30:11];\n" +
            "datetime = [2012.06.13 13:30:10, 2012.06.13 13:30:10];\n" +
            "timestamp = [2012.06.13 13:30:10.008, 2012.06.13 13:30:10.009];\n" +
            "nanotime = [13:30:10.008007006, 13:30:10.008007007];\n" +
            "nanotimestamp = [2012.06.13 13:30:10.008007006, 2012.06.13 13:30:10.008007007];\n" +
            "tb1= table(bool,char,short,int,long,float,double,string,date,month,time,minute,second,datetime,timestamp,nanotime,nanotimestamp);\n" +
            "db=database(\"" + database + "\");\n" +
            "saveTable(db, tb1, `" + tableName + ");\n";
        db = new DBConnection();
        db.connect(host, port);
        db.run(sb);
        success = true;
    } catch (Exception e) {
        e.printStackTrace();
        success = false;
    } finally {
        if (db != null)
            db.close();
        return success;
    }
}
```

### 1.1. 内存表新增记录

通过 JDBC 接口对内存表的操作主要是通过 prepareStatement 预置 sql 模板，并通过 set 写入参数，最后通过 `executeUpdate` 函数填充参数并执行语句。

```java
public static void InMemmoryAddTest(String database, String tableName) {
    try {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL_WITHLOGIN);

        JDBCStatement stm = (JDBCStatement) conn.createStatement();
        stm.execute("memTable = loadTable('" + database + "',\"" + tableName + "\")");
        // SQL insert 语句
        stmt = conn.prepareStatement("insert into memTable values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        stmt.setBoolean(1, true);
        stmt.setByte(2, (byte) 98);
        stmt.setShort(3, (short) 112);
        stmt.setInt(4, 21);
        stmt.setLong(5, 22l);
        stmt.setFloat(6, 2.1f);
        stmt.setDouble(7, 2.1);
        stmt.setString(8, "hello");
        stmt.setDate(9, Date.valueOf(LocalDate.of(2013, 06, 13)));
        stmt.setObject(10, new BasicMonth(YearMonth.of(2016, 06)));
        stmt.setObject(11, Time.valueOf("13:30:10"));
        stmt.setObject(12, LocalTime.of(13, 30));
        stmt.setObject(13, LocalTime.of(13, 30, 10));
        stmt.setObject(14, LocalDateTime.of(2012, 06, 13, 13, 30, 10));
        stmt.setObject(15, LocalDateTime.of(2012, 06, 13, 13, 30, 10, 8000000));
        stmt.setObject(16, LocalTime.of(13, 30, 10, 8007006));
        stmt.setObject(17, LocalDateTime.of(2012, 06, 13, 13, 30, 10, 8007006));
        stmt.executeUpdate();

        // 加载数据库中的表格
        ResultSet rs = stmt.executeQuery("select * from memTable");
        printData(rs);
    } catch (Exception e) {
        e.printStackTrace();
    } finally {
        try {
            if (stmt != null)
                stmt.close();
        } catch (SQLException se2) {
        }
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }
}
```

### 1.2. 删除内存表中数据

需要删除内存表中数据，需在以下脚本的 "?" 处填相应的的删除条件。

```java
public static void InMemoryDeleteTest(String database, String tableName){
    try {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL_WITHLOGIN);
        JDBCStatement stm = (JDBCStatement)conn.createStatement();
        stm.execute("memTable = loadTable('" + database + "',\"" + tableName + "\")");
        // SQL delete 语句
        stmt = conn.prepareStatement("delete from memTable where char = ?");
        stmt.setByte(1, (byte)'A');
        stmt.executeUpdate();
        // 读取表格检查是否删除
        ResultSet rs = stmt.executeQuery("select * from memTable");
        System.out.println("==========InMemoryDeleteTest======================");
        printData(rs);
    } catch (Exception e) {
        e.printStackTrace();
    } finally {
        try {
            if (stmt != null)
                stmt.close();
        } catch (SQLException se2) {
        }
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }
}
```

### 1.3. 内存表的更改

```java
public static void InMemoryUpdateTest(String database, String tableName){
    try {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL_WITHLOGIN);
        JDBCStatement stm = (JDBCStatement)conn.createStatement();
        stm.execute("memTable = loadTable('" + database + "',\"" + tableName + "\")");
        // SQL update 语句
        stmt = conn.prepareStatement("update memTable set bool = 0b where char = 97c");
        stmt.executeUpdate();
        // 读取表格检查是否更新
        ResultSet rs = stmt.executeQuery("select * from memTable where char=97c");
        printData(rs);
    } catch (Exception e) {
        e.printStackTrace();
    } finally
    {
        try {
            if (stmt != null)
                stmt.close();
        } catch (SQLException se2) {
        }
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }
}
```

## 2. 分布式表的新增和查询

DolphinDB 支持分布式数据表。本例子演示通过 JDBC 来进行分布式表的新增和查询。要操作分布式表，连接的时候需在 URL 中加入 databasePath。`getConnection()` 时会预先加载分区表的元数据。

#### Example

```URL
jdbc:dolphindb://localhost:8848?databasePath=dfs://valuedb
```

### 2.1. 创建分区表

使用 Java API 执行创建分区表的语句，以创建示例所需的分区数据库。示例中使用了 VALUE 方式进行数据分区。需要了解其他分区方式，请点击 [DolphinDB 数据库分区教程](https://github.com/dolphindb/Tutorials_CN/blob/master/database.md)

```java
public static boolean CreateValueTable(String database, String tableName, String host, String port) {
    boolean success = false;
    DBConnection db = null;
    StringBuilder sb = new StringBuilder();
    sb.append("login(\"admin\",\"123456\")\n");
    sb.append("n=3000\n");
    sb.append("month=take(2000.01M..2019.05M, n)\n");
    sb.append("x=take(1..1000, n)\n");
    sb.append("t=table(month, x)\n");
    sb.append("if(existsDatabase(\"" + database + "\"))\n" +
              "dropDatabase(\"" + database + "\")\n");
    sb.append("db=database(\"" + database + "\", VALUE, 2000.01M..2019.05M)\n");
    sb.append("pt = db.createPartitionedTable(t, `" + tableName + ", `month)\n");
    sb.append("pt.append!(t)\n");
    db = new DBConnection();
    try {
        db.connect(host, Integer.parseInt(port));
        db.run(sb.toString());
        success = true;
    } catch (NumberFormatException | IOException e) {
        e.printStackTrace();
        success = false;
    } finally {
        if (db != null)
            db.close();
        return success;
    }
}
```

### 2.2. 分区表内容的增加和查询

```java
public static void DFSAddTest(String database, String tableName) {
    try {
        Class.forName(JDBC_DRIVER);

        // dfs 下会预先 load table
        conn = DriverManager.getConnection(DB_URL_WITHLOGIN);
        JDBCStatement stm = (JDBCStatement) conn.createStatement();
        stm.execute("dfsTable = loadTable('" + database + "',\"" + tableName + "\")");
        // SQL insert 语句
        stmt = conn.prepareStatement("insert into dfsTable values(?,?)");
        stmt.setObject(1, new BasicMonth(YearMonth.of(2016, 06)));
        stmt.setInt(2, 3);
        stmt.executeUpdate();
        // 读取表格检查是否新增数据
        ResultSet rs = stmt.executeQuery("select count(*) from loadTable(\"" + database + "\", `"+ tableName +")");
        printData(rs);
    } catch (Exception e) {
        e.printStackTrace();
    } finally {
        try {
            if (stmt != null)
                stmt.close();
        } catch (SQLException se2) {
        }
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }
}
```

## 3 参考及附录

* 在 JDBC 接口中，可以使用 `execute` 方法执行所有的 DolphinDB SQL 语句，具体语法参考 [DolphinDB SQL 语法](https://www.dolphindb.cn/cn/help/SQLStatements/index.html)。
* JDBC 中 executeUpdate(sql) 返回 SQL 语句更新的记录数，而在 DolphinDB JDBC API 中 executeUpdate(sql) 不支持返回 delete, update 和调用 append 的语句所影响的记录数。
* 由于 DolphinDB 不支持更高精度的 BigDecimal 类型，故 DolphinDB JDBC API 将 BigDecimal 类型转换为 DOUBLE 类型。
* [下载](sample.txt) 示例所有代码。

## 4 如何在支持 JDBC 的软件中配置 JDBC 连接 DolphinDB

  在支持 JDBC 连接的应用中，需要配置如下 JDBC 信息：

* Driver Class Name: 驱动名称
    DolphinDB JDBC 驱动名称是： `com.dolphindb.jdbc.Driver`
* JDBC Url: 连接字符串
    连接字符串提供连接数据库的一些关键信息，通常为一个 DolphinDB JDBC Url，示例如下：

    ```URL
    jdbc:dolphindb://localhost:8848?user=admin&password=123456
    ```

    URL 支持的参数如下：

    |参数|作用|
    |------|----------|
    |user|数据库用户名（用于连接数据库）|
    |password|用户密码（用于连接数据库）|
    |waitingTime|测试连接的超时时间，单位为秒，默认值为3。|
    |initialScript|传入函数定义脚本|
    |allowMultiQueries|在一条语句中，允许使用“;”来分隔多条查询（布尔类型，默认为 false）。|
    |databasePath|分布式数据库路径。指定该参数可以在初始化时将分布式表加载到内存。|
    |tableName|分布式表的表名。指定该参数可以加载指定的分布式表。|
    |enableHighAvailability|高可用参数，布尔类型，默认为 true。指定该参数可以开启或关闭高可用模式。|
    |enableHighAvailability \| highAvailability |高可用参数，布尔类型，默认为 true。指定该参数可以开启或关闭高可用模式。|

    **注：** 自1.30.21.1版本起，JDBC 支持高可用参数 *enableHighAvailability*，其作用与 *highAvailability* 相同。使用时只需设置其中一个参数即可（推荐使用 *enableHighAvailability*），若配置冲突则会报错。

    若需要创建 JDBCCallableStatement 对象，则连接字符串须指定 allowMultiQueries=true。
