# DolphinDB JDBC API

DolphinDB 提供 JDBC 的接口的实现，可以让支持 JDBC 接口的客户端程序直接接入 DolphinDB。DolphinDB 的 JDBC 接口基于 DolphinDB Java API 的包实现。

JDBC 接口主要通过 `JDBCStatement`, `JDBCPrepareStatement` 与 `JDBCCallableStatement` 提供直接执行和预编译执行三种方式的接口。

|接口|介绍|
|----|-------|
|JDBCStatement|可以正常访问数据库，适用于运行静态 SQL 语句。 Statement 接口不接受参数。|
|JDBCPrepareStatement|继承了 JDBCStatement。可多次使用 SQL 语句， PreparedStatement 接口运行时接受输入的参数。|
|JDBCCallableStatement|继承了 JDBCPrepareStatement。支持通过分号（;）分隔多个 SQL 语句，执行时不使用存储过程。|

<!-- 待讨论包含方法不发是否需要放上去
|接口|介绍|包含方法|
|----|-------|-----|
|JDBCStatement|可以正常访问数据库，适用于运行静态 SQL 语句。 Statement 接口不接受参数。|execute, executeUpdate, executeQuery|
|JDBCPrepareStatement|继承了 JDBCStatement。可多次使用 SQL 语句， PreparedStatement 接口运行时接受输入的参数。|setInt, setFloat, setString, setDate, addBatch, setCharacterStream, setBinaryStream|
|JDBCCallableStatement|继承了 JDBCPrepareStatement（不调用存储过程）。支持通过分号（;）分隔多个 SQL 语句，实现多条语句串行执行。|registerOutParameter, setNull, setString, wasNull, getlnt|
-->

下面通过几个示例程序来展示以上三个对象的使用方法。

使用前，可以通过 maven 引入 JDBC：以 1.30.17.1 版本为例：

```xml
<dependency>
    <groupId>com.dolphindb</groupId>
    <artifactId>jdbc</artifactId>
    <version>1.30.17.1</version>
</dependency>
```

## 1. 连接 DolphinDB

  在支持 JDBC 连接的应用中，需要配置如下 JDBC 信息：

* **Driver Class Name**: 驱动名称
    DolphinDB JDBC 驱动名称是： `com.dolphindb.jdbc.Driver`。
* **url**: 连接字符串
    连接字符串提供连接数据库的一些关键信息。一个 DolphinDB JDBC url 示例如下：

    ```URL
    jdbc:dolphindb://localhost:8848?user=admin&password=123456
    ```

    URL 支持的参数如下：

    |参数|作用|
    |------|----------|
    |user|数据库用户名。用于连接数据库。|
    |password|用户密码。用于连接数据库。|
    |waitingTime|测试连接的超时时间，单位为秒，默认值为3。|
    |initialScript|传入函数定义脚本。|
    |allowMultiQueries|是否支持多条语句查询。布尔类型，默认为 false。在一条语句中，允许使用“;”来分隔多条查询。|
    |databasePath|分布式数据库路径。指定该参数可以在初始化时将分布式表加载到内存。|
    |tableName|分布式表的表名。指定该参数可以加载指定的分布式表。|
    |enableHighAvailability 或 highAvailability |高可用参数，布尔类型，默认为 false。指定该参数可以开启或关闭高可用模式。|
    |sqlStd|枚举类型，用于指定传入 SQL 脚本的解析语法。支持三种解析语法：DolphinDB、Oracle、MySQL，其中默认为 DolphinDB 解析。|
    |tableAlias|数据库表别名，用于在建立连接时传入一个或多个别名与数据库的组合。用户可通过别名访问数据库表。|
    
    **注：**

  * 自1.30.21.1版本起，JDBC 支持高可用参数 *enableHighAvailability*，其作用与 *highAvailability* 相同。使用时只需设置其中一个参数即可（推荐使用 *enableHighAvailability*），若配置冲突则会报错。
  * 若需要创建 JDBCCallableStatement 对象，则连接字符串须指定 `allowMultiQueries=true`。
  * 自1.30.22.1版本起，JDBC 支持参数 *sqlStd*。用户可通过 url 直接传参，见示例1；也可通过 JDBCConnection 构造方法的 *Properties* 进行传参，见示例2。注意：须使用2.00.10版本以上的 DolphinDB。

    **示例1**

    通过 url 直接传参。
    ```java
    Properties prop = new Properties();
    prop.setProperty("user","admin");
     prop.setProperty("password","123456");
    String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "sqlStd:" + SqlStdEnum.MySQL.getName();
    conn = new JDBCConnection(url,prop);
     ```

     **示例2**

    JDBCConnection 构造方法的 *Properties* 参数支持 sqlStd 属性。用户通过 *Properties* 设置属性 key 为 sqlStd，值为字符串，即用户通过 SqlStdEnum 指定传入 SQL 脚本的解析语法。使用示例如下：

    ```java
     Properties prop = new Properties();
     prop.setProperty("user","admin");
    prop.setProperty("password","123456");
    prop.setProperty("sqlStd", SqlStdEnum.DolphinDB.getName());
    String url = "jdbc:dolphindb://"+JDBCTestUtil.HOST+":"+JDBCTestUtil.PORT;
     conn = new JDBCConnection(url,prop);
     ```

* 自1.30.22.2版本起，JDBC 支持参数 *tableAlias*。用户可通过 url 直接传参，见示例3-示例5；也可通过 JDBCConnection 构造方法的 *Properties* 进行传参，见示例6。

    **示例3 DFS 表使用别名**

    1. 使用默认别名。下例中以表名 pt 做为别名。

    ```java
    tableAlias=dfs://valuedb/pt
    ```

    2. 指定别名。别名写在开头，别名与库表路径之间用冒号连接。下例中表的别名为 `ttt`。

    ```java
    tableAlias=ttt:dfs://valuedb/pt
    ```   

    3. 同时设置多个库表的别名。值与值之间通过逗号“,”进行分隔。

    ```java
    tableAlias=t1:dfs://valuedb/pt,dfs://valuedb/dt,dfs://valuedb/Order_tmp,dfs://testValue/nt
    ```

    **示例4 MVCC 表使用别名**

    1. 使用默认别名。下例中以 MVCC 表的名字 `mvcc13` 做为别名。

    ```java
    tableAlias=mvcc://work_dir/mvcc13
    ```

    2. 指定别名。下例中以 `mvcc13` 做为别名。

    ```java
    tableAlias=mvcc3:mvcc://work_dir/mvcc13
    ```

    3. linux server 中使用绝对路径指定别名。下例中使用 “/// ”表示绝对路径，表的别名为 `mvcc2`。

    ```java
    tableAlias=mvcc2:mvcc:///home/username/Adolphindb/2.00.6/server/work_dir/mvcc12；
    ```

    4. linux server 中使用相对路径指定别名。下例中使用“//” 表示相对路径，表的别名为 `mvcc3`。

    ```java
    tableAlias=mvcc3:mvcc://work_dir/mvcc13
    ```

    5. windows server 中指定别名。下例中表的别名为 `mvcc14`。

    ```java
    tableAlias=mvcc14:mvcc://C://DolphinDB/Data1/db12/mvcc14

    tableAlias=mvcc14:mvcc://C:\\DolphinDB\\Data1\\db12\\mvcc14；
    ```

    **示例5 共享内存表使用别名**

    指定别名。下例中以 `tb8` 做为表的别名。

    ```java
    tableAlias=tb8:memTb2
    ```

    **示例6 通过 Properties 指定别名**

    下例中指定表的别名为 `t1`。

    ```java
    Properties info = new Properties();
    info.put("tableAlias","t1:dfs://valuedb/pt");
    ```

* 自1.30.22.2版本起，JDBC 的高可用参数 *highAvailabilitySites* 支持通过逗号“,”分隔输入值。

    **示例7 多个值之间通过逗号“,”分割**（推荐写法）

    ```java
    highAvailabilitySites=192.168.1.111:8841,192.168.1.111:8842,192.168.1.111:8843,192.168.1.111:8844
    ```

    **示例8 多个值之间通过空格分割**（不推荐）

    ```java
    highAvailabilitySites=192.168.1.111:8841 192.168.1.111:8842 192.168.1.111:8843 192.168.1.111:8844
    ```

## 2. 内存表的增删改查

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

### 2.1 内存表新增记录

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

### 2.2 删除内存表中数据

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

### 2.3 内存表的更改

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

## 3. 分布式表的新增和查询

DolphinDB 支持分布式数据表。本例子演示通过 JDBC 来进行分布式表的新增和查询。要操作分布式表，连接的时候需在 URL 中加入 *databasePath*。`getConnection()` 时会预先加载分区表的元数据。

示例如下：

```URL
jdbc:dolphindb://localhost:8848?databasePath=dfs://valuedb
```

### 3.1 创建分区表

使用 Java API 执行创建分区表的语句，以创建示例所需的分区数据库。示例中使用了 VALUE 方式进行数据分区。需要了解其他分区方式，请点击 [DolphinDB 数据库分区教程](https://github.com/dolphindb/Tutorials_CN/blob/master/database.md)。

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

### 3.2 分区表内容的增加和查询

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

## 参考及附录

* 在 JDBC 接口中，可以使用 `execute` 方法执行所有的 DolphinDB SQL 语句，具体语法参考 [DolphinDB SQL 语法](https://www.dolphindb.cn/cn/help/SQLStatements/index.html)。
* JDBC 中 `executeUpdate(sql)` 返回 SQL 语句更新的记录数，而在 DolphinDB JDBC API 中 `executeUpdate(sql)` 不支持返回 delete, update 和调用 append 的语句所影响的记录数。
* 由于 DolphinDB 不支持更高精度的 BigDecimal 类型，故 DolphinDB JDBC API 将 BigDecimal 类型转换为 DOUBLE 类型。
* [下载](sample.txt) 示例所有代码。
