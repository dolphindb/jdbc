#  DolphinDB JDBC API

The DolphinDB JDBC interface enables compatible clients to connect to DolphinDB. The JDBC interface is implemented based on DolphinDB Java API package.

- [DolphinDB JDBC API](#dolphindb-jdbc-api)
  - [1. Configure JDBC Connection to DolphinDB](#1-configure-jdbc-connection-to-dolphindb)
  - [2. Operations on In-Memory Tables](#2-operations-on-in-memory-tables)
    - [2.1 Create a Template Table](#21-create-a-template-table)
    - [2.2 Append Data](#22-append-data)
    - [2.3 Delete Data](#23-delete-data)
    - [2.4 Update Data](#24-update-data)
  - [3. Operations on DFS Tables](#3-operations-on-dfs-tables)
    - [3.1. Create a DFS Table](#31-create-a-dfs-table)
    - [3.2 Query and Insert Data](#32-query-and-insert-data)
  - [References](#references)


DolphinDB JDBC interface mainly provides three types of interfaces for direct execution and precompiled execution through `JDBCStatement`, `JDBCPrepareStatement` and `JDBCCallableStatement`.

| Interface 	| Description 	|
|:---	|:---	|
| JDBCStatement 	| Can access databases and execute static SQL statements;<br>Does not take parameters. 	|
| JDBCPrepareStatement 	| Inherits from `JDBCStatement`;<br>Can execute SQL statements for multiple times and take runtime parameters. 	|
| JDBCCallableStatement 	| Inherits from `JDBCPrepareStatement`;<br>Can execute multiple SQL statements separated by semicolons(;), which does not use the stored procedures. 	|

You can use the following Maven dependency to import JDBC. For example:

```xml
<dependency>
    <groupId>com.dolphindb</groupId>
    <artifactId>jdbc</artifactId>
    <version>1.30.22.1</version>
</dependency>
```

The following sections demonstrate the usage of the three interfaces.

## 1. Configure JDBC Connection to DolphinDB

You can set up a JDBC connection to DolphinDB with the following parameters:

- Driver Class Name: The driver name. The DolphinDB JDBC driver name is `com.dolphindb.jdbc.Driver`.

- JDBC URL: The connection string. Normally it is a DolphinDB JDBC URL like: 

```url
jdbc:dolphindb://localhost:8848?user=admin&password=123456
```

It supports the following properties:

| Property 	| Description 	|
|:---	|:---	|
| user 	| The username for connecting to the database. 	|
| password 	| The password for connecting to the database.  	|
| waitingTime 	| The timeout (in seconds) for testing the connection. The default value is 3. 	|
| initialScript 	| The script that pre-defines functions.  	|
| allowMultiQueries 	| A Boolean value that specifies whether to allow multiple queries separated by ";" in a single statement. The default value is false.  	|
| databasePath 	| The path to a DFS database. Specifying this parameter to load the specified database during connection.  	|
| tableName 	| The name of a DFS table. Specifying this parameter to load the specified table during connection.  	|
| enableHighAvailability \| highAvailability 	| A Boolean value that specifies whether to enable or disable high availability. The default value is true. 	|
| sqlStd 	| An enumeration type, specifying the syntax to parse input SQL scripts. Three parsing syntaxes are supported: DolphinDB (default), Oracle, and MySQL. 	|

**Note**:

- Starting from version 1.30.22.1, JDBC supports parameter *sqlStd*. You can pass it through url (see example 1), or specify the connection property for the constructor JDBCConnection (see example 2).

- Starting from version 1.30.21.1, DolphinDB JDBC API supports *enableHighAvailability* property for connection strings, and the original *highAvailability* can be used as an alias. Configuration conflicts are reported if inconsistencies occur.

- To create a `JDBCCallableStatement` object, you must specify the property *allowMultiQueries*=true for the connection strings.

Example 1. Pass sqlStd through url:

```java
Properties prop = new Properties();
prop.setProperty("user","admin");
prop.setProperty("password","123456");
String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "sqlStd:" + SqlStdEnum.MySQL.getName();
conn = new JDBCConnection(url,prop);
```

Alternatively, you can set the attribute key as "sqlStd" and the value as a string (which specifies the parsing syntax through SqlStdEnum) for the _Properties_ of `JDBCConnection`.

Example 2.

```java
Properties prop = new Properties();
prop.setProperty("user","admin");
prop.setProperty("password","123456");
prop.setProperty("sqlStd", SqlStdEnum.DolphinDB.getName());
String url = "jdbc:dolphindb://"+JDBCTestUtil.HOST+":"+JDBCTestUtil.PORT;
conn = new JDBCConnection(url,prop);
```

## 2. Operations on In-Memory Tables

### 2.1 Create a Template Table

Use the following code to create a template table and save it to disk through DolphinDB Java API. You can use `loadTable` later to create an in-memory table quickly. Note that the variable names cannot be the same as DolphinDB keywords. 

```java
public static boolean CreateTable(String database, String tableName, String host, int port) {
    boolean success = false;
    DBConnection db = null;
    try {
        String sb = "bool = [1b, 0b];\n" +
            "char = [97c, 'A'];\n" +
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

### 2.2 Append Data

The append operation on in-memory tables through the JDBC interface is to first preset the SQL template through `JDBCPrepareStatement`, then write the parameters through the `set` method, and finally specify the parameters and execute the statement through the `executeUpdate` function.

```java
public static void InMemmoryAddTest(String database, String tableName) {
    try {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL_WITHLOGIN);

        JDBCStatement stm = (JDBCStatement) conn.createStatement();
        stm.execute("memTable = loadTable('" + database + "',\"" + tableName + "\")");
        // SQL insert statement
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

        // load table
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

### 2.3 Delete Data

To delete data from an in-memory table, fill in the corresponding conditions at "?".

```java
public static void InMemoryDeleteTest(String database, String tableName){
    try {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL_WITHLOGIN);
        JDBCStatement stm = (JDBCStatement)conn.createStatement();
        stm.execute("memTable = loadTable('" + database + "',\"" + tableName + "\")");
        //SQL delete statement
        stmt = conn.prepareStatement("delete from memTable where char = ?");
        stmt.setByte(1, (byte)'A');
        stmt.executeUpdate();
        // Check if the records have been deleted
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

### 2.4 Update Data

```java
public static void InMemoryUpdateTest(String database, String tableName){
    try {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(DB_URL_WITHLOGIN);
        JDBCStatement stm = (JDBCStatement)conn.createStatement();
        stm.execute("memTable = loadTable('" + database + "',\"" + tableName + "\")");
        // SQL update statement
        stmt = conn.prepareStatement("update memTable set bool = 0b where char = 97c");
        stmt.executeUpdate();
        // check if records have been updated
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

## 3. Operations on DFS Tables

The code examples below demonstrate querying of and appending to a DFS table through JDBC. In order to connect to a DFS table, you can specify path and corresponding content for databasePath to the URL when connecting, so that `getConnection()` will preload the metadata of the table.

**Example**

```URL
jdbc:dolphindb://localhost:8848?databasePath=dfs://valuedb&partitionType=VALUE&partitionScheme=2000.01M..2019.05M
```

### 3.1. Create a DFS Table

Use the following code to create a DFS database with VALUE-based partitions through DolphinDB Java API. 

```java
public static boolean CreateValueTable(String database, String tableName, String host, String port)
{
    boolean success=false;
    DBConnection db = null;
    StringBuilder sb = new StringBuilder();
    sb.append("login(\"admin\",\"123456\")\n");
    sb.append("n=3000\n");
    sb.append("month=take(2000.01M..2019.05M, n)\n");
    sb.append("x=take(1..1000, n)\n");
    sb.append("t=table(month, x)\n");
    sb.append("if(existsDatabase(\""+database+"\"))\n" +
              "			dropDatabase(\""+database+"\")\n");
    sb.append("db=database(\""+database+"\", VALUE, 2000.01M..2019.05M)\n");
    sb.append("pt = db.createPartitionedTable(t, `"+tableName+", `month)\n");
    sb.append("pt.append!(t)\n");
    db = new DBConnection();
    try {
        db.connect(host, Integer.parseInt(port));
        db.run(sb.toString());
        success=true;
    } catch (NumberFormatException | IOException e) {
        e.printStackTrace();
        success=false;
    }finally {
        if (db != null)
            db.close();
        return success;
    }
}
```
### 3.2 Query and Insert Data

```java
public static void DFSAddTest(Properties info, String database, String tableName)
{
    try {
        Class.forName(JDBC_DRIVER);
        
        // load the partitioned table
        conn = DriverManager.getConnection(url2,info);
        JDBCStatement stm = (JDBCStatement)conn.createStatement();
        stm.execute("dfsTable = loadTable('" + database + "',\"" + tableName + "\")");
        // SQL insert statement
        stmt = conn.prepareStatement("insert into dfsTable values(?,?)");
        stmt.setObject(1, new BasicMonth(YearMonth.of(2016,06)));
        stmt.setInt(2,3);
        stmt.executeUpdate();
        // query the table ot see if the records have been inserted.
        ResultSet rs = stmt.executeQuery("select count(*) from loadTable(\""+database+"\", `"+tableName+")");
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

## References

- You can use the `execute` method to execute all DolphinDB SQL statements with JDBC interface. For details, see [DolphinDB SQL](https://www.dolphindb.com/help/SQLStatements/index.html).

- The method `executeUpdate(sql)` returns the number of records updated by the SQL statements in JDBC, while with DolphinDB JDBC API, `executeUpdate(sql)` does not return the number of records involved in delete, update or append statements.

- Since DolphinDB does not support BigDecimal type, the JDBC API converts the BigDecimal data to the DOUBLE type.

- Download [sample code](sample.txt).
