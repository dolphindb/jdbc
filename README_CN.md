#  DolphinDB JDBC

DolphinDB提供JDBC的接口的实现，可以让支持JDBC接口的客户端程序直接接入DolphinDB。DolphinDB的JDBC接口是基于 DolphinDB Java API 实现，所以JDBC包内置了 DolphinDB Java API 的包。

JDBC接口主要通过`JDBCStatement`与`JDBCPrepareStatement`这两个方法，来提供直接执行和预编译执行这两种方式的接口。

下面通过几个示例程序来展示这两个对象的使用方法。

## 1. 内存表的增删改查

使用Java API将demo需要的模板表保存到磁盘，在demo中通过loadTable可以快速创建内存表。脚本代码如下：

```java
public static boolean CreateTable(String database,String tableName,String host, String port)
{
    boolean success=false;
    DBConnection db = null;
    try {
        String sb="bool = [1b, 0b];\n" +
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
                "db=database(\""+database+"\")\n" +
                "saveTable(db, tb1, "+tableName+");\n";
        db = new DBConnection();
        db.connect(host, Integer.parseInt(port));
        db.run(sb);
        success=true;
    } catch (Exception e) {
        e.printStackTrace();
        success=false;
    } finally {
        if (db != null)
            db.close();
        return success;
    }
}
```
### 1.1. 内存表新增记录

通过JDBC接口对内存表的操作方式主要是通过prepareStatement的方式预置sql模板，并通过set方式写入参数，最后通过`executeUpdate`函数填充参数并执行语句。

```java
public static void InMemmoryAddTest(Properties info, String database, String tableName)
{
    try {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url1,info);
            
        JDBCStatement stm = (JDBCStatement)conn.createStatement();
        stm.execute("memTable = loadTable('" + database + "',\"" + tableName + "\")");
        //SQL insert语句
        stmt = conn.prepareStatement("insert into memTable values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        stmt.setBoolean(1,true);
        stmt.setByte(2,(byte)98);
        stmt.setShort(3,(short)112);
        stmt.setInt(4,21);
        stmt.setLong(5,22l);
        stmt.setFloat(6,2.1f);
        stmt.setDouble(7,2.1);
        stmt.setString(8,"hello");
        stmt.setDate(9, Date.valueOf(LocalDate.of(2013,06,13)));
        stmt.setObject(10,  YearMonth.of(2016,06));
        stmt.setObject(11, Time.valueOf("13:30:10"));
        stmt.setObject(12, LocalTime.of(13,30));
        stmt.setObject(13,LocalTime.of(13,30,10));
        stmt.setObject(14,LocalDateTime.of(2012,06,13,13,30,10));
        stmt.setObject(15,LocalDateTime.of(2012,06,13,13,30,10,8000000));
        stmt.setObject(16,LocalTime.of(13,30,10,8007006));
        stmt.setObject(17,LocalDateTime.of(2012,06,13,13,30,10,8007006));
        stmt.executeUpdate();

        //load数据库中的表格
        ResultSet rs = stmt.executeQuery("select * from memTable");
        printData(rs);
    } catch (Exception e) {
        e.printStackTrace();
    }
    finally {
        //释放
        try {
            if (stmt != null)
                stmt.close();
        } catch (SQLException se2) {
        }
        //释放
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

要删除内存表中数据，需在"?"处填相应的的删除条件。

```java
public static void InMemoryDeleteTest(Properties info, String database, String tableName)
{
    try {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url1);
        JDBCStatement stm = (JDBCStatement)conn.createStatement();
        stm.execute("memTable = loadTable('" + database + "',\"" + tableName + "\")");
        //SQL delete语句
        stmt = conn.prepareStatement("delete from memTable where char = ?");
        stmt.setByte(1, (byte)'A');
        stmt.executeUpdate();
        //读取表格检查是否删除
        ResultSet rs = stmt.executeQuery("select * from memTable");
        System.out.println("==========InMemoryDeleteTest======================");
        printData(rs);
    } catch (Exception e) {
        e.printStackTrace();
    } finally {
        //释放
        try {
            if (stmt != null)
                stmt.close();
        } catch (SQLException se2) {
        }
        //释放
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

对数据表内容更新

```java
public static void InMemoryUpdateTest(Properties info, String database, String tableName)
{
    try {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url1);
        JDBCStatement stm = (JDBCStatement)conn.createStatement();
        stm.execute("memTable = loadTable('" + database + "',\"" + tableName + "\"))");
        //SQL update语句
        stmt = conn.prepareStatement("update memTable set bool = 0b where char = 97c");
        stmt.executeUpdate();
        //读取表格检查是否更新
        ResultSet rs = stmt.executeQuery("select * from memTable where char=97c");
        printData(rs);        
        } catch (Exception e) {
            e.printStackTrace();
        } finally
        {
        //释放
        try {
            if (stmt != null)
                stmt.close();
        } catch (SQLException se2) {
        }
        //释放
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

DolphinDB支持分布式数据表，本例子中演示通过JDBC来进行分布式表的新增和查询。要操作分布式表，连接的时候可以在URL中加入path以及相应内容，这样getConnection()时会预先加载分区表的元数据。


#### Example：
```URL
jdbc:dolphindb://localhost:8848?databasePath=dfs://valuedb&partitionType=VALUE&partitionScheme=2000.01M..2019.05M
```

### 2.1. 创建分区表

使用Java API 来执行创建分区表的语句，创建示例所需的分区数据库。
示例中使用了VALUE方式进行数据分区。需要了解其他分区方式，请点击 [DolphinDB数据库分区教程](https://github.com/dolphindb/Tutorials_CN/blob/master/database.md) 

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
### 2.2. 分区表内容的增加和查询


```java
public static void DFSAddTest(Properties info, String database, String tableName)
{
    try {
        Class.forName(JDBC_DRIVER);

        //dfs下会预先load table
        conn = DriverManager.getConnection(url2,info);
        JDBCStatement stm = (JDBCStatement)conn.createStatement();
        stm.execute("dfsTable = loadTable('" + database + "',\"" + tableName + "\")");
        //SQL insert语句
        stmt = conn.prepareStatement("insert into dfsTable values(?,?)");
        stmt.setObject(1, new BasicMonth(YearMonth.of(2016,06)));
        stmt.setInt(2,3);
        stmt.executeUpdate();
        //读取表格检查是否新增数据
        ResultSet rs = stmt.executeQuery("select count(*) from loadTable(\""+database+"\", `"+tableName+")");
        printData(rs);
    } catch (Exception e) {
        e.printStackTrace();
    } finally {
        //释放
        try {
            if (stmt != null)
                stmt.close();
        } catch (SQLException se2) {
        }
        //释放
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
 
 * 在JDBC接口中，可以使用`excute`方法执行所有的DolphinDB SQL语句，具体语法可以参考[DolphinDB SQL语法](http://www.dolphindb.com/help/Chapter8SQLStatements.html) 

 * JDBC中executeUpdate（sql）返回值是sql语句更新的记录数，而在DolphinDB JDBC API中executeUpdate（sql）不支持返回delete、update和调用append的语句所影响的记录数。

 * 由于BigDecimal类型的精度较高，而DolphinDB没有此数据类型，故将其统一转换为Double类型。

 * [下载](sample.txt)示例所有代码