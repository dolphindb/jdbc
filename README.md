#  DolphinDB JDBC


DolphinDB provides an implementation of the JDBC interface, allowing client programs that support the JDBC interface to directly access DolphinDB.

DolphinDB's JDBC interface is based on the `DolphinDB Java Api` implementation, so the JDBC package has a built-in DolphinDB Java Api package.

The JDBC interface mainly provides two interfaces, direct execution and pre-compilation, through the two objects `JDBCStatement` and `JDBCPrepareStatement`, respectively.


Here are a few sample programs to show how to use these two methods.


### 1. Add, delete, and update an in-memory table

First of all, we create a template table and save to disk through DolphinDB Java API. The code is as the following:

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
#### 1.1. Add new records to an in-memory table

The operation of in-memory table through the jdbc interface is mainly to preset the sql template through the `prepareStatement` method, 
and write the parameters through the set methods, and finally fill the parameters and execute the statement through the `executeUpdate` function.

```java
		public static void InMemmoryAddTest(Properties info, String database, String tableName)
    {
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url1,info);

            JDBCStatement stm = (JDBCStatement)conn.createStatement();
            stm.execute("memTable = loadTable('" + database + "','" + tableName + "')");
            //SQL insert statement
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

            //load table
            ResultSet rs = stmt.executeQuery("select * from memTable");
            printData(rs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
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

#### 1.2. Delete records from an in-memory table


To delete the contents of the data table,  you should fill in the corresponding deletion conditions at "?"

```java
	public static void InMemoryDeleteTest(Properties info, String database, String tableName)
    {
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url1);
            JDBCStatement stm = (JDBCStatement)conn.createStatement();
            stm.execute("memTable = loadTable('" + database + "','" + tableName + "')");
            // SQL delete statement
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

#### 1.3. Update in-memory table

Update the table contents

```java
	
	public static void InMemoryUpdateTest(Properties info, String database, String tableName)
    {
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url1);
            JDBCStatement stm = (JDBCStatement)conn.createStatement();
            stm.execute("memTable = loadTable('" + database + "','" + tableName + "')");
            //SQL update语句
            stmt = conn.prepareStatement("update memTable set bool = 0b where char = 97c");
            stmt.executeUpdate();
            // check if records have been deleted
            ResultSet rs = stmt.executeQuery("select * from memTable where char=97c");
        
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
 
### 2. Query or add records to a partitioned table

The example below demonstrates adding new records and querying of a partitioned table through JDBC. In order to connect to a partitioned table, 
you can add path and corresponding content to the URL when connecting, so that getConnection() will preload the metadata of the partition table.


##### Example：
```URL
jdbc:dolphindb://localhost:8848?databasePath=dfs://valuedb&partitionType=VALUE&partitionScheme=2000.01M..2019.05M
```

#### 2.1. Create a partitioned table

Use Java Api to create the partitioned table.

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
#### 2.2. Query and append to a partitioned table


```java
	public static void DFSAddTest(Properties info, String database, String tableName)
    {
        try {
            Class.forName(JDBC_DRIVER);

            //load the partitioned table
            conn = DriverManager.getConnection(url2,info);
            JDBCStatement stm = (JDBCStatement)conn.createStatement();
            stm.execute("dfsTable = loadTable('" + database + "','" + tableName + "')");
            //SQL insert statement
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

### 3 References
 
 In the JDBC interface, you can use the `excute` method to execute all DolphinDB Sql statements. 
 For details, see [DolphinDB Sql] [DolphinDB SQL](http://www.dolphindb.com/help/Chapter8SQLStatements.html) 

* [Download](sample.txt)sample code
