# DolphinDB JDBC API

DolphinDB提供JDBC的接口的实现，可以让支持JDBC接口的客户端程序直接接入DolphinDB。DolphinDB的JDBC接口是基于 DolphinDB Java API 实现，所以JDBC包内置了 DolphinDB Java API 的包。

JDBC接口主要通过`JDBCStatement`与`JDBCPrepareStatement`这两个方法，来提供直接执行和预编译执行这两种方式的接口。

文档将从建立连接，内存表、流表以及分布式表的`JDBCStatement`和`JDBCPrepareStatement`介绍基本操作。教程的所有代码编写于`JDBCBasicInterfaceTest`测试类中，该类包含如下基础字段和方法。

```java
public class JDBCBasicInterfaceTest {
    private Properties properties = new Properties();
    private String url = null;
    private String dataBase = null;
    private String tableName = null;

    private Statement statement;
    private PreparedStatement preparedStatement;
    //用于添加操作
    private PreparedStatement preAdd;
    //用于删除操作
    private PreparedStatement preDel;
    //用于修改操作
    private PreparedStatement preMod;
    //用于查询操作
    private PreparedStatement preSel;
    private Connection connection;
}
```

## 建立数据库连接

在properties中配置`user`和`password`，加载DolphinDB的驱动类`com.dolphindb.jdbc.Driver`，并使用JDBC规范的DriverManager.getConnection建立与DolphinDB的数据连接。

```java
    @Before
    public void init() throws ClassNotFoundException, SQLException {
        properties.put("user", "admin");
        properties.put("password", "123456");
        url = "jdbc:dolphindb://192.168.56.10:9002";
        Class.forName("com.dolphindb.jdbc.Driver");
        connection = DriverManager.getConnection(url, properties);
    }
```

## 获取查询后的数据

下文的查询操作所获得的结果，由ResultSet进行保存，并提供遍历的方法。

`ResultSet.getMetaData()`方法可以获取表的结构，其返回的类为`ResultSetMetaData`，通过`ResultSetMetaData`可以获得表的列数，以及表头属性名等。获得的表的列数作为`ResultSet.getString()`的参数，从结果中指定要获取记录的某一列。`ResultSet.next()`判断当前遍历中，表是否存在下一条记录，如果存在下一条记录，则将其内部的游标移动到下一条数据，并且返回true，如果没有，则返回false。

```java
    private void print_table(ResultSet resultSet) throws SQLException {
        try {
            if(resultSet == null) return;
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int index = 1; index <= columnCount; ++index) {
                System.out.print(metaData.getColumnName(index) + "\t");
            }
            System.out.print("\n");
            while (resultSet.next()) {
                for (int index = 1; index <= columnCount; ++index) {
                    System.out.print(resultSet.getString(index)+"\t");
                }
                System.out.print("\n");
            }
            System.out.print("\n");
        } finally {
            if(resultSet != null) resultSet.close();
        }
    }
```

## 内存表的相关操作

### Statement

#### execute

使用`Statement.execute()`运行建表脚本，创建内存表。

```java
    private void create_memory_table() throws SQLException {
        try {
            statement = connection.createStatement();
            // 创建内存表
            statement.execute("n = 5\n" +
                    "id =1..n\n" +
                    "x = 1..n\n" +
                    "test = table(id as `id, x as `x)");
        } finally {
            if(statement != null) statement.close();
        }
    }
```

#### executeQuery

使用`Statement.executeQuery()`执行查询脚本，并使用ResultSet接收查询结果。ResultSet中结果的遍历可以参考`JDBCBasicInterfaceTest`中的`print_table()`方法。`Statement.executeQuery()`仅可用于查询语句的执行。

```java
    public void stat_executeQuery_memory_table() throws SQLException, ClassNotFoundException {
        try {
            statement = connection.createStatement();
            create_memory_table();
            // 查询内存表
            ResultSet resultSet = statement.executeQuery("select * from test");
            //遍历查询结果
            System.out.println("查询结果如下：");
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
```

#### executeUpdate

`Statement.executeUpdate()`用于执行insert、update、delete操作

```java
    public void stat_executeUpdate_memory_table() throws SQLException {
        try {
            statement = connection.createStatement();
            create_memory_table();
            //查看修改前的结果
            System.out.println("==============修改前的表==============");
            ResultSet resultSet = statement.executeQuery("select * from test");
            print_table(resultSet);
            statement.executeUpdate("insert into test values(11, 11)");
            statement.executeUpdate("update test set id = 12 where id = 11");
            statement.executeUpdate("delete from test where id = 5");
            //查看update之后结果
            resultSet = statement.executeQuery("select * from test");
            System.out.println("==============修改后的表==============");
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
```

### PrepareStatement

#### execute

使用`PreparedStatement.execute()`对内存表进行增删改查，使用占位符`?`表示后期需要动态设置的值，使用`setInt()`、`setDate()`等接口，按照占位符实际类型进行值的设置。

```java
    public void pre_execute_crud_memory_table() throws SQLException {
        try {
            create_memory_table();
            //查询
            preparedStatement = connection.prepareStatement("select * from test where id >= ?");
            preparedStatement.setInt(1,1);
            boolean result = preparedStatement.execute();
            if(result == true) {
                ResultSet resultSet = preparedStatement.getResultSet();
                System.out.println("==============修改前的表==============");
                print_table(resultSet);
            }
            //删除
            preDel = connection.prepareStatement("delete from test where id = ?");
            preDel.setInt(1,1);
            preDel.execute();
            //更新
            preMod = connection.prepareStatement("update test set id = ? where id = ?");
            preMod.setInt(1,5);
            preMod.setInt(2,6);
            preMod.execute();
            //增加
            preAdd = connection.prepareStatement("insert into test values(?,?)");
            preAdd.setInt(1, 7);
            preAdd.setInt(2,10);
            preAdd.execute();
            //查询增删改之后的结果
            result = preparedStatement.execute();
            if(result == true) {
                ResultSet resultSet = preparedStatement.getResultSet();
                System.out.println("==============修改后的表==============");
                print_table(resultSet);
            }
        } finally {
            if(preparedStatement != null) preparedStatement.close();
            if(preAdd != null) preAdd.close();
            if(preDel != null) preDel.close();
            if(preMod != null)preMod.close();
            if(connection != null)connection.close();
        }
    }
```

#### executeQuery

使用`PreparedStatement.executeQuery()`对内存表进行增删改查，使用占位符`?`表示后期需要动态设置的值，使用`setInt()`、`setDate()`等接口，按照占位符实际类型进行值的设置。

```java
   public void stat_executeQuery_memory_table() throws SQLException, ClassNotFoundException {
        try {
            statement = connection.createStatement();
            create_memory_table();
            // 查询内存表
            ResultSet resultSet = statement.executeQuery("select * from test");
            //遍历查询结果
            System.out.println("查询结果如下：");
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
```

#### executeUpdate

使用`PreparedStatement.executeUpdate`对内存表进行增删改查，使用占位符`?`表示后期需要动态设置的值，使用`setInt()`、`setDate()`等接口，按照占位符实际类型进行值的设置。

```java
    public void stat_executeUpdate_memory_table() throws SQLException {
        try {
            statement = connection.createStatement();
            create_memory_table();
            //查看修改前的结果
            System.out.println("==============修改前的表==============");
            ResultSet resultSet = statement.executeQuery("select * from test");
            print_table(resultSet);
            statement.executeUpdate("insert into test values(11, 11)");
            statement.executeUpdate("update test set id = 12 where id = 11");
            statement.executeUpdate("delete from test where id = 5");
            //查看增删改之后结果
            resultSet = statement.executeQuery("select * from test");
            System.out.println("==============修改后的表==============");
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
```

### addBatch、executeBatch、clearBatch

`addBatch()`将给定的 SQL 命令添加到此语句对象的当前命令列表中，如下文`addBatch()`将执行的delete、insert和update添加到statement中。`executeBatch`将添加的语句批量执行。`clearBatch()`用于清空之前添加的SQL语句。

```java
    public void batch_operation_memory_table() throws SQLException {
        try {
            statement = connection.createStatement();
            create_memory_table();
            statement.execute("select * from test");
            ResultSet resultSet = statement.getResultSet();
            print_table(resultSet);
            statement.addBatch("delete from test where id = 1");
            statement.addBatch("insert into test values(6, 6)");
            statement.addBatch("update test set x = 7 where id = 2");
            statement.executeBatch();
            statement.clearBatch();
            statement.execute("select * from test");
            resultSet = statement.getResultSet();
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
```

## streamTable相关操作

### Statement

#### execue

使用`execue()`创建streamTable

```java
    private void create_stream_table() throws SQLException {
        try {
            statement = connection.createStatement();
            String create_stream_table = "id = 1..5\n" +
                    "x = 1..5\n" +
                    "test = streamTable(id as `id, x as x)";
            statement.execute(create_stream_table);
        } finally {
            if(statement != null)statement.close();
        }
    }
```

#### executeQuery

使用`executeQuery()`对流表进行查询

```java
    public void stat_executeQuery_stream_table() throws SQLException, ClassNotFoundException {
        ResultSet resultSet;
        try {
            statement = connection.createStatement();
            create_stream_table();
            Statement query = connection.createStatement();
            resultSet = query.executeQuery("select * from test");
            System.out.println("查询结果：");
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
```

#### executeUpdate

使用`executeUpdate()`对streamTable进行insert操作。注意：streamTable不支持delete和update操作，即不支持删除记录和修改记录中某一字段值的操作。

```java
     public void stat_executeUpdate_stream_table() throws SQLException {
        try {
            create_stream_table();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from test");
            System.out.println("==============修改前的表==============");
            print_table(resultSet);
            statement.executeUpdate("insert into test values(11, 11)");
            resultSet = statement.executeQuery("select * from test");
            System.out.println("==============修改后的表==============");
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
```

### PrepareStatement

#### execute

使用`PreparedStatement.execute()`对streamTable表进行增加和查询操作，使用占位符`?`表示后期需要动态设置的值，使用`setInt()`、`setDate()`等接口，按照占位符实际类型进行值的设置。

```java
    public void pre_execute_crud_stream_table() throws SQLException {
        try {
            create_stream_table();
            boolean result;
            try {
                preparedStatement = connection.prepareStatement("select * from test");
                result = preparedStatement.execute();
            } finally {
                if(preparedStatement != null) preparedStatement.close();
            }
            if(result == true) {
                ResultSet resultSet = preparedStatement.getResultSet();
                ResultSetMetaData metaData = resultSet.getMetaData();
                System.out.println("==============修改前的表==============");
                print_table(resultSet);
            }
            try {
                preAdd = connection.prepareStatement("insert into test values(?, ?)");
                preAdd.setInt(1, 6);
                preAdd.setInt(2, 11);
                preAdd.execute();
            } finally {
                if(preAdd != null) preAdd.close();
            }
            try {
                preparedStatement = connection.prepareStatement("select * from test");
                result = preparedStatement.execute();
            } finally {
                if(preparedStatement != null) preparedStatement.close();
            }
            if(result == true) {
                ResultSet resultSet = preparedStatement.getResultSet();
                System.out.println("==============修改前的表==============");
                print_table(resultSet);
            }
        } finally {
            if(connection != null) connection.close();
        }
    }
```

#### executeQuery

使用`PreparedStatement.executeQurey()`对streamTable表进行增加和查询操作，使用占位符`?`表示后期需要动态设置的值，使用`setInt()`、`setDate()`等接口，按照占位符实际类型进行值的设置。

```java
    public void pre_executeQuery_stream_table() throws SQLException {
        try {
            create_stream_table();
            preparedStatement = connection.prepareStatement("select * from test where id = ?");
            preparedStatement.setInt(1, 1);
            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            System.out.println("==============查询结果==============");
            print_table(resultSet);
        } finally {
            if(preparedStatement != null) preparedStatement.close();
            if(connection != null) connection.close();
        }
    }
```

### addBatch、executeBatch、clearBatch

`addBatch()`将给定的 SQL 命令添加到此语句对象的当前命令列表中，如下文`addBatch()`将执行的delete、insert和update添加到statement中。`executeBatch`将添加的语句批量执行。`clearBatch()`用于清空之前添加的SQL语句。

```java
    public void batch_operation_stream_table() throws SQLException {
        try {
            statement = connection.createStatement();
            create_stream_table();
            statement.execute("select * from test");
            ResultSet resultSet = statement.getResultSet();
            print_table(resultSet);
            statement.addBatch("insert into test values(6, 6)");
            statement.executeBatch();
            statement.clearBatch();
            statement.execute("select * from test");
            resultSet = statement.getResultSet();
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
```

## 分布式表相关操作

### Statement

#### execue

使用`execue()`创建分布式数据表。这里创建的为TSDB分布式表，分区类型为HASH分区。

```java
    public void createDataBaseAndTableForTSDB() throws ClassNotFoundException, SQLException {
        try {
            statement = connection.createStatement();
            String sql_create_databast_table = "login(`admin, `123456)\n" +
                    "n=10\n" +
                    "dates = 2022.01.01..2022.01.10\n" +
                    "price = 1..10\n" +
                    "t = table(dates, price)\n" +
                    "if(existsDatabase(\"dfs://hashdb1\")) dropDatabase(\"dfs://hashdb1\")\n" +
                    "db1 = database(directory=\"dfs://hashdb1\", partitionType=HASH, partitionScheme=[INT, 2], engine=\"TSDB\")\n" +
                    "ht = db1.createPartitionedTable(t,`tsdb_table1, `price, sortColumns=`price)\n" +
                    "ht.append!(t)";
            statement.execute(sql_create_databast_table);
        } finally {
            if(statement != null) statement.close();
        }
    }
```

#### executeQuery

使用`executeQuery()`对分布式表进行查询。

```java
    public void stat_executeQuery_dfs_tsdb_table() throws SQLException, ClassNotFoundException {
        try {
            statement = connection.createStatement();
            createDataBaseAndTableForTSDB();
            // 加载ht表
            statement.execute("ht = loadTable(\"dfs://hashdb1\", `tsdb_table1)");
            // 查询ht表
            ResultSet resultSet = statement.executeQuery("select * from ht");
            System.out.println("查询结果如下：");
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
```

#### executeUpdate

使用`executeUpdate()`对分布式表进行增加、删除、更新操作

```java
    public void stat_executeUpdate_dfs_tsdb_table() throws SQLException, ClassNotFoundException {
        try {
            statement = connection.createStatement();
            createDataBaseAndTableForTSDB();
            statement.execute("ht = loadTable(\"dfs://hashdb1\", `tsdb_table1)");
            ResultSet resultSet = statement.executeQuery("select * from ht");
            System.out.println("==============修改前的表==============");
            print_table(resultSet);
            statement.executeUpdate("insert into ht values(2022.05.11, 11)");
            statement.executeUpdate("delete from ht where price = 1");
            statement.executeUpdate("update ht set dates = 2022.05.12 where price = 2");
            resultSet = statement.executeQuery("select * from ht");
            System.out.println("==============修改后的表==============");
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
```

### PreparedStatement

#### execute

使用`execute()`对分布式表进行增加、删除、更新操作

```java
    public void pre_execute_crud_dfs_tsdb_table() throws ClassNotFoundException, SQLException {
        try {
            createDataBaseAndTableForTSDB();
            statement = connection.createStatement();
            statement.execute("ht = loadTable(\"dfs://hashdb1\", `tsdb_table1)");
            preSel = connection.prepareStatement("select * from ht where price >= ?");
            preSel.setInt(1,1);
            boolean execute = preSel.execute();
            if(execute == true) {
                ResultSet resultSet = preSel.getResultSet();
                System.out.println("==============修改前的表==============");
                print_table(resultSet);
            }
            preAdd = connection.prepareStatement("insert into ht values(?, ?)");
            preAdd.setDate(1, new Date(System.currentTimeMillis()));
            preAdd.setInt(2, 30);
            preAdd.execute();
            preDel = connection.prepareStatement("delete from ht where dates = ?");
            preDel.setDate(1, new Date(System.currentTimeMillis()));
            preDel.execute();
            preMod = connection.prepareStatement("update ht set dates = 2022.01.01 where price = ?");
            preMod.setInt(1, 1);
            preMod.execute();
            preSel = connection.prepareStatement("select * from ht where price >= ?");
            preSel.setInt(1,1);
            execute = preSel.execute();
            if(execute == true) {
                ResultSet resultSet = preSel.getResultSet();
                System.out.println("==============修改后的表==============");
                print_table(resultSet);
            }
        } finally {
            if(preSel != null) preSel.close();
            if(preAdd != null) preAdd.close();
            if(preMod != null) preMod.close();
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
```

#### executeQuery

使用`executeQuery()`对分布式表进行查询。

```java
    public void pre_executeQuery_dfs_tsdb_table() throws ClassNotFoundException, SQLException {
        try {
            statement = connection.createStatement();
            createDataBaseAndTableForTSDB();
            statement.execute("ht = loadTable(\"dfs://hashdb1\", `tsdb_table1)");
            preSel = connection.prepareStatement("select * from ht where price > ?");
            preSel.setInt(1, 0);
            ResultSet resultSet = preSel.executeQuery();
            System.out.println("==============查询结果如下==============");
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(preSel != null) preSel.close();
            if(connection != connection) connection.close();
        }
    }t);
    }
```

#### executeUpdate

使用`executeUpdate()`对分布式表进行增加、删除、更新操作

```java
    public void pre_executeUpdate_dfs_tsdb_table() throws SQLException, ClassNotFoundException {
        try {
            createDataBaseAndTableForTSDB();
            statement = connection.createStatement();
            statement.execute("ht = loadTable(\"dfs://hashdb1\", `tsdb_table1)");
            // 查询ht表
            ResultSet resultSet = statement.executeQuery("select * from ht");
            System.out.println("==============修改前的表==============");
            print_table(resultSet);
            preAdd = connection.prepareStatement("insert into ht values(?, ?)");
            preAdd.setDate(1, new Date(System.currentTimeMillis()));
            preAdd.setInt(2, 42);
            preAdd.executeUpdate();
            preDel = connection.prepareStatement("delete from ht where price = ?");
            preDel.setInt(1, 1);
            preDel.executeUpdate();
            preMod = connection.prepareStatement("update ht set dates = ? where price = 2");
            preMod.setDate(1, new Date(System.currentTimeMillis()));
            preMod.executeUpdate();
            // 查询ht表
            resultSet = statement.executeQuery("select * from ht");
            System.out.println("==============修改后的表==============");
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(preAdd != null) preAdd.close();
            if(preDel != null) preDel.close();
            if(preMod != null) preMod.close();
            if(connection != null) connection.close();
        }
    }
```

### addBatch、executeBatch、clearBatch

`addBatch()`将给定的 SQL 命令添加到此语句对象的当前命令列表中，如下文`addBatch`将执行的delete、insert和update添加到statement中。`executeBatch`将添加的语句批量执行。`clearBatch()`用于清空之前添加的SQL语句。

```java
    public void batch_operation_dfs_tsdb_table() throws SQLException, ClassNotFoundException {
        try {
            statement = connection.createStatement();
            createDataBaseAndTableForTSDB();
            statement.execute("ht = loadTable(\"dfs://hashdb1\", `tsdb_table1)");
            statement.execute("select * from ht");
            ResultSet resultSet = statement.getResultSet();
            System.out.println("==============修改前的表==============");
            print_table(resultSet);
            statement.addBatch("insert into ht values(2022.01.11, 11)");
            statement.addBatch("update ht set dates = 2022.05.18 where price = 2");
            statement.addBatch("delete from ht where dates = 2022.01.11");
            statement.executeBatch();
            statement.clearBatch();
            System.out.println("==============修改后的表==============");
            statement.execute("select * from ht");
            resultSet = statement.getResultSet();
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
```

### 大数据量的读取

对于分布式表而言，实际读取过程中，表的大小可能会超出运行内存。所以，在读取数据量较大的分布式表时，可以设置fetchSize，由数据库对表按照fecthSize大小进行切分，程序再进行分批读取。对于使用者而言，仅需设置fetchSize的大小即可。值得注意的是，fetchSize的大小必须大于等于8192。

通过以下代码创建大数据表

```java
    public void createDataBaseAndTableForTSDB_BV() throws ClassNotFoundException, SQLException {
        try {
            statement = connection.createStatement();
            String sql_create_databast_table = "login(`admin, `123456)\n" +
                    "n=20000\n" +
                    "dates = rand(2022.01.01..2022.01.10, n)\n" +
                    "price = 1..n\n" +
                    "t = table(dates, price)\n" +
                    "if(existsDatabase(\"dfs://hashdb1\")) dropDatabase(\"dfs://hashdb1\")\n" +
                    "db1 = database(directory=\"dfs://hashdb1\", partitionType=HASH, partitionScheme=[INT, 2], engine=\"TSDB\")\n" +
                    "ht = db1.createPartitionedTable(t,`tsdb_table1, `price, sortColumns=`price)\n" +
                    "ht.append!(t)";
            statement.execute(sql_create_databast_table);
        } finally {
            if(statement != null) statement.close();
        }
    }
```

以下代码通过设置fetchSize，对上表的20000条数据进行分批次读取，应该操作对于使用者是透明的。读取的数据依旧有Result使用`next()`函数进行迭代读取。

```java
    public void testFetchSizeJDBC_statemenet_tsdb() throws SQLException, ClassNotFoundException {
        try {
            createDataBaseAndTableForTSDB_BV();
            statement = connection.createStatement();
            statement.execute("ht = loadTable(\"dfs://hashdb1\", `tsdb_table1)");
            statement.setFetchSize(10000);
            statement.execute("select * from ht");
            ResultSet resultSet = statement.getResultSet();
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }
```