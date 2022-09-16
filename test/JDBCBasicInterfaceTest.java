import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class JDBCBasicInterfaceTest {


    private Properties properties = new Properties();
    private String url = null;
    private String dataBase = null;
    private String tableName = null;

    private Statement statement;

    private PreparedStatement preparedStatement;

    private PreparedStatement preAdd;

    private PreparedStatement preDel;

    private PreparedStatement preMod;

    private PreparedStatement preSel;

    private Connection connection;

    /**
     * 创建连接
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Before
    public void init() throws ClassNotFoundException, SQLException {
        properties.put("user", "admin");
        properties.put("password", "123456");
        url = "jdbc:dolphindb://"+JDBCTestUtil.HOST+":"+JDBCTestUtil.PORT;
        dataBase = "dfs://jdbcTest";
        tableName = "test";
        Class.forName("com.dolphindb.jdbc.Driver");
        connection = DriverManager.getConnection(url, properties);
    }


    /**
     * 创建分布式TSDB数据库以及table
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private void createDataBaseAndTableForTSDB() throws ClassNotFoundException, SQLException {
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

    /**
     * 创建分布式OLAP数据库以及table
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private void createDataBaseAndTableForOLAP() throws ClassNotFoundException, SQLException {

        Statement statement = connection.createStatement();
        //创建数据库以及表格
        String sql_create_databast_table = "login(`admin, `123456)\n" +
                "n=10\n" +
                "date = 2022.01.01..2022.05.01\n" +
                "dates = rand(date, n)\n" +
                "price = 1..10\n" +
                "t = table(dates, price)\n" +
                "if(existsDatabase(\"dfs://hashdb1\"))  dropDatabase(\"dfs://hashdb1\")\n" +
                "db1 = database(directory=\"dfs://hashdb1\", partitionType=HASH, partitionScheme=[INT, 2], engine=\"OLAP\")\n" +
                "ht = db1.createPartitionedTable(t,`olap_table1, `price)\n" +
                "ht.append!(t)";
        statement.execute(sql_create_databast_table);
    }

    /**
     * create table 'test' in memory
     * @throws SQLException
     */
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

    /**
     * crate table 'test' in stream
     * @throws SQLException
     */
    private void create_stream_table() throws SQLException {
        try {
            statement = connection.createStatement();
            String create_stream_table = "id = 1..5\n" +
                    "x = 1..5\n" +
                    "test = streamTable(id as `id, x as `x)";
            statement.execute(create_stream_table);
        } finally {
            if(statement != null)statement.close();
        }
    }

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

    //以下为statement

    /**
     * 查询内存表
     *
     * @throws SQLException
     * @throws ClassNotFoundException 运行结果：
     *                                查询结果如下：
     *                                1 9
     *                                2 7
     *                                3 7
     *                                4 1
     *                                5 8
     *                                <p>
     *                                Process finished with exit code 0
     */
    @Test
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

    /**
     * 查询流表
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
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

    /**
     * 查询分布式表-tsdb
     * 数据库与分布式表在查询前已经建立完成，建立代码如下：
     * <p>
     * 查询结果如下：
     * 2022.02.18	2
     * 2022.04.14	4
     * 2022.04.16	6
     * 2022.03.16	6
     * 2022.02.26	10
     * 2022.04.26	1
     * 2022.04.28	3
     * 2022.02.13	7
     * 2022.03.28	9
     * 2022.02.05	9
     * <p>
     * Process finished with exit code 0
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    public void stat_executeQuery_dfs_tsdb_table() throws SQLException, ClassNotFoundException {
        try {
            createDataBaseAndTableForTSDB();
            statement = connection.createStatement();
            // 加载ht表
            statement.execute("ht = loadTable(\"dfs://hashdb1\",\"tsdb_table1\")");
            // 查询ht表
            ResultSet resultSet = statement.executeQuery("select * from ht");
            System.out.println("查询结果如下：");
            print_table(resultSet);
        } finally {
            if(statement != null) statement.close();
            if(connection != null) connection.close();
        }
    }

    /**
     * 查询分布式数据表-olap
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     * 运行结果：
     * 查询结果如下：
     * 2022.04.30	2
     * 2022.01.22	10
     * 2022.01.25	6
     * 2022.03.06	8
     * 2022.04.27	6
     * 2022.04.09	4
     * 2022.04.14	3
     * 2022.03.09	5
     * 2022.02.13	5
     * 2022.01.30	3
     *
     * Process finished with exit code 0
     */
    @Test
    public void stat_executeQuery_dfs_olap_table() throws SQLException, ClassNotFoundException {
        Statement statement = connection.createStatement();
        createDataBaseAndTableForOLAP();
        // 加载ht表
        statement.execute("ht = loadTable(\"dfs://hashdb1\",\"olap_table1\")");
        // 查询ht表
        ResultSet resultSet = statement.executeQuery("select * from ht");
        System.out.println("查询结果如下：");
        print_table(resultSet);
    }

    /**
     * 内存表的executeUpdate（增删改）
     * 运行结果：
     * before update table:
     * 1 5
     * 2 4
     * 3 1
     * 4 9
     * 5 3
     * after update table:
     * 1 5
     * 2 4
     * 3 1
     * 4 9
     * 12 11
     *
     * Process finished with exit code 0
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
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

    /**
     * 分布式TSDB表的executeUpdate（增删改）
     *
     * 运行结果
     * 修改前：
     * 2022.04.11	2
     * 2022.04.21	2
     * 2022.02.10	4
     * 2022.04.26	6
     * 2022.03.05	6
     * 2022.02.25	6
     * 2022.04.24	8
     * 2022.01.27	1
     * 2022.04.10	9
     * 2022.02.17	9
     * 修改后：
     * 2022.04.11	2
     * 2022.04.21	2
     * 2022.02.10	4
     * 2022.04.26	6
     * 2022.03.05	6
     * 2022.02.25	6
     * 2022.04.24	8
     * 2022.01.27	1
     * 2022.04.10	9
     * 2022.02.17	9
     * 2022.05.11	11
     *
     * Process finished with exit code 0
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    public void stat_executeUpdate_dfs_tsdb_table() throws SQLException, ClassNotFoundException {
        try {
            createDataBaseAndTableForTSDB();
            statement = connection.createStatement();
            statement.execute("ht = loadTable(\"dfs://hashdb1\",\"tsdb_table1\")");
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

    /**
     * 分布式OLAP表的executeUpdate（增删改）
     *
     * 运行结果
     * 修改前：
     * 2022.03.05	8
     * 2022.03.29	10
     * 2022.02.09	10
     * 2022.04.12	4
     * 2022.04.28	6
     * 2022.03.18	6
     * 2022.04.30	4
     * 2022.02.12	2
     * 2022.04.10	9
     * 2022.03.03	5
     * 修改后：
     * 2022.03.05	8
     * 2022.03.29	10
     * 2022.02.09	10
     * 2022.04.12	4
     * 2022.04.28	6
     * 2022.03.18	6
     * 2022.04.30	4
     * 2022.02.12	2
     * 2022.04.10	9
     * 2022.03.03	5
     * 2022.05.11	11
     *
     * Process finished with exit code 0
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    public void stat_executeUpdate_dfs_olap_table() throws SQLException, ClassNotFoundException {
        createDataBaseAndTableForOLAP();
        Statement statement = connection.createStatement();
        statement.execute("ht = loadTable(\"dfs://hashdb1\",\"olap_table1\")");
        ResultSet resultSet = statement.executeQuery("select * from ht");
        System.out.println("==============修改前的表==============");
        print_table(resultSet);
        statement.executeUpdate("insert into ht values(2022.05.11, 11)");
        statement.executeUpdate("delete from ht where price = 1");
        statement.executeUpdate("update ht set dates = 2022.05.12 where price = 2");
        resultSet = statement.executeQuery("select * from ht");
        System.out.println("==============修改后的表==============");
        print_table(resultSet);
    }

    //以下为PrepareStatement

    /**
     * 内存表的使用PrepareStatement.execute增删改查
     * @throws ClassNotFoundException
     * @throws SQLException
     * 运行结果
     * 查询结果如下：
     * 5 7
     *
     * Process finished with exit code 0
     */
    @Test
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
            //查询rud之后的结果
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

    /**
     * PrepareStatement.execute 增删改
     * 运行结果
     * 查询结果：
     *
     * Process finished with exit code 0
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    public void pre_execute_crud_stream_table() throws SQLException {
        try {
            create_stream_table();
            boolean result;
            try {
                preparedStatement = connection.prepareStatement("select * from test");
                result = preparedStatement.execute();
            } finally {
                //if(preparedStatement != null) preparedStatement.close();
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

    /**
     * 内存表的使用PrepareStatement.executeQuery查询
     * @throws ClassNotFoundException
     * @throws SQLException
     * 查询结果如下：
     * 5 5
     *
     * Process finished with exit code 0
     */
    @Test
    public void pre_executeQuery_memory_table() throws SQLException {
        create_memory_table();
        PreparedStatement preQuery = connection.prepareStatement("select * from test where id = ?");
        preQuery.setInt(1,5);
        ResultSet resultSet = preQuery.executeQuery();
        System.out.println("==============查询结果==============");
        print_table(resultSet);
    }

    @Test
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

    /**
     * 分布式tsdb表的使用PrepareStatement.execute查询
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
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

    /**
     * 分布式tsdb表的使用PrepareStatement.executeQuery查询
     * @throws ClassNotFoundException
     * @throws SQLException
     * 运行结果
     * 查询结果如下
     * 2022.02.02	2
     * 2022.02.27	6
     * 2022.03.21	8
     * 2022.03.21	10
     * 2022.02.13	10
     * 2022.03.10	1
     * 2022.04.10	1
     * 2022.01.04	7
     * 2022.02.22	9
     * 2022.04.08	9
     *
     * Process finished with exit code 0
     */
    @Test
    public void pre_executeQuery_dfs_tsdb_table() throws ClassNotFoundException, SQLException {
        try {
            createDataBaseAndTableForTSDB();
            statement = connection.createStatement();

            statement.execute("ht = loadTable(\"dfs://hashdb1\",\"tsdb_table1\")");
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
    }

    /**
     * 分布式olap表的使用PrepareStatement.execute增删改查
     * 运行结果：
     * 查询结果如下
     * 2022.02.12	2
     * 2022.04.18	8
     * 2022.04.01	6
     * 2022.04.24	6
     * 2022.02.11	10
     * 2022.04.12	9
     * 2022.02.22	7
     * 2022.05.01	3
     * 2022.02.22	5
     * 2022.03.18	7
     * 查询结果如下
     * 2022.02.12	2
     * 2022.04.18	8
     * 2022.04.01	6
     * 2022.04.24	6
     * 2022.02.11	10
     * 2022.05.12	30
     * 2022.04.12	9
     * 2022.02.22	7
     * 2022.05.01	3
     * 2022.02.22	5
     * 2022.03.18	7
     *
     * Process finished with exit code 0
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
    public void pre_execute_crud_dfs_olap_table() throws ClassNotFoundException, SQLException {
        Statement statement = connection.createStatement();
        createDataBaseAndTableForOLAP();
        // 加载ht表
        statement.execute("ht = loadTable(\"dfs://hashdb1\", `olap_table1)");
        PreparedStatement preCheck = connection.prepareStatement("select * from ht where price > ?");
        preCheck.setInt(1,0);
        boolean result = preCheck.execute();
        if(result == true) {
            ResultSet resultSet = preCheck.getResultSet();
            System.out.println("==============修改前的表==============");
            print_table(resultSet);
        }
        PreparedStatement preAdd = connection.prepareStatement("insert into ht values(?,?)");
        preAdd.setDate(1,new Date(System.currentTimeMillis()));
        preAdd.setInt(2,30);
        preAdd.execute();
        result = preCheck.execute();
        if(result == true) {
            ResultSet resultSet = preCheck.getResultSet();
            System.out.println("==============修改后的表==============");
            print_table(resultSet);
        }
    }

    /**
     * 分布式olap表的使用PrepareStatement.executeQuery查询
     * 运行结果：
     * 查询结果如下
     * 2022.02.11	6
     * 2022.04.03	10
     * 2022.01.21	8
     *
     * Process finished with exit code 0
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
    public void pre_executeQuery_dfs_olap_table() throws ClassNotFoundException, SQLException {
        Statement statement = connection.createStatement();
        createDataBaseAndTableForOLAP();
        // 加载ht表
        statement.execute("ht = loadTable(\"dfs://hashdb1\", `olap_table1)");
        PreparedStatement preExecute = connection.prepareStatement("select * from ht where price > ?");
        preExecute.setInt(1,3);
        ResultSet resultSet = preExecute.executeQuery();
        System.out.println("==============查询结果如下==============");
        print_table(resultSet);
    }

    /**
     * 内存表带参数的executeUpdate
     * 运行结果
     * 查询结果如下：
     * 1 6
     * 2 5
     * 3 4
     * 4 9
     * 5 3
     * 查询结果如下：
     * 2 100
     * 3 4
     * 4 9
     * 5 3
     * 6 11
     *
     * Process finished with exit code 0
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
    public void pre_executeUpdate_memory_table() throws SQLException {
        create_memory_table();
        Statement check = connection.createStatement();
        boolean result = check.execute("select * from test");
        if(result == true) {
            ResultSet resultSet = check.getResultSet();
            System.out.println("==============修改前的表==============");
            print_table(resultSet);
        }
        //向内存表中添加元记录
        PreparedStatement preAdd = connection.prepareStatement("insert into test values(?, ?)");
        preAdd.setInt(1,6);
        preAdd.setInt(2,11);
        preAdd.executeUpdate();
        //删除表中元素
        PreparedStatement preDel = connection.prepareStatement("delete from test where id = ?");
        preDel.setInt(1, 1);
        preDel.executeUpdate();
        //修改元素值
        PreparedStatement preMod = connection.prepareStatement("update test set x = 100 where id = ?");
        preMod.setInt(1, 2);
        preMod.execute();
        result = check.execute("select * from test");
        if(result == true) {
            ResultSet resultSet = check.getResultSet();
            System.out.println("==============修改前的表==============：");
            print_table(resultSet);
        }

    }


    @Test
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

    @Test
    public void pre_executeUpdate_stream_table() throws SQLException {
        create_stream_table();
        PreparedStatement preStat = connection.prepareStatement("select * from test");
        boolean result = preStat.execute();
        if(result == true) {
            ResultSet resultSet = preStat.getResultSet();
            ResultSetMetaData metaData = resultSet.getMetaData();
            System.out.println("==============修改前的表==============");
        }
        PreparedStatement preAdd = connection.prepareStatement("insert into test values(?, ?)");
        preAdd.setInt(1, 6);
        preAdd.setInt(2, 11);
        preAdd.executeUpdate();
        preStat = connection.prepareStatement("select * from test");
        result = preStat.execute();
        if(result == true) {
            ResultSet resultSet = preStat.getResultSet();
            System.out.println("==============修改后的表==============");
            print_table(resultSet);
        }
    }

    /**
     * 分布式TSDB下PrepareStatement.executeUpdate
     * 运行结果
     * 查询结果如下：
     * 2022.01.31	6
     * 2022.03.27	10
     * 2022.01.21	10
     * 2022.02.07	1
     * 2022.03.17	3
     * 2022.02.18	3
     * 2022.02.27	5
     * 2022.01.10	7
     * 2022.04.29	7
     * 2022.02.08	9
     * 更新后结果如下：
     * 2022.01.31	6
     * 2022.03.27	10
     * 2022.01.21	10
     * 2022.05.12	42
     * 2022.02.07	1
     * 2022.03.17	3
     * 2022.02.18	3
     * 2022.02.27	5
     * 2022.01.10	7
     * 2022.04.29	7
     * 2022.02.08	9
     *
     * Process finished with exit code 0
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
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

    /**
     * 分布式OLAP表下PrepareStatement.executeUpdate
     * 输出结果
     * 查询结果如下：
     * 2022.02.13	10
     * 2022.03.13	10
     * 2022.02.14	7
     * 2022.04.22	9
     * 2022.02.09	3
     * 2022.02.01	1
     * 2022.01.06	1
     * 2022.03.25	7
     * 2022.01.07	1
     * 2022.01.20	9
     * 更新后结果如下：
     * 2022.02.13	10
     * 2022.03.13	10
     * 2022.05.12	100
     * 2022.02.14	7
     * 2022.04.22	9
     * 2022.02.09	3
     * 2022.02.01	1
     * 2022.01.06	1
     * 2022.03.25	7
     * 2022.01.07	1
     * 2022.01.20	9
     *
     * Process finished with exit code 0
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
    public void pre_executeUpdate_dfs_olap_table() throws ClassNotFoundException, SQLException {
        Statement statement = connection.createStatement();
        createDataBaseAndTableForOLAP();
        // 加载ht表
        statement.execute("ht = loadTable(\"dfs://hashdb1\",\"olap_table1\")");
        // 查询ht表
        ResultSet resultSet = statement.executeQuery("select * from ht");
        System.out.println("==============修改前的表==============");
        print_table(resultSet);
        //添加记录
        PreparedStatement preAdd = connection.prepareStatement("insert into ht values(?, ?)");
        preAdd.setDate(1, new Date(System.currentTimeMillis()));
        preAdd.setInt(2, 100);
        preAdd.executeUpdate();
        PreparedStatement preDel = connection.prepareStatement("delete from ht where price = ?");
        preDel.setInt(1, 1);
        preDel.execute();
        //preDel.executeUpdate();
        PreparedStatement preMod = connection.prepareStatement("update ht set dates = ? where price = 2");
        preMod.setDate(1, new Date(System.currentTimeMillis()));
        preMod.execute();
        //preMod.executeUpdate();
        // 查询ht表
        resultSet = statement.executeQuery("select * from ht");
        System.out.println("==============修改后的表==============");
        print_table(resultSet);
    }

    /**
     * 内存表 addBatch
     * @throws SQLException
     */
    @Test
    public void batch_operation_memory_table() throws SQLException {
        try {
            create_memory_table();
            statement = connection.createStatement();
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

    @Test
    public void batch_operation_stream_table() throws SQLException {
        try {
            create_stream_table();
            statement = connection.createStatement();
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

    @Test
    public void batch_operation_dfs_olap_table() throws SQLException, ClassNotFoundException {
        Statement statement = connection.createStatement();
        createDataBaseAndTableForOLAP();
        statement.execute("ht = loadTable(\"dfs://hashdb1\", `olap_table1)");
        statement.execute("select * from ht");
        ResultSet resultSet = statement.getResultSet();
        System.out.println("==============修改前的表==============");
        print_table(resultSet);
        statement.addBatch("insert into ht values(2022.01.11, 11)");
        statement.addBatch("insert into ht values(2022.01.12, 12)");
        statement.executeBatch();
        statement.clearBatch();
        System.out.println("==============修改后的表==============");
        statement.execute("select * from ht");
        resultSet = statement.getResultSet();
        print_table(resultSet);
    }

    @Test
    public void batch_operation_dfs_tsdb_table() throws SQLException, ClassNotFoundException {
        try {
            createDataBaseAndTableForTSDB();
            statement = connection.createStatement();
            statement.execute("ht = loadTable(\"dfs://hashdb1\", \"tsdb_table1\")");
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

    @Test
    public void testFetchSizeJDBC_prestatement_tsdb() throws IOException, SQLException, ClassNotFoundException {
        createDataBaseAndTableForTSDB();
        Statement statement = connection.createStatement();
        statement.execute("ht = loadTable(\"dfs://hashdb1\",\"tsdb_table1\")");
        PreparedStatement preparedStatement = connection.prepareStatement("select * from ht");
        preparedStatement.setFetchSize(8192);
        preparedStatement.execute();
        ResultSet resultSet = preparedStatement.getResultSet();
        print_table(resultSet);
    }

    @Test
    public void testFetchSizeJDBC_statemenet_olap() throws IOException, SQLException, ClassNotFoundException {
        createDataBaseAndTableForOLAP();
        Statement statement = connection.createStatement();
        statement.execute("ht = loadTable(\"dfs://hashdb1\",\"olap_table1\")");
        statement.setFetchSize(10000);
        statement.execute("select * from ht");
        ResultSet resultSet = statement.getResultSet();
        print_table(resultSet);
    }

    @Test
    public void testFetchSizeJDBC_prestatement_olap() throws IOException, SQLException, ClassNotFoundException {
        createDataBaseAndTableForOLAP();
        Statement statement = connection.createStatement();
        statement.execute("ht = loadTable(\"dfs://hashdb1\",\"olap_table1\")");
        PreparedStatement preparedStatement = connection.prepareStatement("select * from ht");
        preparedStatement.setFetchSize(8192);
        preparedStatement.execute();
        ResultSet resultSet = preparedStatement.getResultSet();
        print_table(resultSet);
    }
}
