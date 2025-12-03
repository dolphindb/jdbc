import com.dolphindb.jdbc.JDBCConnection;
import com.xxdb.DBConnection;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;

import static com.dolphindb.jdbc.Utils.checkServerVersionIfSupportRunSql;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class JDBCPrepareStatementSqlInjectionTest {
    static String HOST = JDBCTestUtil.HOST;
    static int PORT = JDBCTestUtil.PORT;
    Properties LOGININFO = new Properties();
    String JDBC_DRIVER;
    String DB_URL ;
    Statement stm ;
    Connection conn;
    @Before
    public void SetUp(){
        JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        LOGININFO = new Properties();
        LOGININFO.put("user", "admin");
        LOGININFO.put("password", "123456");
        DB_URL = "jdbc:dolphindb://"+HOST+":"+PORT;
        JDBCTestUtil.LOGININFO.put("user", "admin");
        JDBCTestUtil.LOGININFO.put("password", "123456");
        conn = JDBCTestUtil.getConnection(JDBCTestUtil.LOGININFO);
        try {
            stm = conn.createStatement();
        }catch (SQLException ex){

        }
    }
    @Test
    public void test_JDBCPrepareStatement_sql_insert() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`username`password,[STRING, STRING])  as users ;\n" ;
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        String username = "admin'); DROP TABLE users; --";
        String password = "password123";

        PreparedStatement s = conn.prepareStatement("INSERT INTO users (username, password) VALUES (?,?)");
        s.setObject(1,username);
        s.setObject(2,password);
        s.execute();
        ResultSet rs = s.executeQuery("select * from users");
        rs.next();
        assertEquals(username, rs.getString("username"));
        assertEquals(password, rs.getString("password"));
        assertFalse(rs.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_insert_1() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`username`password,[STRING, STRING])  as users ;\n" ;
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        String username = "admin');  DELETE FROM  users; --";
        String password = "password123";

        PreparedStatement s = conn.prepareStatement("INSERT INTO users (username, password) VALUES (?,?)");
        s.setObject(1,username);
        s.setObject(2,password);
        s.execute();
        ResultSet rs = s.executeQuery("select * from users");
        rs.next();
        assertEquals(username, rs.getString("username"));
        assertEquals(password, rs.getString("password"));
        assertFalse(rs.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_insert_2() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`username`password,[STRING, STRING])  as users ;\n" ;
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        String username = "admin\",\"password123\"); \n delete from  users; --";
        String password = "password123";

        PreparedStatement s = conn.prepareStatement("INSERT INTO users (username, password) VALUES (?,?)");
        s.setString(1,username);
        s.setString(2,password);
        s.execute();
        ResultSet rs = s.executeQuery("select * from users");
        rs.next();
        assertEquals(username, rs.getString("username"));
        assertEquals(password, rs.getString("password"));
        assertFalse(rs.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_insert_execute() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`username`password,[STRING, STRING])  as users ;\n" ;
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        //prepareStatement中输入一个INSERT类型query
        PreparedStatement s = conn.prepareStatement("INSERT INTO users (username, password) VALUES (?,?)");

        //调用setString, 参数中输入恶意sql
        String username = "admin\",\"password123\");   drop table users;  --";
        String password = "password123";
        s.setString(1,username);
        s.setString(2,password);

        //调用execute执行query
        s.execute();

        //检查表是否被删除（预期不能删除），校验表中数据结果
        ResultSet rs = s.executeQuery("select * from users");
        rs.next();
        assertEquals(username, rs.getString("username"));
        assertEquals(password, rs.getString("password"));
        assertFalse(rs.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_insert_executeBatch() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`username`password,[STRING, STRING])  as users ;\n" ;
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        //prepareStatement中输入一个INSERT类型query
        PreparedStatement s = conn.prepareStatement("INSERT INTO users (username, password) VALUES (?,?)");

        //调用setString, 参数中输入恶意sql
        String username = "admin\",\"password123\");  drop table users; --";
        String password = "password123";
        s.setString(1,username);
        s.setString(2,password);
        s.addBatch();
        //调用executeBatch执行query
        s.executeBatch();

        //检查表是否被删除（预期不能删除），校验表中数据结果
        ResultSet rs = s.executeQuery("select * from users");
        rs.next();
        assertEquals(username, rs.getString("username"));
        assertEquals(password, rs.getString("password"));
        assertFalse(rs.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_insert_executeUpdate() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`username`password,[STRING, STRING])  as users ;\n" ;
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        //prepareStatement中输入一个INSERT类型query
        PreparedStatement s = conn.prepareStatement("INSERT INTO users (username, password) VALUES (?,?)");

        //调用setString, 参数中输入恶意sql
        String username = "admin\",\"password123\");  drop table users; --";
        String password = "password123";
        s.setString(1,username);
        s.setString(2,password);

        //调用executeUpdate执行query
        s.executeUpdate();

        //检查表是否被删除（预期不能删除），校验表中数据结果
        ResultSet rs = s.executeQuery("select * from users");
        rs.next();
        assertEquals(username, rs.getString("username"));
        assertEquals(password, rs.getString("password"));
        assertFalse(rs.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_insert_setObject_execute() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`username`password,[STRING, STRING])  as users ;\n" ;
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        //prepareStatement中输入一个INSERT类型query
        PreparedStatement s = conn.prepareStatement("INSERT INTO users (username, password) VALUES (?,?)");

        //调用setString, 参数中输入恶意sql
        String username = "admin\",\"password123\");   drop table users;  --";
        String password = "password123";
        s.setObject(1,username);
        s.setObject(2,password);

        //调用execute执行query
        s.execute();

        //检查表是否被删除（预期不能删除），校验表中数据结果
        ResultSet rs = s.executeQuery("select * from users");
        rs.next();
        assertEquals(username, rs.getString("username"));
        assertEquals(password, rs.getString("password"));
        assertFalse(rs.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_insert_execute_dfs() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个dfs表
        String script = "login(\"admin\", \"123456\"); \n"+
                "if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')} \n" +
                "try{undef(\"users\",SHARED);\n}catch(ex){\n};\n"+
                "users = table(10:0,`id`username`password,[INT, STRING, STRING]); \n" +
                "db=database('dfs://db_testStatement', VALUE, 1..10); \n" +
                "db.createPartitionedTable(users, `users, `id);\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        //prepareStatement中输入一个INSERT类型query
        PreparedStatement s = conn.prepareStatement("INSERT INTO loadTable('dfs://db_testStatement','users') (id, username, password) VALUES (?,?,?)");

        //调用setString, 参数中输入恶意sql
        String username = "admin'); DROP TABLE users; --";
        String password = "password123";
        s.setInt(1,1);
        s.setObject(2,username);
        s.setObject(3,password);

        //调用execute执行query
        s.execute();

        //检查表是否被删除（预期不能删除），校验表中数据结果
        ResultSet rs = s.executeQuery("select * from loadTable('dfs://db_testStatement','users')");
        rs.next();
        assertEquals(username, rs.getString("username"));
        assertEquals(password, rs.getString("password"));
        assertFalse(rs.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_insert_setObject_execute_1() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`username`password,[STRING, STRING])  as users ;\n" ;
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        //prepareStatement中输入一个INSERT类型query
        PreparedStatement s = conn.prepareStatement("INSERT INTO users (username, password) VALUES (?,?)");

        //调用setString, 参数中输入恶意sql
        String username = "admin','password123');   drop table users;  --";
        String password = "password123";
        s.setObject(1,username);
        s.setObject(2,password);

        //调用execute执行query
        s.execute();

        //检查表是否被删除（预期不能删除），校验表中数据结果
        ResultSet rs = s.executeQuery("select * from users");
        rs.next();
        assertEquals(username, rs.getString("username"));
        assertEquals(password, rs.getString("password"));
        assertFalse(rs.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }
    @Test
    public void test_JDBCPrepareStatement_sql_select_execute() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
            //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`id`username`password,[INT,STRING, STRING])  as users ;\n" +
                "insert into users values(1,'admin','123456');\n" +
                "insert into users values(2,'user1','123456');\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);
        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        //prepareStatement中输入一个select类型query
        String sql = "SELECT * FROM users WHERE username = ?";
        PreparedStatement s = conn.prepareStatement(sql);

        //调用setString, 参数中输入双引号，且带有OR "1"="1
        String username = "admin\" OR \"1\"=\"1";
        s.setString(1,username);
        s.execute();

        //检查结果，因为username没有这样的值：admin" OR "1"="1  故预期查询不出数据
        ResultSet resultSet = s.getResultSet();
        assertFalse(resultSet.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_select_execute_dfs() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n" +
                "try{undef(\"users\",SHARED);\n}catch(ex){\n}\n" +
                "go;\n"+
                "users = table(10:0,`id`username`password,[INT, STRING, STRING]);\n" +
                "insert into users values(1,'admin','123456');\n" +
                "insert into users values(2,'user1','123456');\n"+
                "if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')}; \n" +
                "db=database('dfs://db_testStatement', VALUE, 1..10); \n" +
                "db.createPartitionedTable(users, `users, `id).append!(users);\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);
        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        //prepareStatement中输入一个select类型query
        String sql = "SELECT * FROM loadTable('dfs://db_testStatement', `users) WHERE username = ?";
        PreparedStatement s = conn.prepareStatement(sql);

        //调用setString, 参数中输入双引号，且带有OR "1"="1
        String username = "admin\" OR \"1\"=\"1";
        s.setString(1,username);

        s.execute();

        //检查结果，因为username没有这样的值：admin" OR "1"="1  故预期查询不出数据
        ResultSet resultSet = s.getResultSet();
        assertFalse(resultSet.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_select_execute_setObject() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`id`username`password,[INT,STRING, STRING])  as users ;\n" +
                "insert into users values(1,'admin','123456');\n" +
                "insert into users values(2,'user1','123456');\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);
        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        //prepareStatement中输入一个select类型query
        String sql = "SELECT * FROM users WHERE username = ?";
        PreparedStatement s = conn.prepareStatement(sql);

        //调用setString, 参数中输入双引号，且带有OR "1"="1
        String username = "admin\" OR \"1\"=\"1";
        s.setObject(1,username);
        //调用execute执行query
        s.execute();

        //检查结果，因为username没有这样的值：admin" OR "1"="1  故预期查询不出数据
        ResultSet resultSet = s.getResultSet();
        assertFalse(resultSet.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_delete_execute() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`id`username`password,[INT,STRING, STRING])  as users ;\n" +
                "insert into users values(1,'admin','123456');\n" +
                "insert into users values(2,'user1','123456');\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);

        //prepareStatement中输入一个delete类型query
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        String sql = "delete FROM users WHERE username = ?";
        PreparedStatement s = conn.prepareStatement(sql);

        //调用setString, 参数中输入双引号，且带有OR "1"="1
        String username = "admin\" OR \"1\"=\"1";
        s.setString(1,username);
        s.execute();

        //检查结果，因为username没有这样的值：admin" OR "1"="1  故预期不会删除数据
        ResultSet rs = s.executeQuery("select * from users");
        assertTrue(rs.next());
        assertTrue(rs.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_delete_execute_dfs() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n" +
                "try{undef(\"users\",SHARED);\n}catch(ex){\n}\n"+
                "go;\n"+
                "users = table(10:0,`id`username`password,[INT, STRING, STRING]);\n" +
                "insert into users values(1,'admin','123456');\n" +
                "insert into users values(2,'user1','123456');\n"+
                "if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')}; \n" +
                "db=database('dfs://db_testStatement', VALUE, 1..10); \n" +
                "db.createPartitionedTable(users, `users, `id).append!(users);\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);

        //prepareStatement中输入一个delete类型query
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        String sql = "delete FROM loadTable('dfs://db_testStatement',`users) WHERE username = ?";
        PreparedStatement s = conn.prepareStatement(sql);

        //调用setString, 参数中输入双引号，且带有OR "1"="1
        String username = "admin\" OR \"1\"=\"1";
        s.setString(1,username);
        s.execute();

        //检查结果，因为username没有这样的值：admin" OR "1"="1  故预期不会删除数据
        ResultSet rs = s.executeQuery("select * from loadTable('dfs://db_testStatement',`users)");
        assertTrue(rs.next());
        assertTrue(rs.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_delete_executeBatch() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`id`username`password,[INT,STRING, STRING])  as users ;\n" +
                "insert into users values(1,'admin','123456');\n" +
                "insert into users values(2,'user1','123456');\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);

        //prepareStatement中输入一个delete类型query
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        String sql = "delete FROM users WHERE username = ?";
        PreparedStatement s = conn.prepareStatement(sql);

        //调用setString, 参数中输入双引号，且带有OR "1"="1
        String username = "admin\" OR \"1\"=\"1";
        s.setString(1,username);
        s.addBatch();
        //调用executeBatch执行query
        s.executeBatch();

        //检查结果，因为username没有这样的值：admin" OR "1"="1  故预期不会删除数据
        ResultSet rs = s.executeQuery("select * from users");
        assertTrue(rs.next());
        assertTrue(rs.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_delete_executeUpdate() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`id`username`password,[INT,STRING, STRING])  as users ;\n" +
                "insert into users values(1,'admin','123456');\n" +
                "insert into users values(2,'user1','123456');\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);

        //prepareStatement中输入一个delete类型query
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        String sql = "delete FROM users WHERE username = ?";
        PreparedStatement s = conn.prepareStatement(sql);

        //调用setString, 参数中输入双引号，且带有OR "1"="1
        String username = "admin\" OR \"1\"=\"1";
        s.setString(1,username);
        //调用executeUpdate执行query
        s.executeUpdate();

        //检查结果，因为username没有这样的值：admin" OR "1"="1  故预期不会删除数据
        ResultSet rs = s.executeQuery("select * from users");
        assertTrue(rs.next());
        assertTrue(rs.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_delete_execute_setObject() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`id`username`password,[INT,STRING, STRING])  as users ;\n" +
                "insert into users values(1,'admin','123456');\n" +
                "insert into users values(2,'user1','123456');\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);

        //prepareStatement中输入一个delete类型query
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        String sql = "delete FROM users WHERE username = ?";
        PreparedStatement s = conn.prepareStatement(sql);

        //调用setString, 参数中输入双引号，且带有OR "1"="1
        String username = "admin\" OR \"1\"=\"1";
        s.setObject(1,username);
        //调用execute执行query
        s.execute();

        //检查结果，因为username没有这样的值：admin" OR "1"="1  故预期不会删除数据
        ResultSet rs = s.executeQuery("select * from users");
        assertTrue(rs.next());
        assertTrue(rs.next());
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_update_execute() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`id`username`password,[INT,STRING, STRING])  as users ;\n" +
                "insert into users values(1,'admin','123456');\n" +
                "insert into users values(2,'user1','123456');\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);

        //prepareStatement中输入一个update类型query
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        String sql = "update users set username = ?";
        PreparedStatement s = conn.prepareStatement(sql);

        //调用setString, 参数中有恶意sql
        String username = "admin\"; DROP TABLE users; //";
        s.setString(1,username);
        s.execute();

        //检查结果，恶意sql没有被执行,表不会被删除
        ResultSet rs = s.executeQuery("select * from users");
        assertTrue(rs.next());
        assertEquals(username, rs.getString("username"));
        assertTrue(rs.next());
        assertEquals(username, rs.getString("username"));
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test//getAuditLog()
    public void test_JDBCPrepareStatement_sql_update_execute_dfs() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n" +
                "try{undef(\"users\",SHARED);\n}catch(ex){\n};\n"+
                "users = table(10:0,`id`username`password,[INT, STRING, STRING]);\n" +
                "insert into users values(1,'admin','123456');\n" +
                "insert into users values(2,'user1','123456');\n"+
                "if(existsDatabase('dfs://db_testStatement')){ dropDatabase('dfs://db_testStatement')}; \n" +
                "db=database('dfs://db_testStatement', VALUE, 1..10); \n" +
                "db.createPartitionedTable(users, `users, `id).append!(users);\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);

        //prepareStatement中输入一个update类型query
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        String sql = "update loadTable('dfs://db_testStatement',`users) set username = ?";
        PreparedStatement s = conn.prepareStatement(sql);

        //调用setString, 参数中有恶意sql
        String username = "admin\"; DROP TABLE users; //";
        s.setString(1,username);
        s.execute();

        //检查结果，恶意sql没有被执行,表不会被删除
        ResultSet rs = s.executeQuery("select * from loadTable('dfs://db_testStatement',`users)");
        assertTrue(rs.next());
        assertEquals(username, rs.getString("username"));
        assertTrue(rs.next());
        assertEquals(username, rs.getString("username"));
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_update_executeBatch() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`id`username`password,[INT,STRING, STRING])  as users ;\n" +
                "insert into users values(1,'admin','123456');\n" +
                "insert into users values(2,'user1','123456');\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);

        //prepareStatement中输入一个update类型query
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        String sql = "update users set username = ?";
        PreparedStatement s = conn.prepareStatement(sql);

        //调用setString, 参数中有恶意sql
        String username = "admin\"; DROP TABLE users; //";
        s.setString(1,username);
        s.addBatch();
        //调用executeBatch执行query
        s.executeBatch();

        //检查结果，恶意sql没有被执行,表不会被删除
        ResultSet rs = s.executeQuery("select * from users");
        assertTrue(rs.next());
        assertEquals(username, rs.getString("username"));
        assertTrue(rs.next());
        assertEquals(username, rs.getString("username"));
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_update_executeUpdate() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`id`username`password,[INT,STRING, STRING])  as users ;\n" +
                "insert into users values(1,'admin','123456');\n" +
                "insert into users values(2,'user1','123456');\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);

        //prepareStatement中输入一个update类型query
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        String sql = "update users set username = ?";
        PreparedStatement s = conn.prepareStatement(sql);

        //调用setString, 参数中有恶意sql
        String username = "admin\"; DROP TABLE users; //";
        s.setString(1,username);
        //调用executeBatch执行query
        s.executeUpdate();

        //检查结果，恶意sql没有被执行,表不会被删除
        ResultSet rs = s.executeQuery("select * from users");
        assertTrue(rs.next());
        assertEquals(username, rs.getString("username"));
        assertTrue(rs.next());
        assertEquals(username, rs.getString("username"));
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }

    @Test
    public void test_JDBCPrepareStatement_sql_update_execute_setObject() throws SQLException, ClassNotFoundException, IOException {
        if(checkServerVersionIfSupportRunSql((JDBCConnection) conn)){
        //连接数据库，建立一个数据表
        String script = "login(\"admin\", \"123456\"); \n"+
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`id`username`password,[INT,STRING, STRING])  as users ;\n" +
                "insert into users values(1,'admin','123456');\n" +
                "insert into users values(2,'user1','123456');\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT,"admin","123456");
        db.run(script);

        //JDBC创建连接
        String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        Class.forName(JDBC_DRIVER);

        //prepareStatement中输入一个update类型query
        String url = "jdbc:dolphindb://"+HOST+":"+PORT+"?user=admin&password=123456";
        conn = DriverManager.getConnection(url);

        String sql = "update users set username = ?";
        PreparedStatement s = conn.prepareStatement(sql);

        //调用setString, 参数中有恶意sql
        String username = "admin\"; DROP TABLE users; //";
        s.setObject(1,username);
        //调用executeBatch执行query
        s.execute();

        //检查结果，恶意sql没有被执行,表不会被删除
        ResultSet rs = s.executeQuery("select * from users");
        assertTrue(rs.next());
        assertEquals(username, rs.getString("username"));
        assertTrue(rs.next());
        assertEquals(username, rs.getString("username"));
        }else{
            System.out.println("The server version does not support 'runSql'; this case will be skipped.");
        }
    }
}
