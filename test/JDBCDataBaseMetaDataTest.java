import com.dolphindb.jdbc.JDBCConnection;
import com.xxdb.DBConnection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.sql.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JDBCDataBaseMetaDataTest {
    static String HOST = JDBCTestUtil.HOST;
    static int PORT = JDBCTestUtil.PORT;
    Properties LOGININFO = new Properties();
    String JDBC_DRIVER;
    String url;

    @Before
    public void SetUp() {
        JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        LOGININFO = new Properties();
        LOGININFO.put("user", "admin");
        LOGININFO.put("password", "123456");
        url = "jdbc:dolphindb://" + HOST + ":" + PORT;
    }

    public static boolean createPartitionTable1() {
        boolean success = false;
        DBConnection db = null;
        try {
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('dfs://test_append_type_tsdb'))" +
                    "{ dropDatabase('dfs://test_append_type_tsdb')} \n" +
                    "colNames=\"col\"+string(1..29);\n" +
                    "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                    "t=table(1:0,colNames,colTypes);\n" +
                    "db=database('dfs://test_append_type_tsdb', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n" +
                    "db.createPartitionedTable(t, `pt, `col1,,`col1) \n" +
                    "db.createPartitionedTable(t, `pt1, `col1,,`col1)\n";
            db = new DBConnection();
            db.connect(HOST, PORT);
            db.run(script);
            success = true;
        } catch (Exception e) {
            e.printStackTrace();
            success = false;
        } finally {
            if (db != null) {
                db.close();
            }
            return success;
        }

    }

    public static boolean createTable1() {
        boolean success = false;
        DBConnection db = null;
        try {
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('dfs://test_append_type_tsdb'))" +
                    "{ dropDatabase('dfs://test_append_type_tsdb')} \n" +
                    "colNames=\"col\"+string(1..29);\n" +
                    "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                    "t=table(1:0,colNames,colTypes);\n" +
                    "db=database('dfs://test_append_type_tsdb', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n" +
                    "db.createTable(t, `pt,,`col1)\n" +
                    "db.createTable(t, `pt1,,`col1)\n";
            db = new DBConnection();
            db.connect(HOST, PORT);
            db.run(script);
            success = true;
        } catch (Exception e) {
            e.printStackTrace();
            success = false;
        } finally {
            if (db != null) {
                db.close();
            }
            return success;
        }

    }

    public static void printData(ResultSet rs) throws SQLException {
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        int len = resultSetMetaData.getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= len; ++i) {
                System.out.print(
                        MessageFormat.format("{0}: {1},    ", resultSetMetaData.getColumnName(i), rs.getObject(i)));
            }
            System.out.print("\n");
        }
    }
    public static String printData1(ResultSet rs) throws SQLException {
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        int len = resultSetMetaData.getColumnCount();
        String result1 = null;
        while (rs.next()) {
            for (int i = 1; i <= len; ++i) {
                result1 += MessageFormat.format("{0}: {1}    ", resultSetMetaData.getColumnName(i), rs.getObject(i));
            }
        }
        System.out.print(result1);
        result1 = result1.replaceFirst("null","");
        return result1;
    }
    public static String getTablesData(ResultSet rs) throws SQLException {
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        int len = resultSetMetaData.getColumnCount();
        String result1 = null;
        while (rs.next()) {
            for (int i = 1; i <= len; ++i) {
                result1 += MessageFormat.format("{0}: {1}    ", resultSetMetaData.getColumnName(i), rs.getObject(i));
            }
        }
        System.out.print(result1);
        return result1;
    }

    @Test
    public void test_DatabaseMetaData() throws Exception {
        System.out.println("TestStatementExecute begin");
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            ResultSet rs = null;
            DatabaseMetaData metaData = conn.getMetaData();
            rs = metaData.getCatalogs();
            printData(rs);
            rs = metaData.getColumns("", "", "", "");
            printData(rs);
            String s = metaData.getCatalogSeparator();
            System.out.println(s);
            s = metaData.getCatalogTerm();
            System.out.println(s);
            s = metaData.getDatabaseProductName();
            System.out.println(s);
            s = metaData.getDatabaseProductVersion();
            System.out.println(s);
            s = metaData.getDriverName();
            System.out.println(s);
            System.out.println(metaData.getDriverVersion());
            System.out.println(metaData.getExtraNameCharacters());
            System.out.println(metaData.getIdentifierQuoteString());
            System.out.println(metaData.getJDBCMajorVersion());
            System.out.println(metaData.getJDBCMinorVersion());
            System.out.println(metaData.getMaxBinaryLiteralLength());
            System.out.println(metaData.getMaxCatalogNameLength());
            System.out.println(metaData.getMaxColumnNameLength());
            System.out.println(metaData.getNumericFunctions());
            System.out.println(metaData.getProcedureTerm());
            System.out.println(metaData.getResultSetHoldability());
            System.out.println(metaData.getSchemaTerm());
            ResultSet ss = null;
            ss = metaData.getSchemas();
            printData(ss);
            System.out.println(metaData.getSearchStringEscape());
            System.out.println("getSQLKeywords() = " + metaData.getSQLKeywords());
            System.out.println("getSQLStateType() = " + metaData.getSQLStateType());
            System.out.println("getStringFunctions() =" + metaData.getStringFunctions());
            System.out.println("getSystemFunctions() = " + metaData.getSystemFunctions());
            printData(metaData.getTableTypes());
            System.out.println("getTimeDateFunctions() = " + metaData.getTimeDateFunctions());
            printData(metaData.getTypeInfo());
            System.out.println("getURL() = " + metaData.getURL());
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("fail");
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
        System.out.println("TestStatementExecute end");

    }
    @Test
    public void test_DatabaseMetaData_getColumns_catalog_null() throws Exception {
        DBConnection connDB = new DBConnection();
        connDB.connect(HOST,PORT,"admin","123456");
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
        ResultSet rs = null;
        connDB.run("share table(1..10 as id) as table1");
        DatabaseMetaData metaData = conn.getMetaData();
        rs = metaData.getColumns(null,null, "table1", "");
        Assert.assertEquals("COLUMN_NAME: id    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: null    ",printData1(rs));
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getColumns_catalog_null_1() throws Exception {
        DBConnection connDB = new DBConnection();
        connDB.connect(HOST,PORT,"admin","123456");
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
        ResultSet rs = null;
        connDB.run("share table(1..10 as id) as table1");
        DatabaseMetaData metaData = conn.getMetaData();
        rs = metaData.getColumns("",null, "table1", "");
        Assert.assertEquals("COLUMN_NAME: id    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: null    ",printData1(rs));
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getColumns_catalog_not_exist() throws Exception {
        DBConnection connDB = new DBConnection();
        connDB.connect(HOST,PORT,"admin","123456");
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
        ResultSet rs = null;
        connDB.run("share table(1..10 as id) as table1");
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        try{
            rs = metaData.getColumns("12323",null, "table1", "");
        }catch(Exception E){
            results = E.getMessage();
        }
        Assert.assertFalse(results.isEmpty());
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getColumns_catalog_DFS() throws Exception {
        createPartitionTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        rs = metaData.getColumns("test_append_type_tsdb",null, "pt", "");
        Assert.assertEquals("COLUMN_NAME: col1    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: null    COLUMN_NAME: col2    DATA_TYPE: BOOL    TYPE_INT: 1    EXTRA: null    COMMENT: null    COLUMN_NAME: col3    DATA_TYPE: CHAR    TYPE_INT: 2    EXTRA: null    COMMENT: null    COLUMN_NAME: col4    DATA_TYPE: SHORT    TYPE_INT: 3    EXTRA: null    COMMENT: null    COLUMN_NAME: col5    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: null    COLUMN_NAME: col6    DATA_TYPE: LONG    TYPE_INT: 5    EXTRA: null    COMMENT: null    COLUMN_NAME: col7    DATA_TYPE: DATE    TYPE_INT: 6    EXTRA: null    COMMENT: null    COLUMN_NAME: col8    DATA_TYPE: MONTH    TYPE_INT: 7    EXTRA: null    COMMENT: null    COLUMN_NAME: col9    DATA_TYPE: TIME    TYPE_INT: 8    EXTRA: null    COMMENT: null    COLUMN_NAME: col10    DATA_TYPE: MINUTE    TYPE_INT: 9    EXTRA: null    COMMENT: null    COLUMN_NAME: col11    DATA_TYPE: SECOND    TYPE_INT: 10    EXTRA: null    COMMENT: null    COLUMN_NAME: col12    DATA_TYPE: DATETIME    TYPE_INT: 11    EXTRA: null    COMMENT: null    COLUMN_NAME: col13    DATA_TYPE: TIMESTAMP    TYPE_INT: 12    EXTRA: null    COMMENT: null    COLUMN_NAME: col14    DATA_TYPE: NANOTIME    TYPE_INT: 13    EXTRA: null    COMMENT: null    COLUMN_NAME: col15    DATA_TYPE: NANOTIMESTAMP    TYPE_INT: 14    EXTRA: null    COMMENT: null    COLUMN_NAME: col16    DATA_TYPE: FLOAT    TYPE_INT: 15    EXTRA: null    COMMENT: null    COLUMN_NAME: col17    DATA_TYPE: DOUBLE    TYPE_INT: 16    EXTRA: null    COMMENT: null    COLUMN_NAME: col18    DATA_TYPE: SYMBOL    TYPE_INT: 17    EXTRA: null    COMMENT: null    COLUMN_NAME: col19    DATA_TYPE: STRING    TYPE_INT: 18    EXTRA: null    COMMENT: null    COLUMN_NAME: col20    DATA_TYPE: UUID    TYPE_INT: 19    EXTRA: null    COMMENT: null    COLUMN_NAME: col21    DATA_TYPE: DATEHOUR    TYPE_INT: 28    EXTRA: null    COMMENT: null    COLUMN_NAME: col22    DATA_TYPE: IPADDR    TYPE_INT: 30    EXTRA: null    COMMENT: null    COLUMN_NAME: col23    DATA_TYPE: INT128    TYPE_INT: 31    EXTRA: null    COMMENT: null    COLUMN_NAME: col24    DATA_TYPE: BLOB    TYPE_INT: 32    EXTRA: null    COMMENT: null    COLUMN_NAME: col25    DATA_TYPE: COMPLEX    TYPE_INT: 34    EXTRA: null    COMMENT: null    COLUMN_NAME: col26    DATA_TYPE: POINT    TYPE_INT: 35    EXTRA: null    COMMENT: null    COLUMN_NAME: col27    DATA_TYPE: DECIMAL32(2)    TYPE_INT: 37    EXTRA: 2    COMMENT: null    COLUMN_NAME: col28    DATA_TYPE: DECIMAL64(7)    TYPE_INT: 38    EXTRA: 7    COMMENT: null    COLUMN_NAME: col29    DATA_TYPE: DECIMAL128(19)    TYPE_INT: 39    EXTRA: 19    COMMENT: null    ",printData1(rs));
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getColumns_schemaPattern_null() throws Exception {
        createPartitionTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        rs = metaData.getColumns("test_append_type_tsdb","", "pt", "");
        Assert.assertEquals("COLUMN_NAME: col1    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: null    COLUMN_NAME: col2    DATA_TYPE: BOOL    TYPE_INT: 1    EXTRA: null    COMMENT: null    COLUMN_NAME: col3    DATA_TYPE: CHAR    TYPE_INT: 2    EXTRA: null    COMMENT: null    COLUMN_NAME: col4    DATA_TYPE: SHORT    TYPE_INT: 3    EXTRA: null    COMMENT: null    COLUMN_NAME: col5    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: null    COLUMN_NAME: col6    DATA_TYPE: LONG    TYPE_INT: 5    EXTRA: null    COMMENT: null    COLUMN_NAME: col7    DATA_TYPE: DATE    TYPE_INT: 6    EXTRA: null    COMMENT: null    COLUMN_NAME: col8    DATA_TYPE: MONTH    TYPE_INT: 7    EXTRA: null    COMMENT: null    COLUMN_NAME: col9    DATA_TYPE: TIME    TYPE_INT: 8    EXTRA: null    COMMENT: null    COLUMN_NAME: col10    DATA_TYPE: MINUTE    TYPE_INT: 9    EXTRA: null    COMMENT: null    COLUMN_NAME: col11    DATA_TYPE: SECOND    TYPE_INT: 10    EXTRA: null    COMMENT: null    COLUMN_NAME: col12    DATA_TYPE: DATETIME    TYPE_INT: 11    EXTRA: null    COMMENT: null    COLUMN_NAME: col13    DATA_TYPE: TIMESTAMP    TYPE_INT: 12    EXTRA: null    COMMENT: null    COLUMN_NAME: col14    DATA_TYPE: NANOTIME    TYPE_INT: 13    EXTRA: null    COMMENT: null    COLUMN_NAME: col15    DATA_TYPE: NANOTIMESTAMP    TYPE_INT: 14    EXTRA: null    COMMENT: null    COLUMN_NAME: col16    DATA_TYPE: FLOAT    TYPE_INT: 15    EXTRA: null    COMMENT: null    COLUMN_NAME: col17    DATA_TYPE: DOUBLE    TYPE_INT: 16    EXTRA: null    COMMENT: null    COLUMN_NAME: col18    DATA_TYPE: SYMBOL    TYPE_INT: 17    EXTRA: null    COMMENT: null    COLUMN_NAME: col19    DATA_TYPE: STRING    TYPE_INT: 18    EXTRA: null    COMMENT: null    COLUMN_NAME: col20    DATA_TYPE: UUID    TYPE_INT: 19    EXTRA: null    COMMENT: null    COLUMN_NAME: col21    DATA_TYPE: DATEHOUR    TYPE_INT: 28    EXTRA: null    COMMENT: null    COLUMN_NAME: col22    DATA_TYPE: IPADDR    TYPE_INT: 30    EXTRA: null    COMMENT: null    COLUMN_NAME: col23    DATA_TYPE: INT128    TYPE_INT: 31    EXTRA: null    COMMENT: null    COLUMN_NAME: col24    DATA_TYPE: BLOB    TYPE_INT: 32    EXTRA: null    COMMENT: null    COLUMN_NAME: col25    DATA_TYPE: COMPLEX    TYPE_INT: 34    EXTRA: null    COMMENT: null    COLUMN_NAME: col26    DATA_TYPE: POINT    TYPE_INT: 35    EXTRA: null    COMMENT: null    COLUMN_NAME: col27    DATA_TYPE: DECIMAL32(2)    TYPE_INT: 37    EXTRA: 2    COMMENT: null    COLUMN_NAME: col28    DATA_TYPE: DECIMAL64(7)    TYPE_INT: 38    EXTRA: 7    COMMENT: null    COLUMN_NAME: col29    DATA_TYPE: DECIMAL128(19)    TYPE_INT: 39    EXTRA: 19    COMMENT: null    ",printData1(rs));
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getColumns_schemaPattern_not_exist() throws Exception {
        createPartitionTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        rs = metaData.getColumns("test_append_type_tsdb","test_append_type_tsdb对方的对方对方的", "pt", "");
        Assert.assertEquals("COLUMN_NAME: col1    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: null    COLUMN_NAME: col2    DATA_TYPE: BOOL    TYPE_INT: 1    EXTRA: null    COMMENT: null    COLUMN_NAME: col3    DATA_TYPE: CHAR    TYPE_INT: 2    EXTRA: null    COMMENT: null    COLUMN_NAME: col4    DATA_TYPE: SHORT    TYPE_INT: 3    EXTRA: null    COMMENT: null    COLUMN_NAME: col5    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: null    COLUMN_NAME: col6    DATA_TYPE: LONG    TYPE_INT: 5    EXTRA: null    COMMENT: null    COLUMN_NAME: col7    DATA_TYPE: DATE    TYPE_INT: 6    EXTRA: null    COMMENT: null    COLUMN_NAME: col8    DATA_TYPE: MONTH    TYPE_INT: 7    EXTRA: null    COMMENT: null    COLUMN_NAME: col9    DATA_TYPE: TIME    TYPE_INT: 8    EXTRA: null    COMMENT: null    COLUMN_NAME: col10    DATA_TYPE: MINUTE    TYPE_INT: 9    EXTRA: null    COMMENT: null    COLUMN_NAME: col11    DATA_TYPE: SECOND    TYPE_INT: 10    EXTRA: null    COMMENT: null    COLUMN_NAME: col12    DATA_TYPE: DATETIME    TYPE_INT: 11    EXTRA: null    COMMENT: null    COLUMN_NAME: col13    DATA_TYPE: TIMESTAMP    TYPE_INT: 12    EXTRA: null    COMMENT: null    COLUMN_NAME: col14    DATA_TYPE: NANOTIME    TYPE_INT: 13    EXTRA: null    COMMENT: null    COLUMN_NAME: col15    DATA_TYPE: NANOTIMESTAMP    TYPE_INT: 14    EXTRA: null    COMMENT: null    COLUMN_NAME: col16    DATA_TYPE: FLOAT    TYPE_INT: 15    EXTRA: null    COMMENT: null    COLUMN_NAME: col17    DATA_TYPE: DOUBLE    TYPE_INT: 16    EXTRA: null    COMMENT: null    COLUMN_NAME: col18    DATA_TYPE: SYMBOL    TYPE_INT: 17    EXTRA: null    COMMENT: null    COLUMN_NAME: col19    DATA_TYPE: STRING    TYPE_INT: 18    EXTRA: null    COMMENT: null    COLUMN_NAME: col20    DATA_TYPE: UUID    TYPE_INT: 19    EXTRA: null    COMMENT: null    COLUMN_NAME: col21    DATA_TYPE: DATEHOUR    TYPE_INT: 28    EXTRA: null    COMMENT: null    COLUMN_NAME: col22    DATA_TYPE: IPADDR    TYPE_INT: 30    EXTRA: null    COMMENT: null    COLUMN_NAME: col23    DATA_TYPE: INT128    TYPE_INT: 31    EXTRA: null    COMMENT: null    COLUMN_NAME: col24    DATA_TYPE: BLOB    TYPE_INT: 32    EXTRA: null    COMMENT: null    COLUMN_NAME: col25    DATA_TYPE: COMPLEX    TYPE_INT: 34    EXTRA: null    COMMENT: null    COLUMN_NAME: col26    DATA_TYPE: POINT    TYPE_INT: 35    EXTRA: null    COMMENT: null    COLUMN_NAME: col27    DATA_TYPE: DECIMAL32(2)    TYPE_INT: 37    EXTRA: 2    COMMENT: null    COLUMN_NAME: col28    DATA_TYPE: DECIMAL64(7)    TYPE_INT: 38    EXTRA: 7    COMMENT: null    COLUMN_NAME: col29    DATA_TYPE: DECIMAL128(19)    TYPE_INT: 39    EXTRA: 19    COMMENT: null    ",printData1(rs));
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getColumns_tableNamePattern_null() throws Exception {
        createPartitionTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        try{
            rs = metaData.getColumns("test_append_type_tsdb","", "", "");
        }catch(Exception E){
            results = E.getMessage();
        }
        Assert.assertFalse(results.isEmpty());
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getColumns_tableNamePattern_null_1() throws Exception {
        createPartitionTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        try{
            rs = metaData.getColumns("","", "", "");
        }catch(Exception E){
            results = E.getMessage();
            System.out.println(results);
        }
        Assert.assertFalse(results.isEmpty());
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getColumns_tableNamePattern_dfs() throws Exception {
        createPartitionTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        stmt.execute("pt = loadTable(\"dfs://test_append_type_tsdb\",`pt)");
        rs = metaData.getColumns("","", "pt", "");
        Assert.assertEquals("COLUMN_NAME: col1    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: null    COLUMN_NAME: col2    DATA_TYPE: BOOL    TYPE_INT: 1    EXTRA: null    COMMENT: null    COLUMN_NAME: col3    DATA_TYPE: CHAR    TYPE_INT: 2    EXTRA: null    COMMENT: null    COLUMN_NAME: col4    DATA_TYPE: SHORT    TYPE_INT: 3    EXTRA: null    COMMENT: null    COLUMN_NAME: col5    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: null    COLUMN_NAME: col6    DATA_TYPE: LONG    TYPE_INT: 5    EXTRA: null    COMMENT: null    COLUMN_NAME: col7    DATA_TYPE: DATE    TYPE_INT: 6    EXTRA: null    COMMENT: null    COLUMN_NAME: col8    DATA_TYPE: MONTH    TYPE_INT: 7    EXTRA: null    COMMENT: null    COLUMN_NAME: col9    DATA_TYPE: TIME    TYPE_INT: 8    EXTRA: null    COMMENT: null    COLUMN_NAME: col10    DATA_TYPE: MINUTE    TYPE_INT: 9    EXTRA: null    COMMENT: null    COLUMN_NAME: col11    DATA_TYPE: SECOND    TYPE_INT: 10    EXTRA: null    COMMENT: null    COLUMN_NAME: col12    DATA_TYPE: DATETIME    TYPE_INT: 11    EXTRA: null    COMMENT: null    COLUMN_NAME: col13    DATA_TYPE: TIMESTAMP    TYPE_INT: 12    EXTRA: null    COMMENT: null    COLUMN_NAME: col14    DATA_TYPE: NANOTIME    TYPE_INT: 13    EXTRA: null    COMMENT: null    COLUMN_NAME: col15    DATA_TYPE: NANOTIMESTAMP    TYPE_INT: 14    EXTRA: null    COMMENT: null    COLUMN_NAME: col16    DATA_TYPE: FLOAT    TYPE_INT: 15    EXTRA: null    COMMENT: null    COLUMN_NAME: col17    DATA_TYPE: DOUBLE    TYPE_INT: 16    EXTRA: null    COMMENT: null    COLUMN_NAME: col18    DATA_TYPE: SYMBOL    TYPE_INT: 17    EXTRA: null    COMMENT: null    COLUMN_NAME: col19    DATA_TYPE: STRING    TYPE_INT: 18    EXTRA: null    COMMENT: null    COLUMN_NAME: col20    DATA_TYPE: UUID    TYPE_INT: 19    EXTRA: null    COMMENT: null    COLUMN_NAME: col21    DATA_TYPE: DATEHOUR    TYPE_INT: 28    EXTRA: null    COMMENT: null    COLUMN_NAME: col22    DATA_TYPE: IPADDR    TYPE_INT: 30    EXTRA: null    COMMENT: null    COLUMN_NAME: col23    DATA_TYPE: INT128    TYPE_INT: 31    EXTRA: null    COMMENT: null    COLUMN_NAME: col24    DATA_TYPE: BLOB    TYPE_INT: 32    EXTRA: null    COMMENT: null    COLUMN_NAME: col25    DATA_TYPE: COMPLEX    TYPE_INT: 34    EXTRA: null    COMMENT: null    COLUMN_NAME: col26    DATA_TYPE: POINT    TYPE_INT: 35    EXTRA: null    COMMENT: null    COLUMN_NAME: col27    DATA_TYPE: DECIMAL32(2)    TYPE_INT: 37    EXTRA: 2    COMMENT: null    COLUMN_NAME: col28    DATA_TYPE: DECIMAL64(7)    TYPE_INT: 38    EXTRA: 7    COMMENT: null    COLUMN_NAME: col29    DATA_TYPE: DECIMAL128(19)    TYPE_INT: 39    EXTRA: 19    COMMENT: null    ",printData1(rs));
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getColumns_tableNamePattern_memory_table() throws Exception {
        createPartitionTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        stmt.execute("dfsptfdfd时代的 =  table(1..10 as id)");
        rs = metaData.getColumns("","", "dfsptfdfd时代的", "");
        Assert.assertEquals("COLUMN_NAME: id    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: null    ",printData1(rs));
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getColumns_columnNamePattern_null() throws Exception {
        createPartitionTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        stmt.execute("dfsptfdfd时代的 =  table(1..10 as id)");
        rs = metaData.getColumns("","", "dfsptfdfd时代的", null);
        Assert.assertEquals("COLUMN_NAME: id    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: null    ",printData1(rs));
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getColumns_columnNamePattern_not_exist() throws Exception {
        createPartitionTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        stmt.execute("dfsptfdfd时代的 =  table(1..10 as id)");
        rs = metaData.getColumns("","", "dfsptfdfd时代的", "  ds时代的     ");
        Assert.assertEquals("COLUMN_NAME: id    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: null    ",printData1(rs));
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getColumns_columnNamePattern_DFS_exist() throws Exception {
        createPartitionTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        stmt.execute("pt = loadTable(\"dfs://test_append_type_tsdb\",`pt);setColumnComment(pt,{col1:\"标志符1！！！！！\",col2:\"标志符2@@@@@\",col3:\"标志符3#￥%……&*()\",col4:\"4fdfee\",col5:\"5__++_-=\",col6:\"6^^<>?>>>>\",col7:\"标志符7\"})");
        String results = null;
        rs = metaData.getColumns("test_append_type_tsdb",null, "pt", "col1");
        //printData(rs);
        Assert.assertEquals("COLUMN_NAME: col1    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: 标志符1！！！！！    ",printData1(rs));
        rs = metaData.getColumns("test_append_type_tsdb",null, "pt", "col2");
        Assert.assertEquals("COLUMN_NAME: col2    DATA_TYPE: BOOL    TYPE_INT: 1    EXTRA: null    COMMENT: 标志符2@@@@@    ",printData1(rs));
        rs = metaData.getColumns("test_append_type_tsdb",null, "pt", "col3");
        Assert.assertEquals("COLUMN_NAME: col3    DATA_TYPE: CHAR    TYPE_INT: 2    EXTRA: null    COMMENT: 标志符3#￥%……&*()    ",printData1(rs));
        rs = metaData.getColumns("test_append_type_tsdb",null, "pt", "col29");
        Assert.assertEquals("COLUMN_NAME: col29    DATA_TYPE: DECIMAL128(19)    TYPE_INT: 39    EXTRA: 19    COMMENT: null    ",printData1(rs));

        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getColumns_columnNamePattern_memory_table_exist() throws Exception {
        createPartitionTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        stmt.execute("dfsptfdfd时代的 =  table(1..10 as id,take(`weqw`wewe`qa22,10) as sym)");
        rs = metaData.getColumns("","", "dfsptfdfd时代的", "id");
        Assert.assertEquals("COLUMN_NAME: id    DATA_TYPE: INT    TYPE_INT: 4    EXTRA: null    COMMENT: null    ",printData1(rs));
        stmt.close();
        conn.close();
    }

    @Test
    public void test_DatabaseMetaData_getTables_catalog_null() throws Exception {
        createPartitionTable1();
        DBConnection connDB = new DBConnection();
        connDB.connect(HOST,PORT,"admin","123456");
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        stmt.execute("dfsptfdfd时代的 =  table(1..10 as id)");
        connDB.run("share table(1..1234 as id) as shareTable1234;");
        rs = metaData.getTables(null,null,"%", null);
        //printData(rs);
        String results1 = getTablesData(rs);
        Assert.assertFalse(results1.isEmpty());
        Assert.assertTrue(results1.contains("dfsptfdfd时代的"));
        Assert.assertTrue(results1.contains("dfs://test_append_type_tsdb"));
        Assert.assertTrue(results1.contains("shareTable1234"));

        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getTables_catalog_null_1() throws Exception {
        createPartitionTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        stmt.execute("dfsptfdfd时代的 =  table(1..10 as id)");
        rs = metaData.getTables("",null,"%", null);
        //printData(rs);
        String results1 = getTablesData(rs);
        Assert.assertFalse(results1.isEmpty());
        Assert.assertTrue(results1.contains("dfsptfdfd时代的"));
        Assert.assertTrue(results1.contains("dfs://test_append_type_tsdb"));
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getTables_catalog_not_exist() throws Exception {
        createPartitionTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        stmt.execute("dfsptfdfd时代的 =  table(1..10 as id)");
        String results = null;
        try{
            rs = metaData.getTables("test_append_type_tsdb1212",null,"pt", null);
        }catch(Exception E){
            results = E.getMessage();
            System.out.println(results);
        }
        Assert.assertFalse(results.isEmpty());
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getTables_catalog_dfs() throws Exception {
        createPartitionTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        rs = metaData.getTables("test_append_type_tsdb",null,"pt1", null);
        String results1 = getTablesData(rs);
        //printData(rs);
        Assert.assertEquals("nullTABLE_CAT: dfs://test_append_type_tsdb    TABLE_NAME: pt1    TABLE_SCHEM: null    TABLE_TYPE: TABLE    REMARKS: null    ",results1);
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getTables_catalog_dimension() throws Exception {
        createTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        //stmt.execute("dfsptfdfd时代的 =  table(1..10 as id)");
        rs = metaData.getTables("test_append_type_tsdb",null,"pt1", null);
        String results1 = getTablesData(rs);
        //printData(rs);
        Assert.assertEquals("nullTABLE_CAT: dfs://test_append_type_tsdb    TABLE_NAME: pt1    TABLE_SCHEM: null    TABLE_TYPE: TABLE    REMARKS: null    ",results1);
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getTables_schemaPattern_null() throws Exception {
        createTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        rs = metaData.getTables("test_append_type_tsdb","","pt1", null);
        String results1 = getTablesData(rs);
        //printData(rs);
        Assert.assertEquals("nullTABLE_CAT: dfs://test_append_type_tsdb    TABLE_NAME: pt1    TABLE_SCHEM: null    TABLE_TYPE: TABLE    REMARKS: null    ",results1);
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getTables_schemaPattern_not_exist() throws Exception {
        createTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        rs = metaData.getTables("test_append_type_tsdb","sdsdswsaSFE    份额","pt1", null);
        String results1 = getTablesData(rs);
        //printData(rs);
        Assert.assertEquals("nullTABLE_CAT: dfs://test_append_type_tsdb    TABLE_NAME: pt1    TABLE_SCHEM: null    TABLE_TYPE: TABLE    REMARKS: null    ",results1);
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getTables_tableNamePattern_null() throws Exception {
        createTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        rs = metaData.getTables("test_append_type_tsdb",null,null, null);
        String results1 = getTablesData(rs);
        //printData(rs);
        Assert.assertEquals("nullTABLE_CAT: dfs://test_append_type_tsdb    TABLE_NAME: pt1    TABLE_SCHEM: null    TABLE_TYPE: TABLE    REMARKS: null    ",results1);
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getTables_tableNamePattern_null_1() throws Exception {
        createTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        rs = metaData.getTables("test_append_type_tsdb",null,"", null);
        String results1 = getTablesData(rs);
        //printData(rs);
        Assert.assertEquals("nullTABLE_CAT: dfs://test_append_type_tsdb    TABLE_NAME: pt1    TABLE_SCHEM: null    TABLE_TYPE: TABLE    REMARKS: null    ",results1);
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getTables_tableNamePattern_all() throws Exception {
        createTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        rs = metaData.getTables("test_append_type_tsdb",null,"%", null);
        String results1 = getTablesData(rs);
        //printData(rs);
        Assert.assertEquals("nullTABLE_CAT: dfs://test_append_type_tsdb    TABLE_NAME: pt1    TABLE_SCHEM: null    TABLE_TYPE: TABLE    REMARKS: null    ",results1);
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getTables_tableNamePattern_memory_table() throws Exception {
        createTable1();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        stmt.execute("dfsptfdfd时代的 =  table(1..10 as id);");
        rs = metaData.getTables("",null,"dfsptfdfd时代的", null);
        String results1 = getTablesData(rs);
        //printData(rs);
        Assert.assertEquals("nullTABLE_CAT: null    TABLE_NAME: dfsptfdfd时代的    TABLE_SCHEM: null    TABLE_TYPE: TABLE    REMARKS: null    ",results1);
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_tableNamePattern_share_table() throws Exception {
        DBConnection connDB = new DBConnection();
        connDB.connect(HOST,PORT,"admin","123456");
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        connDB.run("share table(1..1234 as id) as shareTable1234;");
        rs = metaData.getTables(null,null,"shareTable1234", null);
        //printData(rs);
        String results1 = getTablesData(rs);
        Assert.assertEquals("nullTABLE_CAT: null    TABLE_NAME: shareTable1234    TABLE_SCHEM: null    TABLE_TYPE: TABLE    REMARKS: null    ",results1);
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_types_null() throws Exception {
        DBConnection connDB = new DBConnection();
        connDB.connect(HOST,PORT,"admin","123456");
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        connDB.run("share table(1..1234 as id) as shareTable1234;");
        rs = metaData.getTables(null,null,"shareTable1234", new String[]{});
        //printData(rs);
        String results1 = getTablesData(rs);
        Assert.assertEquals("nullTABLE_CAT: null    TABLE_NAME: shareTable1234    TABLE_SCHEM: null    TABLE_TYPE: TABLE    REMARKS: null    ",results1);
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_types_TABLE() throws Exception {
        DBConnection connDB = new DBConnection();
        connDB.connect(HOST,PORT,"admin","123456");
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        connDB.run("share table(1..1234 as id) as shareTable1234;");
        rs = metaData.getTables(null,null,"shareTable1234", new String[]{"TABLE"});
        //printData(rs);
        String results1 = getTablesData(rs);
        Assert.assertEquals("nullTABLE_CAT: null    TABLE_NAME: shareTable1234    TABLE_SCHEM: null    TABLE_TYPE: TABLE    REMARKS: null    ",results1);
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_types_VIEW() throws Exception {
        DBConnection connDB = new DBConnection();
        connDB.connect(HOST,PORT,"admin","123456");
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
        connDB.run("share table(1..1234 as id) as shareTable1234;");
        rs = metaData.getTables(null,null,"shareTable1234", new String[]{"1233"});
        printData(rs);
        //String results1 = getTablesData(rs);
        //Assert.assertEquals("nullTABLE_CAT: null    TABLE_NAME: shareTable1234    TABLE_SCHEM: null    TABLE_TYPE: TABLE    REMARKS: null    ",results1);
        stmt.close();
        conn.close();
    }
}
