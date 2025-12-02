import com.dolphindb.jdbc.JDBCConnection;
import com.xxdb.DBConnection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.Properties;

import static com.dolphindb.jdbc.Utils.checkServerVersionIfSupportCatalog;

public class JDBCDataBaseMetaDataTest {
    static String HOST = JDBCTestUtil.HOST;
    static int PORT = JDBCTestUtil.PORT;
    Properties LOGININFO = new Properties();
    String JDBC_DRIVER;
    String url;
    Properties prop = new Properties();
    public void clear_env() throws IOException {
        DBConnection conn = new DBConnection();
        conn.connect(HOST,PORT,"admin","123456");
        conn.run("def getAllShare(){\n" +
                "\treturn select name from objs(true) where shared=1\n" +
                "\t}\n" +
                "\n" +
                "def clearShare(){\n" +
                "\tlogin(`admin,`123456)\n" +
                "\tallShare=exec name from pnodeRun(getAllShare)\n" +
                "\tfor(i in allShare){\n" +
                "\t\ttry{\n" +
                "\t\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],clearTablePersistence,objByName(i))\n" +
                "\t\t\t}catch(ex1){}\n" +
                "\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],undef,i,SHARED)\n" +
                "\t}\n" +
                "\ttry{\n" +
                "\t\tPST_DIR=rpc(getControllerAlias(),getDataNodeConfig{getNodeAlias()})['persistenceDir']\n" +
                "\t}catch(ex1){}\n" +
                "}\n" +
                "clearShare()");
    }
    @Before
    public void SetUp() throws IOException {
        clear_env();
        JDBC_DRIVER = "com.dolphindb.jdbc.Driver";
        LOGININFO = new Properties();
        LOGININFO.put("user", "admin");
        LOGININFO.put("password", "123456");
        url = "jdbc:dolphindb://" + HOST + ":" + PORT+"?user=admin&password=123456";
        prop.setProperty("hostName",HOST);
        prop.setProperty("port",String.valueOf(PORT));
        //prop.setProperty("sqlStd", String.valueOf(SqlStdEnum.Oracle));
        url = "jdbc:dolphindb://"+JDBCTestUtil.HOST+":"+JDBCTestUtil.PORT;

    }

    public static boolean createTable(String dataBaseName) {
        boolean success = false;
        DBConnection db = null;
        try {
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('"+dataBaseName+"'))" +
                    "{ dropDatabase('"+dataBaseName+"')} \n" +
                    "colNames=\"col\"+string(1..3);\n" +
                    "colTypes=[INT,DECIMAL128(19),DECIMAL128(19)[]];\n" +
                    "t=table(1:0,colNames,colTypes);\n" +
                    "db=database('"+dataBaseName+"', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n" +
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

    public static boolean createPartitionTable1(String dataBaseName) {
        boolean success = false;
        DBConnection db = null;
        try {
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('"+dataBaseName+"'))" +
                    "{ dropDatabase('"+dataBaseName+"')} \n" +
                    "colNames=\"col\"+string(1..29);\n" +
                    "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                    "t=table(1:0,colNames,colTypes);\n" +
                    "db=database('"+dataBaseName+"', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n" +
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

    public static boolean createTable1(String dataBaseName) {
        boolean success = false;
        DBConnection db = null;
        try {
            String script = "login(`admin, `123456); \n" +
                    "if(existsDatabase('"+dataBaseName+"'))" +
                    "{ dropDatabase('"+dataBaseName+"')} \n" +
                    "colNames=\"col\"+string(1..29);\n" +
                    "colTypes=[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID,DATEHOUR,IPADDR,INT128,BLOB,COMPLEX,POINT,DECIMAL32(2),DECIMAL64(7),DECIMAL128(19)];\n" +
                    "t=table(1:0,colNames,colTypes);\n" +
                    "db=database('"+dataBaseName+"', RANGE, 1 2001 4001 6001 8001 10001,,'TSDB') \n" +
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
    public static boolean createPartitionTable_Array1() {
        boolean success = false;
        DBConnection db = null;
        try{
            String script = "login(`admin, `123456); \n"+
                    "if(existsDatabase('dfs://test_append_array_tsdb1'))" +
                    "{ dropDatabase('dfs://test_append_array_tsdb1')} \n"+
                    "colNames=\"col\"+string(1..26);\n" +
//                    "colNames=\"col\"+string(1..2);\n" +
//                    "colTypes=[INT,"+dataType+"[]];\n" +
                    "colTypes=[INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],MONTH[],TIME[],MINUTE[],SECOND[],DATETIME[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],UUID[],DATEHOUR[],IPADDR[],INT128[],COMPLEX[],POINT[],DECIMAL32(2)[],DECIMAL64(7)[],DECIMAL128(19)[]];\n" +
                    "t=table(1:0,colNames,colTypes);\n" +
                    "db=database('dfs://test_append_array_tsdb1', RANGE, 1 2001 4001 6001 8001 10001 10000000,,'TSDB') \n"+
                    "db.createPartitionedTable(t, `pt, `col1,,`col1)\n";
            db = new DBConnection();
            db.connect(HOST, PORT);
            db.run(script);
            success = true;
        }catch(Exception e){
            e.printStackTrace();
            success = false;
        }finally{
            if(db != null){
                db.close();
            }
            return success;
        }
    }
    public static boolean createCatalog(String CatalogName) {
        boolean success = false;
        DBConnection db = null;
        try {
            String script = " login(`admin, `123456); \n" +
                    "try{\n dropCatalog(\""+ CatalogName +"\")\n }catch(ex){\n }\n" +
                    "createCatalog(\""+ CatalogName +"\")\n";
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
    public static boolean createSchema(String CatalogName,String dbName,String schemaName) {
        boolean success = false;
        DBConnection db = null;
        try {
            String script = " login(`admin, `123456); \n" +
                    "dbName = \""+ dbName +"\"\n" +
                    "if(existsDatabase(dbName)){\n" +
                    "        dropDatabase(dbName)\n" +
                    "}\n" +
                    "n=1000        \n" +
                    "ID=rand(10, n)\n" +
                    "x=rand(1.0, n)\n" +
                    "sys=take(`qq`www`ddd, n)\n" +
                    "t=table(ID, x);\n" +
                    "t1=table(ID, x, sys);\n" +
                    "db=database(dbName,RANGE,  0 5 10)\n" +
                    "db.createPartitionedTable(t, `pt, `ID).append!(t);\n" +
                    "db.createTable(t1, `dt).append!(t);" +
                    "try{\n dropCatalog(\""+ CatalogName +"\")\n }catch(ex){\n }\n" +
                    "createCatalog(\""+ CatalogName +"\")\n"+
                    "createSchema(\""+ CatalogName +"\", \""+ dbName +"\", \""+schemaName+"\")\n";
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
    public static boolean createSchema_decimal(String CatalogName,String dbName,String schemaName) {
        boolean success = false;
        DBConnection db = null;
        try {
            String script = " login(`admin, `123456); \n" +
                    "dbName = \""+ dbName +"\"\n" +
                    "if(existsDatabase(dbName)){\n" +
                    "        dropDatabase(dbName)\n" +
                    "}\n" +
                    "n=1000\n" +
                    "ID=rand(10, n)\n" +
                    "cdecimal32=decimal32(rand(1.0, n),8)\n" +
                    "cdecimal64=decimal64(rand(1.0, n),17)\n" +
                    "cdecimal128=decimal128(rand(1.0, n),30)\n" +
                    "t=table(ID, cdecimal32,cdecimal64,cdecimal128);\n" +
                    "t1=table(ID, cdecimal32);\n" +
                    "db=database(dbName,RANGE,  0 5 10)\n" +
                    "pt=db.createPartitionedTable(t, `pt, `ID);\n" +
                    "pt.append!(t);\n" +
                    "setColumnComment(pt,{ID:\"股票代码\",cdecimal32:\"decimal32类型\",cdecimal64:\"decimal64(17)类型\",cdecimal128:\"decimal128类型\"});\n" +
                    "db.createTable(t1, `dt).append!(t1);" +
                    "try{\n dropCatalog(\""+ CatalogName +"\")\n }catch(ex){\n }\n" +
                    "createCatalog(\""+ CatalogName +"\")\n"+
                    "createSchema(\""+ CatalogName +"\", \""+ dbName +"\", \""+schemaName+"\")\n";
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
    public static boolean createTable(String CatalogName,String dbName,String schemaName) {
        boolean success = false;
        DBConnection db = null;
        try {
            String script = " login(`admin, `123456); \n" +
                    "dbName = \""+ dbName +"\"\n" +
                    "if(existsDatabase(dbName)){\n" +
                    "        dropDatabase(dbName)\n" +
                    "}\n" +
                    "n=1000        \n" +
                    "ID=rand(10, n)\n" +
                    "x=rand(1.0, n)\n" +
                    "t=table(ID, x);\n" +
                    "db=database(dbName,RANGE,  0 5 10)\n" +
                    "db.createPartitionedTable(t, `pt, `ID).append!(t);\n" +
                    "db.createTable(t, `dt).append!(t);" +
                    "try{\n dropCatalog(\"catalog1\")\n }catch(ex){\n }\n" +
                    "createCatalog(\""+ CatalogName +"\")\n"+
                    "createSchema(\""+ CatalogName +"\", \""+ dbName +"\", \""+schemaName+"\")\n";
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
        if (result1 != null) {
            result1 = result1.replaceFirst("null","");
        } else {
            return null;
        }
        return result1;
    }
    public static String getTablesData(ResultSet rs) throws SQLException {
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        int len = resultSetMetaData.getColumnCount();
        String result1 = "";
        while (rs.next()) {
            for (int i = 1; i <= len; ++i) {
                result1 += MessageFormat.format("{0}: {1}    ", resultSetMetaData.getColumnName(i), rs.getObject(i));
            }
            result1 +="\n";
        }
        if (result1 != null) {
            System.out.print(result1);
            return result1;
        } else {
            return null;
        }
    }

    @Test
    public void test_DatabaseMetaData() throws Exception {
        System.out.println("TestStatementExecute begin");
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(url,LOGININFO);
            stmt = conn.createStatement();
            ResultSet rs = null;
            DatabaseMetaData metaData = conn.getMetaData();
            rs = metaData.getCatalogs();
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
            System.out.println("metaData.getTableTypes()=-------------------------------- ");
            printData(metaData.getTableTypes());
            System.out.println("metaData.getTableTypes()=-------------------------------- ");

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
    public void test_DatabaseMetaData_getCatalogs() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        DBConnection connDB = new DBConnection();
        connDB.connect(HOST,PORT,"admin","123456");
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
        connDB.run("login(`admin, `123456); \ntry{\n dropCatalog(\"catalog1\")\n }catch(ex){\n }\n ");
        DatabaseMetaData metaData = conn.getMetaData();
        rs = metaData.getCatalogs();
        //printData1(rs);
        Assert.assertFalse(printData1(rs).contains("catalog1"));
        createCatalog("catalog1");
        createCatalog("aaaatalog1");
        DatabaseMetaData metaData1 = conn.getMetaData();
        rs = metaData1.getCatalogs();
        Assert.assertEquals(true,printData1(rs).contains("catalog1"));
        }else{
            //200分支 catalog 返回DolphinDB
            connDB.run("login(`admin, `123456); \nif(existsDatabase('dfs://test_append_type_tsdb1234')) \n{ dropDatabase('dfs://test_append_type_tsdb1234')}");
            DatabaseMetaData metaData = conn.getMetaData();
            rs = metaData.getCatalogs();
            //printData1(rs);
            Assert.assertEquals(true,printData1(rs).contains("DolphinDB"));
        }
        stmt.close();
        conn.close();
    }

    @Test
    public void test_DatabaseMetaData_getSchemas_NULL() throws Exception {
        url = "jdbc:dolphindb://"+JDBCTestUtil.HOST+":"+JDBCTestUtil.PORT;
        System.out.println("TestStatementExecute begin");
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet ss = null;
        ss = metaData.getSchemas();
        printData(ss);
    }
    @Test
    public void test_DatabaseMetaData_getSchemas() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        DBConnection connDB = new DBConnection();
        connDB.connect(HOST,PORT,"admin","123456");
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
        connDB.run("login(`admin, `123456); \ntry{\n dropSchema(\"catalog1\",\"schema_test2\")\n }catch(ex){\n }\n ");
        DatabaseMetaData metaData = conn.getMetaData();
        rs = metaData.getSchemas();
        Assert.assertFalse(printData1(rs).contains("schema_test2"));
        createSchema("catalog1","dfs://db","schema_test2");
        DatabaseMetaData metaData1 = conn.getMetaData();
        rs = metaData1.getSchemas();
        Assert.assertEquals(true,printData1(rs).contains("schema_test2"));
        }else{
            createTable1("dfs://test_append_type_tsdb1");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getSchemas();
            Assert.assertEquals(true,printData1(rs).contains("TABLE_SCHEM: test_append_type_tsdb1    TABLE_CATALOG: DolphinDB"));
        }
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getSchemas_Catalog() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        DBConnection connDB = new DBConnection();
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        connDB.connect(HOST,PORT,"admin","123456");
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
        connDB.run("login(`admin, `123456); \ntry{\n dropCatalog(\"catalog1\");createCatalog(\"catalog1\");\n dropSchema(\"catalog1\",\"schema_test\")\n }catch(ex){\n }\n ");
        DatabaseMetaData metaData = conn.getMetaData();
        rs = metaData.getSchemas("catalog1", "%");
        Assert.assertNull(printData1(rs));
        createSchema("catalog1","dfs://db","schema_test");
        DatabaseMetaData metaData1 = conn.getMetaData();
        rs = metaData1.getSchemas("catalog1", "%");
        Assert.assertEquals(true,printData1(rs).contains("schema_test"));
        }else{
            createTable1("dfs://test_append_type_tsdb1");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getSchemas("DolphinDB","%");
            Assert.assertEquals(true,printData1(rs).contains("TABLE_SCHEM: test_append_type_tsdb1    TABLE_CATALOG: DolphinDB"));
        }
        stmt.close();
        conn.close();
    }

    @Test//not support
    public void test_DatabaseMetaData_getSchemas_schemaPattern_not_support() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        DBConnection connDB = new DBConnection();
        connDB.connect(HOST,PORT,"admin","123456");
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
            connDB.run("login(`admin, `123456); \ntry{\n createCatalog(\"catalog1\");\n dropSchema(\"catalog1\",\"schema_test\")\n }catch(ex){\n }\n ");
            DatabaseMetaData metaData = conn.getMetaData();
            createSchema("catalog1","dfs://db","schema_test");
            DatabaseMetaData metaData1 = conn.getMetaData();
            String re = null;
            try{
                rs = metaData1.getSchemas("catalog1", "");
            }catch(Exception ex){
                re = ex.getMessage();
            }
            Assert.assertEquals("Illegal param in getSchemas",re);
        }else{
            createTable1("dfs://test_append_type_tsdb1");
            DatabaseMetaData metaData1 = conn.getMetaData();
            String re = null;
            try{
                rs = metaData1.getSchemas("DolphinDB","");
            }catch(Exception ex){
                re = ex.getMessage();
            }
            Assert.assertEquals("Illegal param in getSchemas",re);
        }
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getSchemas_schemaPattern_percent() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        DBConnection connDB = new DBConnection();
        connDB.connect(HOST,PORT,"admin","123456");
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;

        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
            connDB.run("login(`admin, `123456); \ntry{\n createCatalog(\"catalog1\");\n dropSchema(\"catalog1\",\"schema_test\")\n }catch(ex){\n }\n ");
            DatabaseMetaData metaData = conn.getMetaData();
            createSchema("catalog1","dfs://db","schema_test");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getSchemas("catalog1", "%");
            Assert.assertEquals(true,printData1(rs).contains("schema_test"));
        }else{
            createTable1("dfs://test_append_type_tsdb1");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getSchemas("DolphinDB","%");
            Assert.assertEquals(true,printData1(rs).contains("TABLE_SCHEM: test_append_type_tsdb1    TABLE_CATALOG: DolphinDB"));
        }
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getTables() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
        DatabaseMetaData metaData = conn.getMetaData();
        createSchema("catalog1","dfs://db","schema_test");
        rs = metaData.getTables("catalog1","%","%", null);
        String results1 = getTablesData(rs);
        Assert.assertEquals("TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: dt    TABLE_TYPE: TABLE    REMARKS: null    \n" +
                "TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: pt    TABLE_TYPE: TABLE    REMARKS: null    \n",results1);
        }else{
            createTable1("dfs://test_append_type_tsdb1");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getTables("DolphinDB","%","%", null);
            String results1 = getTablesData(rs);
            Assert.assertEquals(true,results1.contains("TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    TABLE_TYPE: TABLE    REMARKS: null    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    TABLE_TYPE: TABLE    REMARKS: null    \n"));
        }
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getTables_catalog_schemaPattern_special() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
        DatabaseMetaData metaData = conn.getMetaData();
        createSchema("catalog1","dfs://db","schema_test");
        createSchema("catalog2","dfs://db1","schema_test");
        DBConnection connDB = new DBConnection();
        connDB.connect(HOST,PORT,"admin","123456");
        connDB.run("login(`admin, `123456); \ntry{\n createSchema(\"catalog1\",\"dfs://db1\",\"schema_test1\")\n }catch(ex){\n }\n ");
        rs = metaData.getTables("catalog1","schema_test","%", null);
        String results1 = getTablesData(rs);
        Assert.assertEquals("TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: dt    TABLE_TYPE: TABLE    REMARKS: null    \n" +
                "TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: pt    TABLE_TYPE: TABLE    REMARKS: null    \n",results1);
        }else{
            createTable1("dfs://test_append_type_tsdb1");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getTables("DolphinDB","test_append_type_tsdb1","%", null);
            String results1 = getTablesData(rs);
            Assert.assertEquals("TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    TABLE_TYPE: TABLE    REMARKS: null    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    TABLE_TYPE: TABLE    REMARKS: null    \n",results1);
        }
        stmt.close();
        conn.close();
    }

    //@Test//https://dolphindb1.atlassian.net/browse/JAVAOS-1558 不支持这种入参 目前会带出库下面所有的表
    public void test_DatabaseMetaData_getTables_catalog_schemaPattern_tableNamePattern_special() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
            DatabaseMetaData metaData = conn.getMetaData();
            String results = null;
            createSchema("catalog1","dfs://db","schema_test");
            createSchema("catalog2","dfs://db1","schema_test");
            DBConnection connDB = new DBConnection();
            connDB.connect(HOST,PORT,"admin","123456");
            connDB.run("login(`admin, `123456); \ntry{\n createSchema(\"catalog1\",\"dfs://db1\",\"schema_test1\")\n }catch(ex){\n }\n ");
            rs = metaData.getTables("catalog1","schema_test","pt", null);
            String results1 = getTablesData(rs);
            Assert.assertEquals("TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: pt    TABLE_TYPE: TABLE    REMARKS: null    \n",results1);
        }else{
            createTable1("dfs://test_append_type_tsdb1");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getTables("DolphinDB","test_append_type_tsdb1","pt1", null);
            String results1 = getTablesData(rs);
            Assert.assertEquals("TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    TABLE_TYPE: TABLE    REMARKS: null    \n",results1);
        }
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getTables_memoryTable_catalog_schemaPattern_null() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
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
        connDB.run("share table(1..10 as id) as table1");
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
        rs = metaData.getTables(null,null,"%", null);
        String results1 = getTablesData(rs);
        //printData(rs);
        Assert.assertEquals(true,results1.contains("table1"));
        }else{
            rs = metaData.getTables(null,null,"%", null);
            String results1 = getTablesData(rs);
            //printData(rs);
            Assert.assertEquals(true,results1.contains("TABLE_CAT: null    TABLE_SCHEM: null    TABLE_NAME: table1    TABLE_TYPE: TABLE    REMARKS: null    "));
        }
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getTables_catalog_null() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        DatabaseMetaData metaData = conn.getMetaData();
        String results = null;
//        createSchema("catalog1","dfs://db","schema_test");
        String re = null;
        try{
            rs = metaData.getTables("","%","dt", null);
        }catch(Exception ex){
            re = ex.getMessage();
        }
            Assert.assertEquals("Invalid params in getTables.",re);
        stmt.close();
        conn.close();
    }

    @Test
    public void test_DatabaseMetaData_getTables_catalog_percent() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        DatabaseMetaData metaData = conn.getMetaData();
//        createSchema("catalog1","dfs://db","schema_test");
        ResultSet rs = null;
        String re = null;
        try{
            rs = metaData.getTables("%","%","dt", null);
        }catch(Exception ex){
            re = ex.getMessage();
        }
        Assert.assertEquals("Invalid params in getTables, not support get all tables with no specific catalog and schema.",re);
        stmt.close();
        conn.close();
    }

    @Test
    public void test_DatabaseMetaData_getTables_schemaPattern_percent() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
            DatabaseMetaData metaData = conn.getMetaData();
            createSchema("catalog1","dfs://db","schema_test");
            createSchema("catalog2","dfs://db1","schema_test");
            DBConnection connDB = new DBConnection();
            connDB.connect(HOST,PORT,"admin","123456");
            connDB.run("login(`admin, `123456); \ntry{\n createSchema(\"catalog1\",\"dfs://db1\",\"schema_test1\")\n }catch(ex){\n }\n ");
            rs = metaData.getTables("catalog1","%","pt", null);
            String results1 = getTablesData(rs);
            Assert.assertEquals("TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: dt    TABLE_TYPE: TABLE    REMARKS: null    \n" +
                    "TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: pt    TABLE_TYPE: TABLE    REMARKS: null    \n",results1);
        }else{
            createTable1("dfs://test_append_type_tsdb1");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getTables("DolphinDB","%","dt", null);
            String results1 = getTablesData(rs);
            Assert.assertEquals(true, results1.contains("TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    TABLE_TYPE: TABLE    REMARKS: null    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    TABLE_TYPE: TABLE    REMARKS: null    "));
        }
        stmt.close();
        conn.close();
    }

    @Test
    public void test_DatabaseMetaData_getTables_catalog_exist() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
        DatabaseMetaData metaData = conn.getMetaData();
        createSchema("catalog1","dfs://db","schema_test");
        createSchema("catalog2","dfs://db1","schema_test");
        rs = metaData.getTables("catalog2","%","dt", null);
        String results1 = getTablesData(rs);
        Assert.assertEquals("TABLE_CAT: catalog2    TABLE_SCHEM: schema_test    TABLE_NAME: dt    TABLE_TYPE: TABLE    REMARKS: null    \n" +
                "TABLE_CAT: catalog2    TABLE_SCHEM: schema_test    TABLE_NAME: pt    TABLE_TYPE: TABLE    REMARKS: null    \n",results1);
        }else{
            createTable1("dfs://test_append_type_tsdb1");
            createTable1("dfs://test_append_type_tsdb2");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getTables("DolphinDB","%","dt", null);
            String results1 = getTablesData(rs);
            Assert.assertEquals(true, results1.contains("TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    TABLE_TYPE: TABLE    REMARKS: null    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    TABLE_TYPE: TABLE    REMARKS: null    "));
            Assert.assertEquals(true, results1.contains("TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb2    TABLE_NAME: pt    TABLE_TYPE: TABLE    REMARKS: null    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb2    TABLE_NAME: pt1    TABLE_TYPE: TABLE    REMARKS: null    "));
        }
        stmt.close();
        conn.close();
    }


    @Test
    public void test_DatabaseMetaData_getTables_catalog_not_exist() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
            DatabaseMetaData metaData = conn.getMetaData();
            createSchema("catalog1","dfs://db","schema_test");
            String re = null;
            try{
                rs = metaData.getTables("catalog22","%","dt", null);
            }catch(Exception ex){
                re = ex.getMessage();
            }
            Assert.assertEquals(true,re.contains("Catalog [catalog22] doesn't exist."));
        }else{
            String re = null;
            DatabaseMetaData metaData = conn.getMetaData();
            try{
                rs = metaData.getTables("catalog22","%","dt", null);
            }catch(Exception ex){
                re = ex.getMessage();
            }
            Assert.assertEquals("Catalog must be \"DolphinDB\" for old version servers.",re);
        }
        stmt.close();
        conn.close();
    }

    @Test
    public void test_DatabaseMetaData_getTables_catalog_schemaPattern_null() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
            DatabaseMetaData metaData = conn.getMetaData();
            createSchema("catalog1","dfs://db","schema_test");
            createSchema("catalog2","dfs://db1","schema_test");
            rs = metaData.getTables("catalog2",null,"dt", null);
            String results1 = getTablesData(rs);
            Assert.assertEquals("TABLE_CAT: catalog2    TABLE_SCHEM: schema_test    TABLE_NAME: dt    TABLE_TYPE: TABLE    REMARKS: null    \n" +
                    "TABLE_CAT: catalog2    TABLE_SCHEM: schema_test    TABLE_NAME: pt    TABLE_TYPE: TABLE    REMARKS: null    \n",results1);
        }else{
            createTable1("dfs://test_append_type_tsdb1");
            createTable1("dfs://test_append_type_tsdb2");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getTables("DolphinDB",null,"dt", null);
            String results1 = getTablesData(rs);
            Assert.assertEquals(true, results1.contains("TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    TABLE_TYPE: TABLE    REMARKS: null    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    TABLE_TYPE: TABLE    REMARKS: null    "));
            Assert.assertEquals(true, results1.contains("TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb2    TABLE_NAME: pt    TABLE_TYPE: TABLE    REMARKS: null    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb2    TABLE_NAME: pt1    TABLE_TYPE: TABLE    REMARKS: null    "));
        }
        stmt.close();
        conn.close();
    }

    @Test
    public void test_DatabaseMetaData_getTables_catalog_schemaPattern_table_not_exist() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
            DatabaseMetaData metaData = conn.getMetaData();
            createSchema("catalog1","dfs://db","schema_test");
            stmt.execute("db=database(\"dfs://db\");\n" +
                    "dropTable(db,`pt);\n" +
                    "dropTable(db,`dt);");
            rs = metaData.getTables("catalog1","schema_test","%", null);
            String results1 = getTablesData(rs);
            //printData(rs);
            Assert.assertEquals("",results1);
        }else{
            createTable1("dfs://test_append_type_tsdb1");
            stmt.execute("db=database(\"dfs://test_append_type_tsdb1\");\n" +
                    "dropTable(db,`pt);\n" +
                    "dropTable(db,`pt1);");
            DatabaseMetaData metaData1 = conn.getMetaData();
            String re = null;
            try{
                rs = metaData1.getTables("DolphinDB","test_append_type_tsdb1","%", null);
            }catch(Exception ex){
                re = ex.getMessage();
            }
            Assert.assertEquals("The database test_append_type_tsdb1 does not exist or contains no tables.",re);
        }
        stmt.close();
        conn.close();
    }

    @Test
    public void test_DatabaseMetaData_getColumns() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
        DatabaseMetaData metaData = conn.getMetaData();
        createSchema("catalog1","dfs://db","schema_test");
        rs = metaData.getColumns("catalog1","schema_test","dt", "%");
        String results1 = getTablesData(rs);
        //printData(rs);
        Assert.assertEquals("TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: dt    COLUMN_NAME: ID    TYPE_NAME: INT    DATA_TYPE: 4    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 1    SQL_DATA_TYPES: 4    \n" +
                "TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: dt    COLUMN_NAME: x    TYPE_NAME: DOUBLE    DATA_TYPE: 8    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 2    SQL_DATA_TYPES: 8    \n" +
                "TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: dt    COLUMN_NAME: sys    TYPE_NAME: STRING    DATA_TYPE: 12    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 3    SQL_DATA_TYPES: 12    \n",results1);
        }else{
            createTable1("dfs://test_append_type_tsdb1");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getColumns("DolphinDB","test_append_type_tsdb1","pt1", "%");
            String results1 = getTablesData(rs);
            Assert.assertEquals("TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col1    TYPE_NAME: INT    DATA_TYPE: 4    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 1    SQL_DATA_TYPES: 4    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col2    TYPE_NAME: BOOL    DATA_TYPE: 16    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 2    SQL_DATA_TYPES: 16    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col3    TYPE_NAME: CHAR    DATA_TYPE: 1    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 3    SQL_DATA_TYPES: 1    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col4    TYPE_NAME: SHORT    DATA_TYPE: 5    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 4    SQL_DATA_TYPES: 5    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col5    TYPE_NAME: INT    DATA_TYPE: 4    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 5    SQL_DATA_TYPES: 4    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col6    TYPE_NAME: LONG    DATA_TYPE: -5    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 6    SQL_DATA_TYPES: -5    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col7    TYPE_NAME: DATE    DATA_TYPE: 91    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 7    SQL_DATA_TYPES: 91    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col8    TYPE_NAME: MONTH    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 8    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col9    TYPE_NAME: TIME    DATA_TYPE: 92    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 9    SQL_DATA_TYPES: 92    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col10    TYPE_NAME: MINUTE    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 10    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col11    TYPE_NAME: SECOND    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 11    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col12    TYPE_NAME: DATETIME    DATA_TYPE: 93    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 12    SQL_DATA_TYPES: 93    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col13    TYPE_NAME: TIMESTAMP    DATA_TYPE: 93    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 13    SQL_DATA_TYPES: 93    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col14    TYPE_NAME: NANOTIME    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 14    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col15    TYPE_NAME: NANOTIMESTAMP    DATA_TYPE: 93    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 15    SQL_DATA_TYPES: 93    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col16    TYPE_NAME: FLOAT    DATA_TYPE: 6    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 16    SQL_DATA_TYPES: 6    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col17    TYPE_NAME: DOUBLE    DATA_TYPE: 8    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 17    SQL_DATA_TYPES: 8    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col18    TYPE_NAME: SYMBOL    DATA_TYPE: 12    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 18    SQL_DATA_TYPES: 12    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col19    TYPE_NAME: STRING    DATA_TYPE: 12    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 19    SQL_DATA_TYPES: 12    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col20    TYPE_NAME: UUID    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 20    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col21    TYPE_NAME: DATEHOUR    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 21    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col22    TYPE_NAME: IPADDR    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 22    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col23    TYPE_NAME: INT128    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 23    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col24    TYPE_NAME: BLOB    DATA_TYPE: 2,005    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 24    SQL_DATA_TYPES: 2,005    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col25    TYPE_NAME: COMPLEX    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 25    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col26    TYPE_NAME: POINT    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 26    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col27    TYPE_NAME: DECIMAL32(2)    DATA_TYPE: 3    EXTRA: 2    REMARKS: null    DECIMAL_DIGITS: 2    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 27    SQL_DATA_TYPES: 3    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col28    TYPE_NAME: DECIMAL64(7)    DATA_TYPE: 3    EXTRA: 7    REMARKS: null    DECIMAL_DIGITS: 7    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 28    SQL_DATA_TYPES: 3    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col29    TYPE_NAME: DECIMAL128(19)    DATA_TYPE: 3    EXTRA: 19    REMARKS: null    DECIMAL_DIGITS: 19    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 29    SQL_DATA_TYPES: 3    \n",results1);
        }
        stmt.close();
        conn.close();
    }

    @Test
    public void test_DatabaseMetaData_getColumns_1() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
            DatabaseMetaData metaData = conn.getMetaData();
            createSchema("catalog1","dfs://db","schema_test");
            rs = metaData.getColumns("catalog1","schema_test","pt", "%");
            String results1 = getTablesData(rs);
            //printData(rs);
            Assert.assertEquals("TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: pt    COLUMN_NAME: ID    TYPE_NAME: INT    DATA_TYPE: 4    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: NO    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 1    SQL_DATA_TYPES: 4    \n" +
                    "TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: pt    COLUMN_NAME: x    TYPE_NAME: DOUBLE    DATA_TYPE: 8    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 2    SQL_DATA_TYPES: 8    \n",results1);
        }else{
            createTable1("dfs://test_append_type_tsdb1");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getColumns("DolphinDB","test_append_type_tsdb1","pt", "%");
            String results1 = getTablesData(rs);
            Assert.assertEquals("TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col1    TYPE_NAME: INT    DATA_TYPE: 4    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 1    SQL_DATA_TYPES: 4    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col2    TYPE_NAME: BOOL    DATA_TYPE: 16    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 2    SQL_DATA_TYPES: 16    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col3    TYPE_NAME: CHAR    DATA_TYPE: 1    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 3    SQL_DATA_TYPES: 1    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col4    TYPE_NAME: SHORT    DATA_TYPE: 5    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 4    SQL_DATA_TYPES: 5    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col5    TYPE_NAME: INT    DATA_TYPE: 4    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 5    SQL_DATA_TYPES: 4    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col6    TYPE_NAME: LONG    DATA_TYPE: -5    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 6    SQL_DATA_TYPES: -5    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col7    TYPE_NAME: DATE    DATA_TYPE: 91    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 7    SQL_DATA_TYPES: 91    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col8    TYPE_NAME: MONTH    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 8    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col9    TYPE_NAME: TIME    DATA_TYPE: 92    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 9    SQL_DATA_TYPES: 92    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col10    TYPE_NAME: MINUTE    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 10    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col11    TYPE_NAME: SECOND    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 11    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col12    TYPE_NAME: DATETIME    DATA_TYPE: 93    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 12    SQL_DATA_TYPES: 93    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col13    TYPE_NAME: TIMESTAMP    DATA_TYPE: 93    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 13    SQL_DATA_TYPES: 93    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col14    TYPE_NAME: NANOTIME    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 14    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col15    TYPE_NAME: NANOTIMESTAMP    DATA_TYPE: 93    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 15    SQL_DATA_TYPES: 93    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col16    TYPE_NAME: FLOAT    DATA_TYPE: 6    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 16    SQL_DATA_TYPES: 6    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col17    TYPE_NAME: DOUBLE    DATA_TYPE: 8    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 17    SQL_DATA_TYPES: 8    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col18    TYPE_NAME: SYMBOL    DATA_TYPE: 12    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 18    SQL_DATA_TYPES: 12    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col19    TYPE_NAME: STRING    DATA_TYPE: 12    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 19    SQL_DATA_TYPES: 12    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col20    TYPE_NAME: UUID    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 20    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col21    TYPE_NAME: DATEHOUR    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 21    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col22    TYPE_NAME: IPADDR    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 22    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col23    TYPE_NAME: INT128    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 23    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col24    TYPE_NAME: BLOB    DATA_TYPE: 2,005    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 24    SQL_DATA_TYPES: 2,005    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col25    TYPE_NAME: COMPLEX    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 25    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col26    TYPE_NAME: POINT    DATA_TYPE: 1,111    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 26    SQL_DATA_TYPES: 1,111    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col27    TYPE_NAME: DECIMAL32(2)    DATA_TYPE: 3    EXTRA: 2    REMARKS: null    DECIMAL_DIGITS: 2    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 27    SQL_DATA_TYPES: 3    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col28    TYPE_NAME: DECIMAL64(7)    DATA_TYPE: 3    EXTRA: 7    REMARKS: null    DECIMAL_DIGITS: 7    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 28    SQL_DATA_TYPES: 3    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col29    TYPE_NAME: DECIMAL128(19)    DATA_TYPE: 3    EXTRA: 19    REMARKS: null    DECIMAL_DIGITS: 19    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 29    SQL_DATA_TYPES: 3    \n",results1);
        }
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getColumns_memoryTable() throws Exception {
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
        connDB.run("share table(1..10 as id) as table1");
        rs = metaData.getColumns("","","table1", "%");
        String results1 = getTablesData(rs);
        Assert.assertEquals("TABLE_CAT: null    TABLE_SCHEM: null    TABLE_NAME: table1    COLUMN_NAME: id    TYPE_NAME: INT    DATA_TYPE: 4    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 1    SQL_DATA_TYPES: 4    \n",results1);
        stmt.close();
        conn.close();
    }

    @Test
    public void test_DatabaseMetaData_getColumns_memoryTable_catalogNamePattern_schemaNamePattern_null() throws Exception {
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
            connDB.run("share table(1..10 as id) as table1");
            rs = metaData.getColumns(null,null,"table1", "%");
            String results1 = getTablesData(rs);
            Assert.assertEquals("TABLE_CAT: null    TABLE_SCHEM: null    TABLE_NAME: table1    COLUMN_NAME: id    TYPE_NAME: INT    DATA_TYPE: 4    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 1    SQL_DATA_TYPES: 4    \n",results1);
            stmt.close();
            conn.close();
    }
    @Test //not support 当前返回空
    public void test_DatabaseMetaData_getColumns_columnNamePattern_null() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
        DatabaseMetaData metaData = conn.getMetaData();
        createSchema("catalog1","dfs://db","schema_test");
        rs = metaData.getColumns("catalog1","schema_test","dt", null);
        String results1 = getTablesData(rs);
        Assert.assertEquals("",results1);
        }else{
            createTable1("dfs://test_append_type_tsdb1");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getColumns("DolphinDB","test_append_type_tsdb1","pt1", null);
            String results1 = getTablesData(rs);
            Assert.assertEquals("",results1);
        }
        stmt.close();
        conn.close();
    }

    @Test//JAVAOS-1563
    public void test_DatabaseMetaData_getColumns_tableNamePattern_percent() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
            DatabaseMetaData metaData = conn.getMetaData();
            String results = null;
            createSchema_decimal("catalog1","dfs://db","schema_test");
            rs = metaData.getColumns("catalog1","schema_test","%", "%");
            String results1 = getTablesData(rs);
            //printData(rs);
            Assert.assertEquals("TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: dt    COLUMN_NAME: ID    TYPE_NAME: INT    DATA_TYPE: 4    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 1    SQL_DATA_TYPES: 4    \n" +
                    "TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: dt    COLUMN_NAME: cdecimal32    TYPE_NAME: DECIMAL32(8)    DATA_TYPE: 3    EXTRA: 8    REMARKS: null    DECIMAL_DIGITS: 8    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 2    SQL_DATA_TYPES: 3    \n" +
                    "TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: pt    COLUMN_NAME: ID    TYPE_NAME: INT    DATA_TYPE: 4    EXTRA: null    REMARKS: 股票代码    DECIMAL_DIGITS: -1    IS_NULLABLE: NO    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 1    SQL_DATA_TYPES: 4    \n" +
                    "TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: pt    COLUMN_NAME: cdecimal32    TYPE_NAME: DECIMAL32(8)    DATA_TYPE: 3    EXTRA: 8    REMARKS: decimal32类型    DECIMAL_DIGITS: 8    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 2    SQL_DATA_TYPES: 3    \n" +
                    "TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: pt    COLUMN_NAME: cdecimal64    TYPE_NAME: DECIMAL64(17)    DATA_TYPE: 3    EXTRA: 17    REMARKS: decimal64(17)类型    DECIMAL_DIGITS: 17    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 3    SQL_DATA_TYPES: 3    \n" +
                    "TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: pt    COLUMN_NAME: cdecimal128    TYPE_NAME: DECIMAL128(30)    DATA_TYPE: 3    EXTRA: 30    REMARKS: decimal128类型    DECIMAL_DIGITS: 30    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 4    SQL_DATA_TYPES: 3    \n",results1);
        }else{
            createTable("dfs://test_append_type_tsdb1");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getColumns("DolphinDB","test_append_type_tsdb1","%", "%");
            String results1 = getTablesData(rs);
            Assert.assertEquals("TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col1    TYPE_NAME: INT    DATA_TYPE: 4    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: NO    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 1    SQL_DATA_TYPES: 4    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col2    TYPE_NAME: DECIMAL128(19)    DATA_TYPE: 3    EXTRA: 19    REMARKS: null    DECIMAL_DIGITS: 19    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 2    SQL_DATA_TYPES: 3    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt    COLUMN_NAME: col3    TYPE_NAME: DECIMAL128(19)[]    DATA_TYPE: 2,003    EXTRA: 19    REMARKS: null    DECIMAL_DIGITS: 19    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 3    SQL_DATA_TYPES: 2,003    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col1    TYPE_NAME: INT    DATA_TYPE: 4    EXTRA: null    REMARKS: null    DECIMAL_DIGITS: -1    IS_NULLABLE: NO    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 1    SQL_DATA_TYPES: 4    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col2    TYPE_NAME: DECIMAL128(19)    DATA_TYPE: 3    EXTRA: 19    REMARKS: null    DECIMAL_DIGITS: 19    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 2    SQL_DATA_TYPES: 3    \n" +
                    "TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col3    TYPE_NAME: DECIMAL128(19)[]    DATA_TYPE: 2,003    EXTRA: 19    REMARKS: null    DECIMAL_DIGITS: 19    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 3    SQL_DATA_TYPES: 2,003    \n",results1);
        }
        stmt.close();
        conn.close();
    }

    //@Test//https://dolphindb1.atlassian.net/browse/JAVAOS-1564  不支持 目前会返回空
    public void test_DatabaseMetaData_getColumns_columnNamePattern_special() throws Exception {
        JDBCConnection jdbcConnection = new JDBCConnection(url,prop);
        Connection conn = null;
        Statement stmt = null;
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(url,LOGININFO);
        stmt = conn.createStatement();
        ResultSet rs = null;
        if(checkServerVersionIfSupportCatalog(jdbcConnection)){
            DatabaseMetaData metaData = conn.getMetaData();
            String results = null;
            createSchema_decimal("catalog1","dfs://db","schema_test");
            rs = metaData.getColumns("catalog1","schema_test","pt", "cdecimal128");
            String results1 = getTablesData(rs);
            //printData(rs);
            Assert.assertEquals(
                    "TABLE_CAT: catalog1    TABLE_SCHEM: schema_test    TABLE_NAME: pt    COLUMN_NAME: cdecimal128    TYPE_NAME: DECIMAL128(30)    DATA_TYPE: 3    EXTRA: 30    REMARKS: decimal128类型    DECIMAL_DIGITS: 30    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 4    SQL_DATA_TYPES: 3    \n",results1);
        }else{
            createTable("dfs://test_append_type_tsdb1");
            DatabaseMetaData metaData1 = conn.getMetaData();
            rs = metaData1.getColumns("DolphinDB","test_append_type_tsdb1","pt1", "col2");
            String results1 = getTablesData(rs);
            Assert.assertEquals("TABLE_CAT: DolphinDB    TABLE_SCHEM: test_append_type_tsdb1    TABLE_NAME: pt1    COLUMN_NAME: col2    TYPE_NAME: DECIMAL128(19)    DATA_TYPE: 3    EXTRA: 19    REMARKS: null    DECIMAL_DIGITS: 19    IS_NULLABLE: YES    IS_AUTOINCREMENT: null    ORDINAL_POSITION: 2    SQL_DATA_TYPES: 3    \n",results1);
        }
        stmt.close();
        conn.close();
    }

    @Test
    public void test_DatabaseMetaData_supportsSchema() throws Exception {
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
        Assert.assertEquals(true,metaData.supportsSchemasInDataManipulation());
        Assert.assertEquals(false,metaData.supportsSchemasInProcedureCalls());
        Assert.assertEquals(true,metaData.supportsSchemasInTableDefinitions());
        Assert.assertEquals(false,metaData.supportsSchemasInIndexDefinitions());
        Assert.assertEquals(false,metaData.supportsSchemasInPrivilegeDefinitions());
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_supportsCatalog() throws Exception {
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
        Assert.assertEquals(true,metaData.supportsCatalogsInDataManipulation());
        Assert.assertEquals(false,metaData.supportsCatalogsInProcedureCalls());
        Assert.assertEquals(true,metaData.supportsCatalogsInTableDefinitions());
        Assert.assertEquals(false,metaData.supportsCatalogsInIndexDefinitions());
        Assert.assertEquals(false,metaData.supportsCatalogsInPrivilegeDefinitions());
        stmt.close();
        conn.close();
    }
    @Test
    public void test_DatabaseMetaData_getIdentifierQuoteString() throws Exception {
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
        Assert.assertEquals(" ",metaData.getIdentifierQuoteString());
        stmt.close();
        conn.close();
    }

    @Test
    public void test_DatabaseMetaData_nullsAreSorted() throws Exception {
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
        Assert.assertFalse(metaData.nullsAreSortedHigh());
        Assert.assertTrue(metaData.nullsAreSortedLow());
        Assert.assertFalse(metaData.nullsAreSortedAtStart());
        Assert.assertFalse(metaData.nullsAreSortedAtEnd());
        stmt.close();
        conn.close();
    }

}
