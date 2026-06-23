import com.xxdb.DBConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Integration tests for PreparedStatement literal encoding and direct run path.
 */
public class JDBCPrepareStatementLiteralSafetyTest {
    private static final String HOST = System.getProperty("ddb.host", "192.168.0.118");
    private static final int PORT = Integer.parseInt(System.getProperty("ddb.port", "8850"));
    private static final String JDBC_DRIVER = "com.dolphindb.jdbc.Driver";

    private Connection conn;
    private Statement stm;

    @Before
    public void setUp() throws SQLException, ClassNotFoundException {
        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(
                "jdbc:dolphindb://" + HOST + ":" + PORT + "?user=admin&password=123456");
        stm = conn.createStatement();
    }

    @After
    public void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    private void createSessionTable() throws SQLException {
        stm.execute("t = table(1 2 3 as id, `alice`bob`carol as name, 'a'b'c as ch)");
    }

    private void assertSessionTableRows(int expectedRows) throws SQLException {
        ResultSet rs = stm.executeQuery("select count(*) as cnt from t");
        assertTrue(rs.next());
        assertEquals(expectedRows, rs.getInt("cnt"));
        assertFalse(rs.next());
    }

    @Test
    public void test_session_table_visible_to_prepared_statement() throws SQLException {
        createSessionTable();

        PreparedStatement ps = conn.prepareStatement("select * from t where id = ?");
        ps.setInt(1, 2);
        ResultSet rs = ps.executeQuery();

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("bob", rs.getString("name"));
        assertFalse(rs.next());
    }

    @Test
    public void test_select_referencing_session_variable_range() throws SQLException {
        createSessionTable();

        PreparedStatement ps = conn.prepareStatement("select * from t where id >= ?");
        ps.setInt(1, 2);
        ResultSet rs = ps.executeQuery();

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("id"));
        assertFalse(rs.next());
    }

    @Test
    public void test_select_string_param_keeps_type() throws SQLException {
        createSessionTable();

        PreparedStatement ps = conn.prepareStatement("select * from t where name = ?");
        ps.setString(1, "bob");
        ResultSet rs = ps.executeQuery();

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("bob", rs.getString("name"));
        assertFalse(rs.next());
    }

    @Test
    public void test_update_session_variable() throws SQLException {
        createSessionTable();

        PreparedStatement ps = conn.prepareStatement("update t set name = ? where id = ?");
        ps.setString(1, "BOB");
        ps.setInt(2, 2);
        assertEquals(1, ps.executeUpdate());

        ResultSet rs = stm.executeQuery("select name from t where id = 2");
        assertTrue(rs.next());
        assertEquals("BOB", rs.getString("name"));
    }

    @Test
    public void test_delete_session_variable() throws SQLException {
        createSessionTable();

        PreparedStatement ps = conn.prepareStatement("delete from t where id = ?");
        ps.setInt(1, 1);
        assertEquals(1, ps.executeUpdate());

        assertSessionTableRows(2);
    }

    @Test
    public void test_select_value_not_mutated_by_template_preprocess() throws SQLException {
        String[] payloads = {"length(", "nvl(", "replace(", "outer join"};
        for (String payload : payloads) {
            stm.execute("t = table(1 as id, string(0) as name)");
            PreparedStatement insert = conn.prepareStatement("insert into t values(?, ?)");
            insert.setInt(1, 1);
            insert.setString(2, payload);
            insert.executeUpdate();

            PreparedStatement ps = conn.prepareStatement("select name from t where name = ?");
            ps.setString(1, payload);
            ResultSet rs = ps.executeQuery();
            assertTrue("payload not found: " + payload, rs.next());
            assertEquals(payload, rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void test_select_escape_roundtrip_crlf_tab() throws SQLException {
        stm.execute("t = table(1 as id, string(0) as payload)");
        String value = "a\r\n\tb\\c\"d";

        PreparedStatement insert = conn.prepareStatement("insert into t values(?, ?)");
        insert.setInt(1, 1);
        insert.setString(2, value);
        insert.executeUpdate();

        PreparedStatement ps = conn.prepareStatement("select payload from t where id = ?");
        ps.setInt(1, 1);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(value, rs.getString("payload"));
    }

    @Test
    public void test_setString_nul_rejected() throws SQLException {
        createSessionTable();
        PreparedStatement ps = conn.prepareStatement("select * from t where name = ?");
        ps.setString(1, "a\0b");
        try {
            ps.executeQuery();
            fail("expected SQLException for NUL in parameter");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("NUL"));
        }
    }

    @Test
    public void test_setString_control_char_rejected() throws SQLException {
        createSessionTable();
        PreparedStatement ps = conn.prepareStatement("select * from t where name = ?");
        ps.setString(1, "a\u0001b");
        try {
            ps.executeQuery();
            fail("expected SQLException for control char in parameter");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Control character"));
        }
    }

    @Test
    public void test_char_literal_escape_and_match() throws SQLException {
        stm.execute("t = table('x' as ch)");
        stm.execute("insert into t values('\\')");

        PreparedStatement ps = conn.prepareStatement("select ch from t where ch = ?");
        ps.setObject(1, '\\');
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals("\\", rs.getString("ch"));
    }

    @Test
    public void test_select_injection_payload_does_not_expand_result() throws SQLException, IOException {
        String script = "login(\"admin\", \"123456\"); \n" +
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`id`username,[INT,STRING]) as users;\n" +
                "insert into users values(1,'admin');\n" +
                "insert into users values(2,'user1');\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT, "admin", "123456");
        db.run(script);

        String[] payloads = {
                "admin\" OR \"1\"=\"1",
                "x'; delete from users; --",
                "x\";//\nselect 1"
        };
        for (String payload : payloads) {
            PreparedStatement ps = conn.prepareStatement("select * from users where username = ?");
            ps.setString(1, payload);
            ResultSet rs = ps.executeQuery();
            assertFalse("payload should not match any row: " + payload, rs.next());
        }

        ResultSet count = stm.executeQuery("select count(*) as cnt from users");
        assertTrue(count.next());
        assertEquals(2, count.getInt("cnt"));
    }

    @Test
    public void test_injection_value_not_executed_against_session_variable() throws SQLException {
        createSessionTable();

        PreparedStatement ps = conn.prepareStatement("select * from t where name = ?");
        ps.setString(1, "bob\"; t = table(1 as id); //");
        ResultSet rs = ps.executeQuery();
        assertFalse(rs.next());

        assertSessionTableRows(3);
    }

    @Test
    public void test_select_injection_payload_does_not_turn_where_clause_into_true() throws SQLException {
        createSessionTable();

        PreparedStatement ps = conn.prepareStatement("select * from t where name = ?");
        ps.setString(1, "bob' or 1=1 //");
        ResultSet rs = ps.executeQuery();

        assertFalse(rs.next());
        assertSessionTableRows(3);
    }

    @Test
    public void test_update_injection_payload_only_updates_literal_value() throws SQLException, IOException {
        String script = "login(\"admin\", \"123456\"); \n" +
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`id`username,[INT,STRING]) as users;\n" +
                "insert into users values(1,'admin');\n" +
                "insert into users values(2,'user1');\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT, "admin", "123456");
        db.run(script);

        String payload = "admin\"; DROP TABLE users; //";
        PreparedStatement ps = conn.prepareStatement("update users set username = ?");
        ps.setString(1, payload);
        ps.executeUpdate();

        ResultSet rs = stm.executeQuery("select * from users");
        assertTrue(rs.next());
        assertEquals(payload, rs.getString("username"));
        assertTrue(rs.next());
        assertEquals(payload, rs.getString("username"));

        ResultSet count = stm.executeQuery("select count(*) as cnt from users");
        assertTrue(count.next());
        assertEquals(2, count.getInt("cnt"));
    }

    @Test
    public void test_update_injection_payload_is_stored_as_literal_text() throws SQLException {
        createSessionTable();

        String payload = "bob'; delete from t; //";
        PreparedStatement ps = conn.prepareStatement("update t set name = ? where id = ?");
        ps.setString(1, payload);
        ps.setInt(2, 2);

        assertEquals(1, ps.executeUpdate());

        ResultSet rs = stm.executeQuery("select id, name from t order by id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("alice", rs.getString("name"));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals(payload, rs.getString("name"));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("id"));
        assertEquals("carol", rs.getString("name"));
        assertFalse(rs.next());
    }

    @Test
    public void test_delete_injection_payload_does_not_delete_all_rows() throws SQLException {
        createSessionTable();

        PreparedStatement ps = conn.prepareStatement("delete from t where name = ?");
        ps.setString(1, "alice' or 1=1 //");

        assertEquals(0, ps.executeUpdate());
        assertSessionTableRows(3);
    }

    @Test
    public void test_batch_insert_injection_payload_does_not_execute_extra_statement() throws SQLException {
        stm.execute("t = table(10:0, `id`name, [INT, STRING])");

        PreparedStatement ps = conn.prepareStatement("insert into t values(?, ?)");
        ps.setInt(1, 1);
        ps.setString(2, "alice'); drop table t; //");
        ps.addBatch();
        ps.setInt(1, 2);
        ps.setString(2, "bob'); delete from t; //");
        ps.addBatch();

        int[] result = ps.executeBatch();
        assertEquals(2, result.length);

        ResultSet rs = stm.executeQuery("select id, name from t order by id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("alice'); drop table t; //", rs.getString("name"));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("bob'); delete from t; //", rs.getString("name"));
        assertFalse(rs.next());
    }

    @Test
    public void test_batch_update_injection_payload() throws SQLException, IOException {
        String script = "login(\"admin\", \"123456\"); \n" +
                "try{undef(`users,SHARED)}catch(ex){};\n" +
                "share table(10:0,`id`username,[INT,STRING]) as users;\n" +
                "insert into users values(1,'admin');\n" +
                "insert into users values(2,'user1');\n";
        DBConnection db = new DBConnection();
        db.connect(HOST, PORT, "admin", "123456");
        db.run(script);

        PreparedStatement ps = conn.prepareStatement("update users set username = ? where id = ?");
        ps.setString(1, "safe\";//");
        ps.setInt(2, 1);
        ps.addBatch();
        ps.setString(1, "also\"safe");
        ps.setInt(2, 2);
        ps.addBatch();
        ps.executeBatch();

        ResultSet rs = stm.executeQuery("select * from users order by id");
        assertTrue(rs.next());
        assertEquals("safe\";//", rs.getString("username"));
        assertTrue(rs.next());
        assertEquals("also\"safe", rs.getString("username"));
    }

    @Test
    public void test_batch_update_injection_payload_does_not_escape_where_binding() throws SQLException {
        createSessionTable();

        PreparedStatement ps = conn.prepareStatement("update t set name = ? where id = ?");
        ps.setString(1, "alice'; drop table t; //");
        ps.setInt(2, 1);
        ps.addBatch();
        ps.setString(1, "carol' or id > 0 //");
        ps.setInt(2, 3);
        ps.addBatch();

        int[] result = ps.executeBatch();
        assertEquals(2, result.length);

        ResultSet rs = stm.executeQuery("select id, name from t order by id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("alice'; drop table t; //", rs.getString("name"));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("bob", rs.getString("name"));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("id"));
        assertEquals("carol' or id > 0 //", rs.getString("name"));
        assertFalse(rs.next());
    }

    @Test
    public void test_execute_update_rejects_exec_statement() throws SQLException {
        PreparedStatement ps = conn.prepareStatement("exec count(*) from table(1 2 3 as id) where id >= ?");
        ps.setInt(1, 2);

        try {
            ps.executeUpdate();
            fail("executeUpdate should reject EXEC statements");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Can not issue SELECT or EXEC via executeUpdate()."));
        }
    }

    @Test
    public void test_execute_batch_rejects_select_statement() throws SQLException {
        createSessionTable();

        PreparedStatement ps = conn.prepareStatement("select * from t where id >= ?");
        ps.setInt(1, 2);
        ps.addBatch();

        try {
            ps.executeBatch();
            fail("executeBatch should reject SELECT statements");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Can not issue SELECT or EXEC via executeUpdate()."));
        }
    }

    @Test
    public void test_setBlob_binary_rejects_on_select() throws SQLException {
        createSessionTable();
        byte[] blobBytes = new byte[] {0x00, 0x01, 0x02};
        Blob blob = new javax.sql.rowset.serial.SerialBlob(blobBytes);

        PreparedStatement ps = conn.prepareStatement("select * from t where name = ?");
        ps.setBlob(1, blob);
        try {
            ps.executeQuery();
            fail("expected SQLException for blob with control bytes in literal path");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("setBlob is not supported for non-INSERT prepared statements"));
        }
    }

    @Test
    public void test_mysql_backtick_select_with_params() throws SQLException {
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?sqlStd=MySQL&user=admin&password=123456";
        try (Connection mysqlConn = DriverManager.getConnection(url);
             Statement mysqlStmt = mysqlConn.createStatement()) {
            mysqlStmt.execute("t = table(1 2 3 as id)");
            PreparedStatement ps = mysqlConn.prepareStatement("select `t`.`id` from `t` where `t`.`id` > ?");
            ps.setInt(1, 1);
            ResultSet rs = ps.executeQuery();
            int count = 0;
            while (rs.next()) {
                assertTrue(rs.getInt(1) > 1);
                count++;
            }
            assertEquals(2, count);
        }
    }

    @Test
    public void test_mysql_backtick_update_with_now_function_and_where_params() throws SQLException {
        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?sqlStd=MySQL&user=admin&password=123456";
        try (Connection mysqlConn = DriverManager.getConnection(url);
             Statement mysqlStmt = mysqlConn.createStatement()) {
            mysqlStmt.execute("oms_unit_init_data = table(2026.06.01 as DATA_TIME, `195343NL633837195266 as tag_phy, " +
                    "0.0 as init_power, 0 as init_status, timestamp(2000.01.01T00:00:00.000) as init_offtime, " +
                    "timestamp(2000.01.01T00:00:00.000) as init_ontime, `old as market_id, " +
                    "timestamp(2000.01.01T00:00:00.000) as update_time, `old as remark_note)");

            PreparedStatement ps = mysqlConn.prepareStatement(
                    "UPDATE `oms_unit_init_data` " +
                            "SET `init_power` = ?, " +
                            "`init_status` = ?, " +
                            "`init_offtime` = ?, " +
                            "`init_ontime` = ?, " +
                            "`market_id` = ?, " +
                            "`update_time` = now(), " +
                            "`remark_note` = ? " +
                            "WHERE `DATA_TIME` = ? " +
                            "AND `tag_phy` = ?");
            ps.setDouble(1, 3000.0);
            ps.setInt(2, 1);
            ps.setTimestamp(3, Timestamp.valueOf("1962-11-25 17:31:44"));
            ps.setTimestamp(4, Timestamp.valueOf("2000-01-01 00:00:00"));
            ps.setString(5, "95511");
            ps.setString(6, "fenhu-hunan dual line");
            ps.setDate(7, java.sql.Date.valueOf("2026-06-01"));
            ps.setString(8, "195343NL633837195266");

            assertEquals(1, ps.executeUpdate());

            ResultSet rs = mysqlStmt.executeQuery("select init_power, init_status, market_id, remark_note from oms_unit_init_data where tag_phy = `195343NL633837195266");
            assertTrue(rs.next());
            assertEquals(3000.0, rs.getDouble("init_power"), 0.001);
            assertEquals(1, rs.getInt("init_status"));
            assertEquals("95511", rs.getString("market_id"));
            assertEquals("fenhu-hunan dual line", rs.getString("remark_note"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void test_oracle_compat_prepared_select_template_preprocess() throws SQLException {
        stm.execute("t = table(1 as id, `abc as name)");
        stm.execute("insert into t values(1, `abc)");

        PreparedStatement ps = conn.prepareStatement("select id, nvl(name, `missing) as nm, length(name) as len from t where id = ?");
        ps.setInt(1, 1);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("abc", rs.getString("nm"));
        assertEquals(3, rs.getInt("len"));
    }

    @Test
    public void test_update_with_date_params_does_not_use_run_sql_long_const_path() throws SQLException {
        stm.execute("sta_pa_reg = table(`330000PA00000019 as tag_phy, `old as caption, `old as sub_caption, " +
                "`old as gen_group_id, 2000.01.01 as effective_date, 2000.01.02 as expiration_date)");

        PreparedStatement ps = conn.prepareStatement(
                "UPDATE sta_pa_reg SET caption=?, sub_caption=?, gen_group_id=?, effective_date=?, expiration_date=? WHERE tag_phy=?");
        ps.setString(1, "zhejiang power plant agent");
        ps.setString(2, "zhejiang power plant agent");
        ps.setString(3, "D00000GR00000001");
        ps.setDate(4, java.sql.Date.valueOf("2026-06-01"));
        ps.setDate(5, java.sql.Date.valueOf("2026-06-02"));
        ps.setString(6, "330000PA00000019");

        assertEquals(1, ps.executeUpdate());

        ResultSet rs = stm.executeQuery("select caption, sub_caption, gen_group_id, effective_date, expiration_date from sta_pa_reg where tag_phy = `330000PA00000019");
        assertTrue(rs.next());
        assertEquals("zhejiang power plant agent", rs.getString("caption"));
        assertEquals("zhejiang power plant agent", rs.getString("sub_caption"));
        assertEquals("D00000GR00000001", rs.getString("gen_group_id"));
        assertEquals(java.sql.Date.valueOf("2026-06-01"), rs.getDate("effective_date"));
        assertEquals(java.sql.Date.valueOf("2026-06-02"), rs.getDate("expiration_date"));
        assertFalse(rs.next());
    }

    @Test
    public void test_dfs_backtick_wrapped_string_table_name_is_rejected_by_server() throws SQLException {
        try {
            stm.execute(
                    "dbName = \"dfs://testDB\"\n" +
                            "tableName = \"`user`\"\n" +
                            "if(existsDatabase(dbName))\n" +
                            "    dropDatabase(dbName)\n" +
                            "db = database(dbName, VALUE, 1..3)\n" +
                            "schemaTable = table(\n" +
                            "    array(INT, 0) as `id,\n" +
                            "    array(STRING, 0) as `name\n" +
                            ")\n" +
                            "db.createPartitionedTable(schemaTable, tableName, `id)");
            fail("DolphinDB should reject a tableName string that includes backticks");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Table name invalid: `user`"));
        }
    }

    @Test
    public void test_dfs_user_table_can_be_created_and_used_by_prepared_statement() throws SQLException {
        stm.execute(
                "dbName = \"dfs://testDB\"\n" +
                        "tableName = \"user\"\n" +
                        "if(existsDatabase(dbName))\n" +
                        "    dropDatabase(dbName)\n" +
                        "db = database(dbName, VALUE, 1..3)\n" +
                        "schemaTable = table(\n" +
                        "    array(INT, 0) as `id,\n" +
                        "    array(STRING, 0) as `name\n" +
                        ")\n" +
                        "db.createPartitionedTable(schemaTable, tableName, `id)");

        PreparedStatement insert = conn.prepareStatement("insert into loadTable(\"dfs://testDB\", `user) values (?, ?)");
        insert.setInt(1, 1);
        insert.setString(2, "alpha");
        assertEquals(1, insert.executeUpdate());

        PreparedStatement query = conn.prepareStatement("select * from loadTable(\"dfs://testDB\", `user) where id = ?");
        query.setInt(1, 1);
        ResultSet rs = query.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("alpha", rs.getString("name"));
        assertFalse(rs.next());
    }

    @Test
    public void test_full_database_path_user_table_insert_select_update_delete_flow() throws SQLException {
        stm.execute(
                "dbName = \"dfs://testDB\"\n" +
                        "tableName = \"user\"\n" +
                        "if(existsDatabase(dbName))\n" +
                        "    dropDatabase(dbName)\n" +
                        "db = database(dbName, VALUE, 1..20)\n" +
                        "schemaTable = table(\n" +
                        "    array(INT, 0) as `id,\n" +
                        "    array(STRING, 0) as `name\n" +
                        ")\n" +
                        "db.createPartitionedTable(schemaTable, tableName, `id)");

        String url = "jdbc:dolphindb://" + HOST + ":" + PORT + "?databasePath=dfs://testDB&user=admin&password=123456";
        try (Connection pathConn = DriverManager.getConnection(url)) {
            PreparedStatement insert = pathConn.prepareStatement("insert into `user values(?, ?)");
            int[] ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
            String[] names = {"Alice", "Bob", "Charlie", "David", "Eve",
                    "Frank", "Grace", "Henry", "Ivy", "Jack"};
            for (int i = 0; i < ids.length; i++) {
                insert.setInt(1, ids[i]);
                insert.setString(2, names[i]);
                insert.addBatch();
                if ((i + 1) % 5 == 0 || i == ids.length - 1) {
                    int[] results = insert.executeBatch();
                    assertEquals(5, results.length);
                }
            }

            PreparedStatement selectById = pathConn.prepareStatement("select * from `user where `id = ?");
            selectById.setInt(1, 3);
            ResultSet rs = selectById.executeQuery();
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals("Charlie", rs.getString("name"));
            assertFalse(rs.next());

            PreparedStatement selectLike = pathConn.prepareStatement("select * from `user where `name like ?");
            selectLike.setString(1, "A%");
            rs = selectLike.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            assertFalse(rs.next());

            PreparedStatement selectRange = pathConn.prepareStatement("select * from `user where `id >= ? and `id <= ?");
            selectRange.setInt(1, 2);
            selectRange.setInt(2, 7);
            rs = selectRange.executeQuery();
            int rangeRows = 0;
            while (rs.next()) {
                rangeRows++;
            }
            assertEquals(6, rangeRows);

            PreparedStatement updateOne = pathConn.prepareStatement("update `user set `name = ? where `id = ?");
            updateOne.setString(1, "Charlie Updated");
            updateOne.setInt(2, 3);
            assertEquals(1, updateOne.executeUpdate());

            PreparedStatement verifyUpdate = pathConn.prepareStatement("select * from `user where `id = ?");
            verifyUpdate.setInt(1, 3);
            rs = verifyUpdate.executeQuery();
            assertTrue(rs.next());
            assertEquals("Charlie Updated", rs.getString("name"));
            assertFalse(rs.next());

            PreparedStatement updateBatch = pathConn.prepareStatement("update `user set `name = ? where `id = ?");
            Map<Integer, String> updates = new LinkedHashMap<>();
            updates.put(4, "David Updated");
            updates.put(5, "Eve Updated");
            updates.put(6, "Frank Updated");
            for (Map.Entry<Integer, String> entry : updates.entrySet()) {
                updateBatch.setString(1, entry.getValue());
                updateBatch.setInt(2, entry.getKey());
                updateBatch.addBatch();
            }
            int[] updateResults = updateBatch.executeBatch();
            assertEquals(3, updateResults.length);

            PreparedStatement updateRange = pathConn.prepareStatement("update `user set `name = `name + ? where `id >= ?");
            updateRange.setString(1, " - Modified");
            updateRange.setInt(2, 7);
            assertEquals(4, updateRange.executeUpdate());

            PreparedStatement deleteById = pathConn.prepareStatement("delete from `user where `id = ?");
            deleteById.setInt(1, 10);
            assertEquals(1, deleteById.executeUpdate());

            PreparedStatement deleteByName = pathConn.prepareStatement("delete from `user where `name = ?");
            deleteByName.setString(1, "Jack");
            assertEquals(0, deleteByName.executeUpdate());

            PreparedStatement deleteBatch = pathConn.prepareStatement("delete from `user where `id = ?");
            for (int id : new int[]{1, 2}) {
                deleteBatch.setInt(1, id);
                deleteBatch.addBatch();
            }
            int[] deleteResults = deleteBatch.executeBatch();
            assertEquals(2, deleteResults.length);

            PreparedStatement deleteRange = pathConn.prepareStatement("delete from `user where `id > ?");
            deleteRange.setInt(1, 8);
            assertEquals(1, deleteRange.executeUpdate());

            PreparedStatement selectAll = pathConn.prepareStatement("select * from `user order by `id");
            rs = selectAll.executeQuery();
            int remaining = 0;
            while (rs.next()) {
                remaining++;
            }
            assertEquals(6, remaining);
        }
    }
}
