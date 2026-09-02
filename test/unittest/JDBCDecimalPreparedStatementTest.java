import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCDecimalPreparedStatementTest {
    private Connection conn;
    private Statement stm;

    @Before
    public void setUp() throws SQLException {
        JDBCTestUtil.LOGININFO.put("user", "admin");
        JDBCTestUtil.LOGININFO.put("password", "123456");
        conn = JDBCTestUtil.getConnection(JDBCTestUtil.LOGININFO);
        stm = conn.createStatement();
    }

    @After
    public void tearDown() throws SQLException {
        if (stm != null) {
            stm.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    public void insertDecimal128WithSetBigDecimal() throws SQLException {
        createMemoryTable("DECIMAL128(6)");
        PreparedStatement ps = conn.prepareStatement("insert into decimal_prepare_test values(?,?)");
        ps.setInt(1, 1);
        ps.setBigDecimal(2, new BigDecimal("1000"));
        ps.executeUpdate();

        ResultSet rs = stm.executeQuery("select dataType from decimal_prepare_test");
        rs.next();
        Assert.assertEquals("1000.000000", rs.getObject("dataType").toString());
        ps.close();
        rs.close();
    }

    @Test
    public void insertDecimal128WithSetObjectBigDecimal() throws SQLException {
        createMemoryTable("DECIMAL128(6)");
        PreparedStatement ps = conn.prepareStatement("insert into decimal_prepare_test values(?,?)");
        ps.setInt(1, 1);
        ps.setObject(2, new BigDecimal("2"));
        ps.executeUpdate();

        ResultSet rs = stm.executeQuery("select dataType from decimal_prepare_test");
        rs.next();
        Assert.assertEquals("2.000000", rs.getObject("dataType").toString());
        ps.close();
        rs.close();
    }

    @Test
    public void insertDecimal128WithSetLong() throws SQLException {
        createMemoryTable("DECIMAL128(6)");
        PreparedStatement ps = conn.prepareStatement("insert into decimal_prepare_test values(?,?)");
        ps.setInt(1, 1);
        ps.setLong(2, 1000L);
        ps.executeUpdate();

        ResultSet rs = stm.executeQuery("select dataType from decimal_prepare_test");
        rs.next();
        Assert.assertEquals("1000.000000", rs.getObject("dataType").toString());
        ps.close();
        rs.close();
    }

    @Test
    public void insertDecimal128ScaleZeroWithSetLong() throws SQLException {
        createMemoryTable("DECIMAL128(0)");
        PreparedStatement ps = conn.prepareStatement("insert into decimal_prepare_test values(?,?)");
        ps.setInt(1, 1);
        ps.setLong(2, 123L);
        ps.executeUpdate();

        ResultSet rs = stm.executeQuery("select dataType from decimal_prepare_test");
        rs.next();
        Assert.assertEquals("123", rs.getObject("dataType").toString());
        ps.close();
        rs.close();
    }

    @Test
    public void batchInsertDecimal128WithLongAndBigDecimal() throws SQLException {
        createMemoryTable("DECIMAL128(6)");
        PreparedStatement ps = conn.prepareStatement("insert into decimal_prepare_test values(?,?)");
        ps.setInt(1, 1);
        ps.setLong(2, 1000L);
        ps.addBatch();
        ps.setInt(1, 2);
        ps.setBigDecimal(2, new BigDecimal("2"));
        ps.addBatch();
        ps.executeBatch();

        ResultSet rs = stm.executeQuery("select dataType from decimal_prepare_test order by id");
        rs.next();
        Assert.assertEquals("1000.000000", rs.getObject("dataType").toString());
        rs.next();
        Assert.assertEquals("2.000000", rs.getObject("dataType").toString());
        Assert.assertFalse(rs.next());
        ps.close();
        rs.close();
    }

    private void createMemoryTable(String decimalType) throws SQLException {
        stm.execute("decimal_prepare_test = table(10:0, `id`dataType, [INT," + decimalType + "])");
    }
}
