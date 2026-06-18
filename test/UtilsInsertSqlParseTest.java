import com.dolphindb.jdbc.Utils;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class UtilsInsertSqlParseTest {
    @Test
    public void testGetTableNameSupportsBacktickQuotedInsertIdentifiers() throws SQLException {
        String sql = "insert into `inser_tradeseq_info` (`TRADESEQ_ID`) values (?)";

        assertEquals("inser_tradeseq_info", Utils.getTableName(sql, true));
    }

    @Test
    public void testGetInsertColumnParamInSqlStripsBacktickQuotes() {
        String sql = "insert into `inser_tradeseq_info` (`TRADESEQ_ID`) values (?)";

        Map<String, Integer> columns = Utils.getInsertColumnParamInSql(sql);

        assertEquals(Integer.valueOf(0), columns.get("tradeseq_id"));
    }

    @Test
    public void testCheckInsertSqlValidSupportsBacktickQuotedColumnList() {
        String sql = "insert into `inser_tradeseq_info` (`TRADESEQ_ID`) values (?)";

        Utils.checkInsertSQLValid(sql, 1);
    }

    @Test
    public void testExistingInsertParsingStillSupportsPlainAndLoadTableNames() throws SQLException {
        assertEquals("t", Utils.getTableName("insert into t (id) values (?)", true));
        assertEquals(
                "loadTable('dfs://db','pt')",
                Utils.getTableName("insert into loadTable('dfs://db','pt') (\"id\") values (?)", true)
        );
    }

    @Test
    public void testExistingInsertColumnParsingStillSupportsSingleAndDoubleQuotes() {
        Map<String, Integer> doubleQuotedColumns =
                Utils.getInsertColumnParamInSql("insert into t (\"id\") values (?)");
        Map<String, Integer> singleQuotedColumns =
                Utils.getInsertColumnParamInSql("insert into t ('id') values (?)");

        assertEquals(Integer.valueOf(0), doubleQuotedColumns.get("id"));
        assertEquals(Integer.valueOf(0), singleQuotedColumns.get("id"));
    }
}
