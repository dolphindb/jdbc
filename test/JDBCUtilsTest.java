
import org.junit.Test;
import java.sql.SQLException;

import static com.dolphindb.jdbc.Utils.*;
import static org.junit.Assert.assertEquals;

public class JDBCUtilsTest {

    //@Before
    public void SetUp() throws SQLException {
    }

    @Test
    public void Test_checkServerVersionIfSupportCatalog() throws SQLException {
        String version1 ="1.30.23.1 2025.08.31 LINUX x86_64";
        String version2 ="2.00.10.1 2025.08.31 LINUX x86_64";
        String version3 ="3.00.1 2025.08.31 LINUX x86_64";
        String version4 ="4.00.0 2025.08.31 LINUX x86_64";
        checkServerVersionIfSupportCatalog(version1);
        assertEquals(false,checkServerVersionIfSupportCatalog(version1));
        assertEquals(false,checkServerVersionIfSupportCatalog(version2));
        assertEquals(true,checkServerVersionIfSupportCatalog(version3));
        assertEquals(true,checkServerVersionIfSupportCatalog(version4));
    }

    @Test
    public void Test_checkServerVersionIfSupportRunSql() throws SQLException {
        String version1 ="1.30.23.1 2025.08.31 LINUX x86_64";
        String version2 ="2.00.14.1 2025.08.31 LINUX x86_64";
        String version3 ="2.00.15.1 2025.08.31 LINUX x86_64";
        String version4 ="3.00.2 2025.08.31 LINUX x86_64";
        String version5 ="3.00.3.4 2025.08.31 LINUX x86_64";
        String version6 ="4.00.0 2025.08.31 LINUX x86_64";
        assertEquals(false,checkServerVersionIfSupportRunSql(version1));
        assertEquals(false,checkServerVersionIfSupportRunSql(version2));
        assertEquals(true,checkServerVersionIfSupportRunSql(version3));
        assertEquals(false,checkServerVersionIfSupportRunSql(version4));
        assertEquals(true,checkServerVersionIfSupportRunSql(version5));
        assertEquals(true,checkServerVersionIfSupportRunSql(version6));
    }

    @Test
    public void Test_checkServerVersionIfSupportRowCount() throws SQLException {
        String version1 ="1.30.23.1 2025.08.31 LINUX x86_64";
        String version2 ="2.00.17.1 2025.08.31 LINUX x86_64";
        String version3 ="2.00.18 2025.08.31 LINUX x86_64";
        String version4 ="3.00.4.3 2025.08.31 LINUX x86_64";
        String version5 ="3.00.5 2025.08.31 LINUX x86_64";
        String version6 ="4.00.0 2025.08.31 LINUX x86_64";
        checkServerVersionIfSupportCatalog(version1);
        assertEquals(false,checkServerVersionIfSupportRowCount(version1));
        assertEquals(false,checkServerVersionIfSupportRowCount(version2));
        assertEquals(true,checkServerVersionIfSupportRowCount(version3));
        assertEquals(false,checkServerVersionIfSupportRowCount(version4));
        assertEquals(true,checkServerVersionIfSupportRowCount(version5));
        assertEquals(true,checkServerVersionIfSupportRowCount(version6));
    }
}
