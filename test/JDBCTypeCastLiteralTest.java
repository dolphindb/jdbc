import com.dolphindb.jdbc.TypeCast;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JDBCTypeCastLiteralTest {

    @Test
    public void test_quoteDolphinStringLiteral_escapes_special_chars() throws SQLException {
        assertEquals("\"hello\"", TypeCast.quoteDolphinStringLiteral("hello"));
        assertEquals("\"a\\\"b\"", TypeCast.quoteDolphinStringLiteral("a\"b"));
        assertEquals("\"a\\\\b\"", TypeCast.quoteDolphinStringLiteral("a\\b"));
        assertEquals("\"a\\nb\"", TypeCast.quoteDolphinStringLiteral("a\nb"));
        assertEquals("\"a\\rb\"", TypeCast.quoteDolphinStringLiteral("a\rb"));
        assertEquals("\"a\\tb\"", TypeCast.quoteDolphinStringLiteral("a\tb"));
    }

    @Test
    public void test_quoteDolphinStringLiteral_rejects_nul() {
        try {
            TypeCast.quoteDolphinStringLiteral("a\0b");
            fail("expected SQLException for NUL");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("NUL"));
        }
    }

    @Test
    public void test_quoteDolphinStringLiteral_rejects_control_char() {
        try {
            TypeCast.quoteDolphinStringLiteral("a\u0001b");
            fail("expected SQLException for control char");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Control character"));
        }
    }

    @Test
    public void test_quoteDolphinCharLiteral_escapes_special_chars() throws SQLException {
        assertEquals("'a'", TypeCast.quoteDolphinCharLiteral('a'));
        assertEquals("'\\''", TypeCast.quoteDolphinCharLiteral('\''));
        assertEquals("'\\\\'", TypeCast.quoteDolphinCharLiteral('\\'));
        assertEquals("'\\n'", TypeCast.quoteDolphinCharLiteral('\n'));
    }

    @Test
    public void test_castDbString_string_with_injection_chars() throws SQLException {
        String payload = "admin\"; DROP TABLE users; //";
        assertEquals("\"" + payload.replace("\"", "\\\"") + "\"", TypeCast.castDbString(payload));
    }

    @Test
    public void test_castDbString_rejects_unsupported_type() {
        try {
            TypeCast.castDbString(new Thread());
            fail("expected SQLException for unsupported parameter type");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Unsupported type for parameter"));
        }
    }
}
