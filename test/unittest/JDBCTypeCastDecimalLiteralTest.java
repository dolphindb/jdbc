import com.dolphindb.jdbc.TypeCast;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;

public class JDBCTypeCastDecimalLiteralTest {

    @Test
    public void castDbStringUsesPlainStringForBigDecimal() throws SQLException {
        Assert.assertEquals(
                "12345678901234567890.123456",
                TypeCast.castDbString(new BigDecimal("12345678901234567890.123456")));
    }
}
