import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserTest;
import xparser.impl.XSqlParserImpl;
public class XSqlParserImplTest extends SqlParserTest {
    public static void main(String[] args) {

    }
    @Override
    protected SqlParserImplFactory parserImplFactory() {
        return XSqlParserImpl.FACTORY;
    }

}
