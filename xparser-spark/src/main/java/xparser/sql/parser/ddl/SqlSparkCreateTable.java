package xparser.sql.parser.ddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import xparser.sql.parser.ExtendedSqlNode;
import xparser.sql.parser.error.SqlValidateException;

import javax.annotation.Nonnull;
import java.util.List;

public class SqlSparkCreateTable extends SqlCreate implements ExtendedSqlNode {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);
    private final SqlIdentifier tableName;
    private final SqlNodeList columnList;



    public SqlSparkCreateTable(SqlOperator operator, SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier tableName, SqlNodeList columnList) {
        super(operator, pos, replace, ifNotExists);
        this.tableName = tableName;
        this.columnList = columnList;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    @Override
    public void validate() throws SqlValidateException {

    }
}
