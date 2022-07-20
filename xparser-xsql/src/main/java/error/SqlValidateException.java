package error;

import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * SQL parse Exception. This exception mainly throws during {@link
 * extended.ExtendedSqlNode} validation.
 */
public class SqlValidateException extends Exception {

    private SqlParserPos errorPosition;

    private String message;

    public SqlValidateException(SqlParserPos errorPosition, String message) {
        this.errorPosition = errorPosition;
        this.message = message;
    }

    public SqlValidateException(SqlParserPos errorPosition, String message, Exception e) {
        super(e);
        this.errorPosition = errorPosition;
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public SqlParserPos getErrorPosition() {
        return errorPosition;
    }

    public void setErrorPosition(SqlParserPos errorPosition) {
        this.errorPosition = errorPosition;
    }
}

