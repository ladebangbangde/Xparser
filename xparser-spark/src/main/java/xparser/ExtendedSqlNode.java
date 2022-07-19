package xparser.sql.parser;

import xparser.sql.parser.error.SqlValidateException;

/**
 * An remark interface which should be inherited by extended sql nodes which are not supported by
 * Calcite core parser.
 *
 * <p>We need this to customize our validation rules combined with the rules defined in {@link
 * org.apache.calcite.sql.validate.SqlValidatorImpl}.
 */
public interface ExtendedSqlNode {
    void validate() throws SqlValidateException;
}