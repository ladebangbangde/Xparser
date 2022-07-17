package com.ldbbd.xparser.parsers;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.SourceStringReader;

import java.io.Reader;

public class CalciteParser {
    private final SqlParser.Config config;

    public CalciteParser(SqlParser.Config config) {
        this.config = config;
    }

//    /**
//     * Parses a SQL statement into a {@link SqlNode}. The {@link SqlNode} is not yet validated.
//     *
//     * @param sql a sql string to parse
//     * @return a parsed sql node
//     * @throws SqlParserException if an exception is thrown when parsing the statement
//     */
//    public SqlNode parse(String sql) {
//        try {
//            SqlParser parser = SqlParser.create(sql, config);
//            return parser.parseStmt();
//        } catch (SqlParseException e) {
//            throw new SqlParserException("SQL parse failed. " + e.getMessage(), e);
//        }
//    }
//
//    /**
//     * Parses a SQL expression into a {@link SqlNode}. The {@link SqlNode} is not yet validated.
//     *
//     * @param sqlExpression a SQL expression string to parse
//     * @return a parsed SQL node
//     * @throws SqlParserException if an exception is thrown when parsing the statement
//     */
//    public SqlNode parseExpression(String sqlExpression) {
//        try {
//            final SqlParser parser = SqlParser.create(sqlExpression, config);
//            return parser.parseExpression();
//        } catch (SqlParseException e) {
//            throw new SqlParserException("SQL parse failed. " + e.getMessage(), e);
//        }
//    }
//
//    /**
//     * Parses a SQL string as an identifier into a {@link SqlIdentifier}.
//     *
//     * @param identifier a sql string to parse as an identifier
//     * @return a parsed sql node
//     * @throws SqlParserException if an exception is thrown when parsing the identifier
//     */
//    public SqlIdentifier parseIdentifier(String identifier) {
//        try {
//            SqlAbstractParserImpl flinkParser = createFlinkParser(identifier);
//            if (flinkParser instanceof FlinkSqlParserImpl) {
//                return ((FlinkSqlParserImpl) flinkParser).TableApiIdentifier();
//            } else if (flinkParser instanceof FlinkHiveSqlParserImpl) {
//                return ((FlinkHiveSqlParserImpl) flinkParser).TableApiIdentifier();
//            } else {
//                throw new IllegalArgumentException(
//                        "Unrecognized sql parser type " + flinkParser.getClass().getName());
//            }
//        } catch (Exception e) {
//            throw new SqlParserException(
//                    String.format("Invalid SQL identifier %s.", identifier), e);
//        }
//    }
//
//    /**
//     * Equivalent to {@link SqlParser#create(Reader, SqlParser.Config)}. The only difference is we
//     * do not wrap the {@link FlinkSqlParserImpl} with {@link SqlParser}.
//     *
//     * <p>It is so that we can access specific parsing methods not accessible through the {@code
//     * SqlParser}.
//     */
//    private SqlAbstractParserImpl createFlinkParser(String expr) {
//        SourceStringReader reader = new SourceStringReader(expr);
//        SqlAbstractParserImpl parser = config.parserFactory().getParser(reader);
//        parser.setTabSize(1);
//        parser.setQuotedCasing(config.quotedCasing());
//        parser.setUnquotedCasing(config.unquotedCasing());
//        parser.setIdentifierMaxLength(config.identifierMaxLength());
//        parser.setConformance(config.conformance());
//        switch (config.quoting()) {
//            case DOUBLE_QUOTE:
//                parser.switchTo(SqlAbstractParserImpl.LexicalState.DQID);
//                break;
//            case BACK_TICK:
//                parser.switchTo(SqlAbstractParserImpl.LexicalState.BTID);
//                break;
//            case BRACKET:
//                parser.switchTo(SqlAbstractParserImpl.LexicalState.DEFAULT);
//                break;
//        }
//
//        return parser;
//    }
}
