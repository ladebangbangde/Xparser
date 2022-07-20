package ddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.NlsString;
import xparser.sql.parser.SqlProperty;
import xparser.sql.parser.ddl.SqlTableOption;
import ddl.SqlHiveTableColum.SqlRegularColumn;

import org.apache.commons.lang3.StringEscapeUtils;
import xparser.sql.parser.impl.ParseException;

import java.util.*;

public class HiveDDLUtils {
    private static final UnescapeStringLiteralShuttle UNESCAPE_SHUTTLE =
            new UnescapeStringLiteralShuttle();
    private static final Set<String> RESERVED_TABLE_PROPERTIES = new HashSet<>();
    private static final List<String> RESERVED_TABLE_PROP_PREFIX = new ArrayList<>();
    public static SqlTableOption toTableOption(String key, SqlNode value, SqlParserPos pos) {
        return new SqlTableOption(SqlLiteral.createCharString(key, pos), value, pos);
    }

    public static SqlTableOption toTableOption(String key, String value, SqlParserPos pos) {
        return new SqlTableOption(
                SqlLiteral.createCharString(key, pos),
                SqlLiteral.createCharString(value, pos),
                pos);
    }
    // the input of sql-client will escape '\', unescape it so that users can write hive dialect
    public static void unescapeProperties(SqlNodeList properties) {
        if (properties != null) {
            properties.accept(UNESCAPE_SHUTTLE);
        }
    }
    public static SqlCharStringLiteral unescapeStringLiteral(SqlCharStringLiteral literal) {
        if (literal != null) {
            return (SqlCharStringLiteral) literal.accept(UNESCAPE_SHUTTLE);
        }
        return null;
    }
    public static SqlNodeList deepCopyColList(SqlNodeList colList) {
        SqlNodeList res = new SqlNodeList(colList.getParserPosition());
        for (SqlNode node : colList) {
            res.add(deepCopyTableColumn((SqlHiveTableColum.SqlRegularColumn) node));
        }
        return res;
    }
    public static SqlNodeList checkReservedTableProperties(SqlNodeList props)
            throws ParseException {
        props = checkReservedProperties(RESERVED_TABLE_PROPERTIES, props, "Tables");
        props = checkReservedPrefix(RESERVED_TABLE_PROP_PREFIX, props, "Tables");
        return props;
    }

    private static SqlNodeList checkReservedProperties(
            Set<String> reservedProperties, SqlNodeList properties, String metaType)
            throws ParseException {
        if (properties == null) {
            return null;
        }
        Set<String> match = new HashSet<>();
        for (SqlNode node : properties) {
            if (node instanceof SqlTableOption) {
                String key = ((SqlTableOption) node).getKeyString();
                if (reservedProperties.contains(key)) {
                    match.add(key);
                }
            }
        }
        if (!match.isEmpty()) {
            throw new ParseException(
                    String.format(
                            "Properties %s are reserved and shouldn't be used for Hive %s",
                            match, metaType));
        }
        return properties;
    }
    private static SqlNodeList checkReservedPrefix(
            List<String> reserved, SqlNodeList properties, String metaType) throws ParseException {
        if (properties == null) {
            return null;
        }
        Set<String> match = new HashSet<>();
        for (SqlNode node : properties) {
            if (node instanceof SqlTableOption) {
                String key = ((SqlTableOption) node).getKeyString();
                for (String prefix : reserved) {
                    if (key.startsWith(prefix)) {
                        match.add(key);
                    }
                }
            }
        }
        if (!match.isEmpty()) {
            throw new ParseException(
                    String.format(
                            "Properties %s have reserved prefix and shouldn't be used for Hive %s",
                            match, metaType));
        }
        return properties;
    }

    public static SqlRegularColumn deepCopyTableColumn(SqlRegularColumn column) {
        return new SqlRegularColumn(
                column.getParserPosition(),
                column.getName(),
                column.getComment().orElse(null),
                column.getType(),
                column.getConstraint().orElse(null));
    }
    private static class UnescapeStringLiteralShuttle extends SqlShuttle {

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode unescaped = nodeList.get(i).accept(this);
                nodeList.set(i, unescaped);
            }
            return nodeList;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlProperty) {
                SqlProperty property = (SqlProperty) call;
                Comparable comparable = SqlLiteral.value(property.getValue());
                if (comparable instanceof NlsString) {
                    String val =
                            StringEscapeUtils.unescapeJava(((NlsString) comparable).getValue());
                    return new SqlProperty(
                            property.getKey(),
                            SqlLiteral.createCharString(
                                    val, property.getValue().getParserPosition()),
                            property.getParserPosition());
                }
            } else if (call instanceof SqlTableOption) {
                SqlTableOption option = (SqlTableOption) call;
                String key = StringEscapeUtils.unescapeJava(option.getKeyString());
                String val = StringEscapeUtils.unescapeJava(option.getValueString());
                SqlNode keyNode =
                        SqlLiteral.createCharString(key, option.getKey().getParserPosition());
                SqlNode valNode =
                        SqlLiteral.createCharString(val, option.getValue().getParserPosition());
                return new SqlTableOption(keyNode, valNode, option.getParserPosition());
            }
            return call;
        }

        @Override
        public SqlNode visit(SqlLiteral literal) {
            if (literal instanceof SqlCharStringLiteral) {
                SqlCharStringLiteral stringLiteral = (SqlCharStringLiteral) literal;
                String unescaped =
                        StringEscapeUtils.unescapeJava(stringLiteral.getNlsString().getValue());
                return SqlLiteral.createCharString(unescaped, stringLiteral.getParserPosition());
            }
            return literal;
        }
    }
}
