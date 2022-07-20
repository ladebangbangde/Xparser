package ddl;

import ddl.constraints.SqlTableConstraint;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import xparser.sql.parser.ddl.SqlTableOption;
import xparser.sql.parser.impl.ParseException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SqlCreateHiveTable extends SqlCreateTable {

    private final SqlIdentifier tableName;
    private final SqlNodeList columnList;
    private final SqlNodeList propertyList;
    private final List<SqlTableConstraint> tableConstraints;
    private final SqlNodeList partitionKeyList;
    private final SqlNodeList originPropList;
    private final boolean isExternal;
    private final HiveTableRowFormat rowFormat;
    private final HiveTableStoredAs storedAs;
    private final SqlCharStringLiteral location;

    private final SqlNodeList origColList;
    private final SqlNodeList origPartColList;
    private final HiveTableRowFormat rowFormat;
    private final HiveTableStoredAs storedAs;
    private final boolean replace;

    protected SqlCreateHiveTable(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> tableConstraints,
            SqlNodeList propertyList,
            SqlNodeList partColList,
            @Nullable SqlCharStringLiteral comment,
            @Nullable SqlTableLike tableLike,
            boolean isTemporary,
            boolean ifNotExists,
            HiveTableRowFormat rowFormat,
            HiveTableStoredAs storedAs,
            SqlCharStringLiteral location, boolean replace) throws ParseException {
        super();
        this.replace = replace;
        HiveDDLUtils.unescapeProperties(propertyList);
        this.origColList = HiveDDLUtils.deepCopyColList(columnList);
        this.origPartColList =
                partColList != null ? HiveDDLUtils.deepCopyColList(partColList) : SqlNodeList.EMPTY;


    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }


    /**
     * To represent ROW FORMAT in CREATE TABLE DDL.
     */
    public static class HiveTableRowFormat {

        public static final String SERDE_LIB_CLASS_NAME = "hive.serde.lib.class.name";
        public static final String SERDE_INFO_PROP_PREFIX = "hive.serde.info.prop.";
        public static final String FIELD_DELIM = SERDE_INFO_PROP_PREFIX + "field.delim";
        public static final String COLLECTION_DELIM = SERDE_INFO_PROP_PREFIX + "collection.delim";
        public static final String ESCAPE_CHAR = SERDE_INFO_PROP_PREFIX + "escape.delim";
        public static final String MAPKEY_DELIM = SERDE_INFO_PROP_PREFIX + "mapkey.delim";
        public static final String LINE_DELIM = SERDE_INFO_PROP_PREFIX + "line.delim";
        public static final String SERIALIZATION_NULL_FORMAT =
                SERDE_INFO_PROP_PREFIX + "serialization.null.format";

        private final SqlParserPos pos;
        private final Map<String, SqlCharStringLiteral> delimitPropToValue = new LinkedHashMap<>();
        private final SqlCharStringLiteral serdeClass;
        private final SqlNodeList serdeProps;

        private HiveTableRowFormat(
                SqlParserPos pos,
                SqlCharStringLiteral fieldsTerminator,
                SqlCharStringLiteral escape,
                SqlCharStringLiteral collectionTerminator,
                SqlCharStringLiteral mapKeyTerminator,
                SqlCharStringLiteral linesTerminator,
                SqlCharStringLiteral nullAs,
                SqlCharStringLiteral serdeClass,
                SqlNodeList serdeProps)
                throws ParseException {
            this.pos = pos;
            if (fieldsTerminator != null) {
                delimitPropToValue.put(FIELD_DELIM, fieldsTerminator);
            }
            if (escape != null) {
                delimitPropToValue.put(ESCAPE_CHAR, escape);
            }
            if (collectionTerminator != null) {
                delimitPropToValue.put(COLLECTION_DELIM, collectionTerminator);
            }
            if (mapKeyTerminator != null) {
                delimitPropToValue.put(MAPKEY_DELIM, mapKeyTerminator);
            }
            if (linesTerminator != null) {
                delimitPropToValue.put(LINE_DELIM, linesTerminator);
            }
            if (nullAs != null) {
                delimitPropToValue.put(SERIALIZATION_NULL_FORMAT, nullAs);
            }
            this.serdeClass = serdeClass;
            this.serdeProps = serdeProps;
            validate();
        }

        private void validate() throws ParseException {
            if (!delimitPropToValue.isEmpty()) {
                if (serdeClass != null || serdeProps != null) {
                    throw new ParseException("Both DELIMITED and SERDE specified");
                }
            } else {
                if (serdeClass == null) {
                    throw new ParseException("Neither DELIMITED nor SERDE specified");
                }
            }
        }

        public SqlNodeList toPropList() {
            SqlNodeList list = new SqlNodeList(pos);
            if (serdeClass != null) {
                list.add(HiveDDLUtils.toTableOption(SERDE_LIB_CLASS_NAME, serdeClass, pos));
                if (serdeProps != null) {
                    for (SqlNode sqlNode : serdeProps) {
                        SqlTableOption option = (SqlTableOption) sqlNode;
                        list.add(
                                HiveDDLUtils.toTableOption(
                                        SERDE_INFO_PROP_PREFIX + option.getKeyString(),
                                        option.getValue(),
                                        pos));
                    }
                }
            } else {
                for (String prop : delimitPropToValue.keySet()) {
                    list.add(HiveDDLUtils.toTableOption(prop, delimitPropToValue.get(prop), pos));
                }
            }
            HiveDDLUtils.unescapeProperties(list);
            return list;
        }

    }

    // Extract the identifiers from partition col list -- that's what SqlCreateTable expects for
    // partition keys
    private static SqlNodeList extractPartColIdentifiers(SqlNodeList partCols) {
        if (partCols == null) {
            return null;
        }
        SqlNodeList res = new SqlNodeList(partCols.getParserPosition());
        for (SqlNode node : partCols) {
            SqlHiveTableColum partCol = (SqlHiveTableColum) node;
            res.add(partCol.getName());
        }
        return res;
    }

    /** To represent STORED AS in CREATE TABLE DDL. */
    public static class HiveTableStoredAs {

        public static final String STORED_AS_FILE_FORMAT = "hive.storage.file-format";
        public static final String STORED_AS_INPUT_FORMAT = "hive.stored.as.input.format";
        public static final String STORED_AS_OUTPUT_FORMAT = "hive.stored.as.output.format";

        private final SqlParserPos pos;
        private final SqlIdentifier fileFormat;
        private final SqlCharStringLiteral intputFormat;
        private final SqlCharStringLiteral outputFormat;

        private HiveTableStoredAs(
                SqlParserPos pos,
                SqlIdentifier fileFormat,
                SqlCharStringLiteral intputFormat,
                SqlCharStringLiteral outputFormat)
                throws ParseException {
            this.pos = pos;
            this.fileFormat = fileFormat;
            this.intputFormat = intputFormat;
            this.outputFormat = outputFormat;
            validate();
        }

        private void validate() throws ParseException {
            if (fileFormat != null) {
                if (intputFormat != null || outputFormat != null) {
                    throw new ParseException(
                            "Both file format and input/output format are specified");
                }
            } else {
                if (intputFormat == null || outputFormat == null) {
                    throw new ParseException(
                            "Neither file format nor input/output format is specified");
                }
            }
        }

        public SqlNodeList toPropList() {
            SqlNodeList res = new SqlNodeList(pos);
            if (fileFormat != null) {
                res.add(
                        HiveDDLUtils.toTableOption(
                                STORED_AS_FILE_FORMAT,
                                fileFormat.getSimple(),
                                fileFormat.getParserPosition()));
            } else {
                res.add(
                        HiveDDLUtils.toTableOption(
                                STORED_AS_INPUT_FORMAT,
                                intputFormat,
                                intputFormat.getParserPosition()));
                res.add(
                        HiveDDLUtils.toTableOption(
                                STORED_AS_OUTPUT_FORMAT,
                                outputFormat,
                                outputFormat.getParserPosition()));
            }
            return res;
        }

        public static HiveTableStoredAs ofFileFormat(SqlParserPos pos, SqlIdentifier fileFormat)
                throws ParseException {
            return new HiveTableStoredAs(pos, fileFormat, null, null);
        }

        public static HiveTableStoredAs ofInputOutputFormat(
                SqlParserPos pos,
                SqlCharStringLiteral intputFormat,
                SqlCharStringLiteral outputFormat)
                throws ParseException {
            return new HiveTableStoredAs(pos, null, intputFormat, outputFormat);
        }
    }

}



