package ddl;

import ddl.constraints.XSqlTableConstraint;
import error.SqlValidateException;
import extended.ExtendedSqlNode;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.ImmutableNullableList;
import ddl.XSqlTableColumn.*;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class will be used to parser new sql.
 * @XCreateTable: Base parsering for create operation
 */
public class XCreateTable extends SqlCreate implements ExtendedSqlNode {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);


    private final SqlIdentifier tableName;

    private final SqlNodeList columnList;

    private final SqlNodeList propertyList;

    private final List<XSqlTableConstraint> tableConstraints;

    private final SqlNodeList partitionKeyList;


    private final SqlCharStringLiteral comment;

//    private final SqlTableLike tableLike;

    private final boolean isTemporary;

    public XCreateTable(SqlOperator operator,
                        SqlParserPos pos,
                        boolean replace,
                        boolean ifNotExists,
                        SqlIdentifier tableName,
                        SqlNodeList columnList,
                        SqlNodeList propertyList,
                        List<XSqlTableConstraint> tableConstraints,
                        SqlNodeList partitionKeyList,
                        SqlCharStringLiteral comment,
                        boolean isTemporary) {
        super(operator, pos, replace, ifNotExists);
        this.tableName = tableName;
        this.columnList = columnList;
        this.propertyList = propertyList;
        this.tableConstraints = tableConstraints;
        this.partitionKeyList = partitionKeyList;
        this.comment = comment;
        this.isTemporary = isTemporary;
    }


    @Override
    public @Nonnull SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public @Nonnull List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                tableName,
                columnList,
                new SqlNodeList(tableConstraints, SqlParserPos.ZERO),
                propertyList,
                partitionKeyList,
                comment);
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    public SqlNodeList getPropertyList() {
        return propertyList;
    }

    public SqlNodeList getPartitionKeyList() {
        return partitionKeyList;
    }

    public List<XSqlTableConstraint> getTableConstraints() {
        return tableConstraints;
    }

    public Optional<SqlCharStringLiteral> getComment() {
        return Optional.ofNullable(comment);
    }

//    public Optional<SqlTableLike> getTableLike() {
//        return Optional.ofNullable(tableLike);
//    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    /** Returns the column constraints plus the table constraints.  */
    public List<XSqlTableConstraint> getFullConstraints() {
        List<XSqlTableConstraint> ret = new ArrayList<>();
        this.columnList.forEach(
                column -> {
                    XSqlTableColumn tableColumn = (XSqlTableColumn) column;
                    if (tableColumn instanceof SqlRegularColumn) {
                        SqlRegularColumn regularColumn = (SqlRegularColumn) tableColumn;
                        regularColumn.getConstraint().map(ret::add);
                    }
                });
        ret.addAll(this.tableConstraints);
        return ret;
    }

    /**
     *
     * @param writer    Target writer
     * @param leftPrec  The precedence of the {@link SqlNode} immediately
     *                  preceding this node in a depth-first scan of the parse
     *                  tree
     * @param rightPrec The precedence of the {@link SqlNode} immediately
     */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (isTemporary()) {
            writer.keyword("TEMPORARY");
        }
        writer.keyword("TABLE");
        if (isIfNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }
        tableName.unparse(writer, leftPrec, rightPrec);
        if (columnList.size() > 0 || tableConstraints.size() > 0) {
            SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
            for (SqlNode column : columnList) {
                printIndent(writer);
                column.unparse(writer, leftPrec, rightPrec);
            }
            if (tableConstraints.size() > 0) {
                for (XSqlTableConstraint constraint : tableConstraints) {
                    printIndent(writer);
                    constraint.unparse(writer, leftPrec, rightPrec);
                }
            }
//            if (watermark != null) {
//                printIndent(writer);
//                watermark.unparse(writer, leftPrec, rightPrec);
//            }

            writer.newlineAndIndent();
            writer.endList(frame);
        }

        if (comment != null) {
            writer.newlineAndIndent();
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
        }

        if (this.partitionKeyList.size() > 0) {
            writer.newlineAndIndent();
            writer.keyword("PARTITIONED BY");
            SqlWriter.Frame partitionedByFrame = writer.startList("(", ")");
            this.partitionKeyList.unparse(writer, leftPrec, rightPrec);
            writer.endList(partitionedByFrame);
            writer.newlineAndIndent();
        }
        if (this.propertyList.size() > 0) {
            writer.keyword("WITH");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : propertyList) {
                printIndent(writer);
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }

//        if (this.tableLike != null) {
//            writer.newlineAndIndent();
//            this.tableLike.unparse(writer, leftPrec, rightPrec);
//        }
    }

    /**
     *printIndent
     */
    protected void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }

    /**
     * This validation is used to
     * @throws SqlValidateException
     */
    @Override
    public void validate() throws SqlValidateException {

        List<XSqlTableConstraint> constraints =
                getFullConstraints().stream()
                        .filter(XSqlTableConstraint::isPrimaryKey)
                        .collect(Collectors.toList());

        if (constraints.size() > 1) {
            throw new SqlValidateException(
                    constraints.get(1).getParserPosition(), "Duplicate primary key definition");
        } else if (constraints.size() == 1) {
            Set<String> primaryKeyColumns =
                    Arrays.stream(constraints.get(0).getColumnNames()).collect(Collectors.toSet());
            for (SqlNode column : columnList) {
                XSqlTableColumn tableColumn = (XSqlTableColumn) column;
                if (tableColumn instanceof SqlRegularColumn
                        && primaryKeyColumns.contains(tableColumn.getName().getSimple())) {
                    SqlRegularColumn regularColumn = (SqlRegularColumn) column;
                    SqlDataTypeSpec notNullType = regularColumn.getType().withNullable(false);
                    regularColumn.setType(notNullType);
                }
            }
        }

    }

    /**
     * Returns the fulltablename
     * @return
     */
    public String[] fullTableName() {
        return tableName.names.toArray(new String[0]);
    }
}
