/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xparser.sql.parser.ddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.ImmutableNullableList;
import xparser.sql.parser.ExtendedSqlNode;
import xparser.sql.parser.ddl.SqlTableColumn.SqlComputedColumn;
import xparser.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import xparser.sql.parser.ddl.constraint.SqlTableConstraint;
import xparser.sql.parser.error.SqlValidateException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/** CREATE TABLE DDL sql call. */
public class SqlCreateTable extends SqlCreate implements ExtendedSqlNode {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    private final SqlIdentifier tableName;

    private final SqlNodeList columnList;

    private final SqlNodeList propertyList;

    private final List<SqlTableConstraint> tableConstraints;

    private final SqlNodeList partitionKeyList;

    private final SqlWatermark watermark;

    private final SqlCharStringLiteral comment;

    private final SqlTableLike tableLike;

    private final boolean isTemporary;

    public SqlCreateTable(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> tableConstraints,
            SqlNodeList propertyList,
            SqlNodeList partitionKeyList,
            @Nullable SqlWatermark watermark,
            @Nullable SqlCharStringLiteral comment,
            @Nullable SqlTableLike tableLike,
            boolean isTemporary,
            boolean ifNotExists) {
        super(OPERATOR, pos, false, ifNotExists);
        this.tableName = requireNonNull(tableName, "tableName should not be null");
        this.columnList = requireNonNull(columnList, "columnList should not be null");
        this.tableConstraints =
                requireNonNull(tableConstraints, "table constraints should not be null");
        this.propertyList = requireNonNull(propertyList, "propertyList should not be null");
        this.partitionKeyList =
                requireNonNull(partitionKeyList, "partitionKeyList should not be null");
        this.watermark = watermark;
        this.comment = comment;
        this.tableLike = tableLike;
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
                watermark,
                comment,
                tableLike);
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

    public List<SqlTableConstraint> getTableConstraints() {
        return tableConstraints;
    }

    public Optional<SqlWatermark> getWatermark() {
        return Optional.ofNullable(watermark);
    }

    public Optional<SqlCharStringLiteral> getComment() {
        return Optional.ofNullable(comment);
    }

    public Optional<SqlTableLike> getTableLike() {
        return Optional.ofNullable(tableLike);
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    @Override
    public void validate() throws SqlValidateException {

        List<SqlTableConstraint> constraints =
                getFullConstraints().stream()
                        .filter(SqlTableConstraint::isPrimaryKey)
                        .collect(Collectors.toList());

        if (constraints.size() > 1) {
            throw new SqlValidateException(
                    constraints.get(1).getParserPosition(), "Duplicate primary key definition");
        } else if (constraints.size() == 1) {
            Set<String> primaryKeyColumns =
                    Arrays.stream(constraints.get(0).getColumnNames()).collect(Collectors.toSet());

            for (SqlNode column : columnList) {
                SqlTableColumn tableColumn = (SqlTableColumn) column;
                if (tableColumn instanceof SqlRegularColumn
                        && primaryKeyColumns.contains(tableColumn.getName().getSimple())) {
                    SqlRegularColumn regularColumn = (SqlRegularColumn) column;
                    SqlDataTypeSpec notNullType = regularColumn.getType().withNullable(false);
                    regularColumn.setType(notNullType);
                }
            }
        }

        if (tableLike != null) {
            tableLike.validate();
        }
    }

    /**
     * This method will be used in some occuasions like judging whether columns only contain regular types
     * @return
     */
    public boolean hasRegularColumnsOnly() {
        for (SqlNode column : columnList) {
            final SqlTableColumn tableColumn = (SqlTableColumn) column;
            if (!(tableColumn instanceof SqlRegularColumn)) {
                return false;
            }
        }
        return true;
    }

    /** Returns the column constraints plus the table constraints. */
    public List<SqlTableConstraint> getFullConstraints() {
        List<SqlTableConstraint> ret = new ArrayList<>();
        this.columnList.forEach(
                column -> {
                    SqlTableColumn tableColumn = (SqlTableColumn) column;
                    if (tableColumn instanceof SqlRegularColumn) {
                        SqlRegularColumn regularColumn = (SqlRegularColumn) tableColumn;
                        regularColumn.getConstraint().map(ret::add);
                    }
                });
        ret.addAll(this.tableConstraints);
        return ret;
    }

    /**
     * Returns the projection format of the DDL columns(including computed columns). i.e. the
     * following DDL:
     *
     * <pre>
     *   create table tbl1(
     *     col1 int,
     *     col2 varchar,
     *     col3 as to_timestamp(col2)
     *   ) with (
     *     'connector' = 'csv'
     *   )
     * </pre>
     *
     * <p>is equivalent with query "col1, col2, to_timestamp(col2) as col3", caution that the
     * "computed column" operandshave been reversed.
     */
    public String getColumnSqlString() {
        SqlPrettyWriter writer =
                new SqlPrettyWriter(
                        SqlPrettyWriter.config()
                                .withDialect(AnsiSqlDialect.DEFAULT)
                                .withAlwaysUseParentheses(true)
                                .withSelectListItemsOnSeparateLines(false)
                                .withIndentation(0));
        writer.startList("", "");
        for (SqlNode column : columnList) {
            writer.sep(",");
            SqlTableColumn tableColumn = (SqlTableColumn) column;
            if (tableColumn instanceof SqlComputedColumn) {
                SqlComputedColumn computedColumn = (SqlComputedColumn) tableColumn;
                computedColumn.getExpr().unparse(writer, 0, 0);
                writer.keyword("AS");
            }
            tableColumn.getName().unparse(writer, 0, 0);
        }

        return writer.toString();
    }

    /**
     * This function is used to analysis all
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
        if (columnList.size() > 0 || tableConstraints.size() > 0 || watermark != null) {
            SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
            for (SqlNode column : columnList) {
                printIndent(writer);
                column.unparse(writer, leftPrec, rightPrec);
            }
            if (tableConstraints.size() > 0) {
                for (SqlTableConstraint constraint : tableConstraints) {
                    printIndent(writer);
                    constraint.unparse(writer, leftPrec, rightPrec);
                }
            }
            if (watermark != null) {
                printIndent(writer);
                watermark.unparse(writer, leftPrec, rightPrec);
            }

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

        if (this.tableLike != null) {
            writer.newlineAndIndent();
            this.tableLike.unparse(writer, leftPrec, rightPrec);
        }
    }

    protected void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }

    /** Table creation context. */
    public static class TableCreationContext {
        public List<SqlNode> columnList = new ArrayList<>();
        public List<SqlTableConstraint> constraints = new ArrayList<>();
        @Nullable public SqlWatermark watermark;
    }

    public String[] fullTableName() {
        return tableName.names.toArray(new String[0]);
    }
}
