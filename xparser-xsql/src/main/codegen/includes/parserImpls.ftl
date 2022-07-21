<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

/**
 * Parses a "IF EXISTS" option, default is false.
 */


/**/
SqlCreate SqlCreateExtended(Span s, boolean replace) :
{
    final SqlCreate create;
    boolean isTemporary = false;
}
{
[
<TEMPORARY> { isTemporary = true; }
    ]
        (
            create = XSqlCreateTable(s, replace, isTemporary)
        )
    {
    return create;
    }
}

boolean IfExistsOpt() :
{
}
{
    (
        LOOKAHEAD(2)
        <IF> <EXISTS> { return true; }
    |
        { return false; }
    )
}

/**
 * Parses a "IF NOT EXISTS" option, default is false.
*/
boolean IfNotExistsOpt() :
{
}
{
    (
        LOOKAHEAD(3)
        <IF> <NOT> <EXISTS> { return true; }
    |
        { return false; }
    )
}

/***********************************************************************************************/
/*
*"TableColumn" can be regard as container of info about columns during creating table operation
*/
void TableColumn(TableCreationContext context):
{
            XSqlTableConstraint constraint;
}
{
(
    LOOKAHEAD(2)
    TypedColumn(context)
  |
   constraint = TableConstraint(){
         context.constraints.add(constraint)
  }
  |
   ComputedColumn(context)
)
}

/** Parses {@code column_name AS expr [COMMENT 'comment']}. */
void ComputedColumn(TableCreationContext context) :
{
            SqlIdentifier name;
            SqlParserPos pos;
            SqlNode expr;
            SqlNode comment = null;
}
{
            name = SimpleIdentifier() {pos = getPos();}
            <AS>
                expr = Expression(ExprContext.ACCEPT_NON_QUERY)
                [
                <COMMENT>
                    comment = StringLiteral()
                ]
            {
              XSqlTableColumn computedColumn = new XSqlTableColumn.SqlComputedColumn(
              getPos(),
              name,
              comment,
              expr);
              context.columnList.add(computedColumn);
            }
}


/** Parses {@code column_name column_data_type [...]}. */
void TypedColumn(TableCreationContext context) :
{
                    SqlIdentifier name;
                    SqlParserPos pos;
                    SqlDataTypeSpec type;
}
{
                    name = SimpleIdentifier() {pos = getPos();}
                    type = ExtendedDataType()
                    (
                    RegularColumn(context, name, type)
                    )
}
/********************************************************************************************************/

/*
*"ConstraintEnforcement"
*/
SqlLiteral ConstraintEnforcement() :
{
SqlLiteral enforcement;
}
{
     (
      <ENFORCED> {
        enforcement = XSqlConstraintEnforcement.ENFORCED.symbol(getPos());
       }
       |
       <NOT> <ENFORCED> {
       enforcement = XSqlConstraintEnforcement.NOT_ENFORCED.symbol(getPos());
        }
      )
        {
         return enforcement;
         }
}


/** Parses a table constraint for CREATE TABLE. */
SqlTableConstraint TableConstraint() :
{
           SqlIdentifier constraintName = null;
           final SqlLiteral uniqueSpec;
           final SqlNodeList columns;
           SqlLiteral enforcement = null;
}
{

           [ constraintName = ConstraintName() ]
           uniqueSpec = UniqueSpec()
           columns = ParenthesizedSimpleIdentifierList()
           [ enforcement = ConstraintEnforcement() ]
{
           return new XSqlTableConstraint(
           constraintName,
           uniqueSpec,
           columns,
           enforcement,
           true,
           getPos());
}
}

/**
* Different with {@link #DataType()}, we support a [ NULL | NOT NULL ] suffix syntax for both the
* collection element data type and the data type itself.
*
* <p>See {@link #SqlDataTypeSpec} for the syntax details of {@link #DataType()}.
*/
SqlDataTypeSpec ExtendedDataType() :
{
               SqlTypeNameSpec typeName;
               final Span s;
               boolean elementNullable = true;
               boolean nullable = true;
}
{
               <#-- #DataType does not take care of the nullable attribute. -->
               typeName = TypeName() {
               s = span();
            }
               (
               LOOKAHEAD(3)
               elementNullable = NullableOptDefaultTrue()
               typeName = ExtendedCollectionsTypeName(typeName, elementNullable)
               )*
               nullable = NullableOptDefaultTrue()
               {
               return new SqlDataTypeSpec(typeName, s.end(this)).withNullable(nullable);
               }
}

/**For RegularColumn**/
void RegularColumn(List<SqlNode> list) :
{
                   SqlIdentifier name;
                   SqlDataTypeSpec type;
                   SqlNode comment = null;
}
{
                   name = SimpleIdentifier()
                   type = ExtendedDataType()
                   [
                   <COMMENT>
                       comment = StringLiteral()
                       ]
                       {
                       SqlTableColumn regularColumn = new XSqlTableColumn.SqlRegularColumn(
                       getPos(),
                       name,
                       comment,
                       type,
                       null);
                       list.add(regularColumn);
                       }
 }




/**
* Parse a collection type name, the input element type name may
* also be a collection type. Different with #CollectionsTypeName,
* the element type can have a [ NULL | NOT NULL ] suffix, default is NULL(nullable).
*/
SqlTypeNameSpec ExtendedCollectionsTypeName(
               SqlTypeNameSpec elementTypeName,
               boolean elementNullable) :
{
               final SqlTypeName collectionTypeName;
}
{
               (
               <MULTISET> { collectionTypeName = SqlTypeName.MULTISET; }
                   |
               <ARRAY> { collectionTypeName = SqlTypeName.ARRAY; }
               )
               {
                       return new XExtendedSqlCollectionTypeNameSpec(
                       elementTypeName,
                       elementNullable,
                       collectionTypeName,
                       true,
                       getPos());
               }
}








/*
*
*
*
*/
SqlNode TableOption() :
{
    SqlNode key;
    SqlNode value;
    SqlParserPos pos;
}
 {
     key = StringLiteral()
  { pos = getPos(); }
      <EQ> value = StringLiteral()
     {
       return new XSqlTableOption(key, value, getPos());
     }
   }

/*
*
*
*
*/
XSqlTableConstraint ColumnConstraint(SqlIdentifier column) :
{
            SqlIdentifier constraintName = null;
            final SqlLiteral uniqueSpec;
            SqlLiteral enforcement = null;
}
{
            [ constraintName = ConstraintName() ]
            uniqueSpec = UniqueSpec()
            [ enforcement = ConstraintEnforcement() ]
            {
            return new XSqlTableConstraint(
                constraintName,
                uniqueSpec,
                SqlNodeList.of(column),
                enforcement,
                false,
                getPos());
            }
}

/*
*"ConstraintName()" Method for stored SqlIndentifier Name
* SqlIdentifier constraintName
*
*/
SqlIdentifier ConstraintName() :
{
SqlIdentifier constraintName;
}
{
    <CONSTRAINT> constraintName = SimpleIdentifier() {
            return constraintName;
    }
}
/********/
SqlLiteral UniqueSpec() :
{
   SqlLiteral uniqueSpec;
}
        {
        (
        <PRIMARY> <KEY> {
            uniqueSpec = XSqlUniqueSpec.PRIMARY_KEY.symbol(getPos());
        }
        |
        <UNIQUE> {
                uniqueSpec = XSqlUniqueSpec.UNIQUE.symbol(getPos());
         }
        )
        {
             return uniqueSpec;
        }
}

/** Parse a table properties. */
SqlNodeList TableProperties():
{
   SqlNode property;
      final List<SqlNode> proList = new ArrayList<SqlNode>();
      final Span span;
}
{
     <LPAREN> { span = span(); }
       [
        property = TableOption()
        {
          proList.add(property);
        }
           (
            <COMMA> property = TableOption()
            {
                proList.add(property);
            }
            )*
            ]
            <RPAREN>
            {  return new SqlNodeList(proList, span.end(this)); }
}


/*
* "XSqlCreateTable" is used by creating table
*  boolean ifNotExists
*  SqlIdentifier tableName
*  List<XSqlTableConstraint> constraints
*  SqlNodeList columnList
* SqlCharStringLiteral comment
* SqlNodeList propertyList
* SqlNodeList partitionColumns
* SqlParserPos pos
*/
SqlCreate XSqlCreateTable(Span s, boolean replace, boolean isTemporary) :
 {
                final SqlParserPos startPos = s.pos();
                boolean ifNotExists = false;
                SqlIdentifier tableName;
                List<XSqlTableConstraint> constraints = new ArrayList<XSqlTableConstraint>();
                SqlNodeList columnList = SqlNodeList.EMPTY;
                SqlCharStringLiteral comment = null;
                SqlNodeList propertyList = SqlNodeList.EMPTY;
                SqlNodeList partitionColumns = SqlNodeList.EMPTY;
                SqlParserPos pos = startPos;
      }
                {
                <TABLE>
                    ifNotExists = IfNotExistsOpt()
                    tableName = CompoundIdentifier()
                    [
                    <LPAREN> {
                    pos = getPos();
                    TableCreationContext ctx = new TableCreationContext();}
                        TableColumn(ctx)
                        (
                    <COMMA> TableColumn(ctx)
                            )*
                            {
                            pos = pos.plus(getPos());
                            columnList = new SqlNodeList(ctx.columnList, pos);
                            constraints = ctx.constraints;
                            }
                    <RPAREN>
                                ]
                                [ <COMMENT> <QUOTED_STRING> {
                                String p = SqlParserUtil.parseString(token.image);
                                comment = SqlLiteral.createCharString(p, getPos());
                                }]
                                [
                                <PARTITIONED> <BY>
                                    partitionColumns = ParenthesizedSimpleIdentifierList()
                                    ]
                                    [
                                    <WITH>
                                        propertyList = TableProperties()
                                        ]

                                    {
                                            return new XSqlCreateTable(startPos.plus(getPos()),
                                                                tableName,
                                                                columnList,
                                                                constraints,
                                                                propertyList,
                                                                partitionColumns,
                                                                comment,
                                                                tableLike,
                                                                isTemporary,
                                                                ifNotExists);
                                       }
 }
