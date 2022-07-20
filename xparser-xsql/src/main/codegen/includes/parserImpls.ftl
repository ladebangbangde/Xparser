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

SqlCreate SqlCreateTable(Span s, boolean replace, boolean isTemporary) :
 {
            final SqlParserPos startPos = s.pos();
            boolean ifNotExists = false;
            SqlIdentifier tableName;
            List<SqlTableConstraint> constraints = new ArrayList<SqlTableConstraint>();
                SqlWatermark watermark = null;
                SqlNodeList columnList = SqlNodeList.EMPTY;
                SqlCharStringLiteral comment = null;
                SqlTableLike tableLike = null;

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
                            watermark = ctx.watermark;
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
                                        [
                                        <LIKE>
                                            tableLike = SqlTableLike(getPos())
                                            ]
                                            {
                                            return new SqlCreateTable(startPos.plus(getPos()),
                                            tableName,
                                            columnList,
                                            constraints,
                                            propertyList,
                                            partitionColumns,
                                            watermark,
                                            comment,
                                            tableLike,
                                            isTemporary,
                                            ifNotExists);
                                            }
                                            }