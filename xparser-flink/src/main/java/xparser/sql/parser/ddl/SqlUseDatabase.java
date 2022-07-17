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
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

/** USE [catalog.]database sql call. */
public class SqlUseDatabase extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("USE DATABASE", SqlKind.OTHER_DDL);
    private final SqlIdentifier databaseName;

    public SqlUseDatabase(SqlParserPos pos, SqlIdentifier databaseName) {
        super(pos);
        this.databaseName = databaseName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList(databaseName);
    }

    public SqlIdentifier getDatabaseName() {
        return databaseName;
    }

    public String[] fullDatabaseName() {
        return databaseName.names.toArray(new String[0]);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("USE");
        databaseName.unparse(writer, leftPrec, rightPrec);
    }
}
