package com.ldbbd.xparser.interfaces;

import com.ldbbd.xparser.operations.Operation;

import java.util.List;

public interface Parser {

        /**
         * Entry point for parsing SQL queries expressed as a String.
         *
         * <p><b>Note:</b>If the created {@link Operation} is a {@link QueryOperation} it must be in a
         * form that will be understood by the {@link Planner#translate(List)} method.
         *
         * <p>The produced Operation trees should already be validated.
         *
         * @param statement the SQL statement to evaluate
         * @return parsed queries as trees of relational {@link Operation}s
         * @throws org.apache.flink.table.api.SqlParserException when failed to parse the statement
         */
        /**
         * Entry point for parsing SQL queries expressed as a String
         * @param statement
         * @return
         */
        List<Operation> parse(String statement);




        /**
         * Returns completion hints for the given statement at the given cursor position. The completion
         * happens case insensitively.
         *
         * @param statement Partial or slightly incorrect SQL statement
         * @param position cursor position
         * @return completion hints that fit at the current cursor position
         */
        String[] getCompletionHints(String statement, int position);
}
