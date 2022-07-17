package com.ldbbd.xparser.impls;

import com.ldbbd.xparser.interfaces.Parser;
import com.ldbbd.xparser.operations.Operation;
import com.ldbbd.xparser.parsers.CalciteParser;

import java.util.List;
import java.util.function.Supplier;

public class HiveParserImpl implements Parser {

    private final Supplier<CalciteParser> calciteParserSupplier;

    public HiveParserImpl(Supplier<CalciteParser> calciteParserSupplier) {
        this.calciteParserSupplier = calciteParserSupplier;
    }

    @Override
    public List<Operation> parse(String statement) {
        return null;
//        //获取解析器
//        CalciteParser parser = calciteParserSupplier.get();
////        FlinkPlannerImpl planner = validatorSupplier.get();
//        //预处理
//        Optional<Operation> command = EXTENDED_PARSER.parse(statement);
//        if (command.isPresent()) {
//            return Collections.singletonList(command.get());
//        }
//        // parse the sql query
//        SqlNode parsed = parser.parse(statement);
//        Operation operation =
//                SqlToOperationConverter.convert(planner, catalogManager, parsed)
//                        .orElseThrow(() -> new TableException("Unsupported query: " + statement));
//        return Collections.singletonList(operation);
    }


    @Override
    public String[] getCompletionHints(String statement, int position) {
        return new String[0];
    }
}
