package xparser.sql.parser.utils;

import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.parser.impl.ParseException;

/** Compiler-checked resources for the Flink SQL parser. */
public interface ParserResource {

    /** Resources. */
    ParserResource RESOURCE = Resources.create(ParserResource.class);

    @Resources.BaseMessage("Multiple WATERMARK statements is not supported yet.")
    Resources.ExInst<ParseException> multipleWatermarksUnsupported();

    @Resources.BaseMessage("OVERWRITE expression is only used with INSERT statement.")
    Resources.ExInst<ParseException> overwriteIsOnlyUsedWithInsert();

    @Resources.BaseMessage(
            "CREATE SYSTEM FUNCTION is not supported, system functions can only be registered as temporary function, you can use CREATE TEMPORARY SYSTEM FUNCTION instead.")
    Resources.ExInst<ParseException> createSystemFunctionOnlySupportTemporary();

    @Resources.BaseMessage("Duplicate EXPLAIN DETAIL is not allowed.")
    Resources.ExInst<ParseException> explainDetailIsDuplicate();
}
