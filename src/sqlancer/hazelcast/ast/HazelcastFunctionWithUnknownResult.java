package sqlancer.hazelcast.ast;

import sqlancer.Randomly;
import sqlancer.hazelcast.gen.HazelcastExpressionGenerator;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

import java.util.ArrayList;
import java.util.List;

public enum HazelcastFunctionWithUnknownResult {

//    ABBREV("abbrev", HazelcastDataType.TEXT, HazelcastDataType.INET),
//    BROADCAST("broadcast", HazelcastDataType.INET, HazelcastDataType.INET),
//    FAMILY("family", HazelcastDataType.INT, HazelcastDataType.INET),
//    HOSTMASK("hostmask", HazelcastDataType.INET, HazelcastDataType.INET),
//    MASKLEN("masklen", HazelcastDataType.INT, HazelcastDataType.INET),
//    NETMASK("netmask", HazelcastDataType.INET, HazelcastDataType.INET),
//    SET_MASKLEN("set_masklen", HazelcastDataType.INET, HazelcastDataType.INET, HazelcastDataType.INT),
//    TEXT("text", HazelcastDataType.TEXT, HazelcastDataType.INET),
//    INET_SAME_FAMILY("inet_same_family", HazelcastDataType.BOOLEAN, HazelcastDataType.INET, HazelcastDataType.INET),

    // https://www.postgresql.org/docs/devel/functions-admin.html#FUNCTIONS-ADMIN-SIGNAL-TABLE
    // PG_RELOAD_CONF("pg_reload_conf", PostgresDataType.BOOLEAN), // too much output
    // PG_ROTATE_LOGFILE("pg_rotate_logfile", PostgresDataType.BOOLEAN), prints warning

    // https://www.postgresql.org/docs/devel/functions-info.html#FUNCTIONS-INFO-SESSION-TABLE
    CURRENT_DATABASE("current_database", HazelcastDataType.TEXT), // name
    // CURRENT_QUERY("current_query", PostgresDataType.TEXT), // can generate false positives
    CURRENT_SCHEMA("current_schema", HazelcastDataType.TEXT), // name
    // CURRENT_SCHEMAS("current_schemas", PostgresDataType.TEXT, PostgresDataType.BOOLEAN),
    INET_CLIENT_PORT("inet_client_port", HazelcastDataType.INT),
    // INET_SERVER_PORT("inet_server_port", PostgresDataType.INT),
    PG_BACKEND_PID("pg_backend_pid", HazelcastDataType.INT),
    PG_CURRENT_LOGFILE("pg_current_logfile", HazelcastDataType.TEXT),
    PG_IS_OTHER_TEMP_SCHEMA("pg_is_other_temp_schema", HazelcastDataType.BOOLEAN),
    PG_JIT_AVAILABLE("pg_jit_available", HazelcastDataType.BOOLEAN),
//    PG_NOTIFICATION_QUEUE_USAGE("pg_notification_queue_usage", HazelcastDataType.REAL),
    PG_TRIGGER_DEPTH("pg_trigger_depth", HazelcastDataType.INT), VERSION("version", HazelcastDataType.TEXT),

    //
    TO_CHAR("to_char", HazelcastDataType.TEXT, HazelcastDataType.TEXT, HazelcastDataType.TEXT) {
        @Override
        public HazelcastExpression[] getArguments(HazelcastDataType returnType, HazelcastExpressionGenerator gen,
                                                 int depth) {
            HazelcastExpression[] args = super.getArguments(returnType, gen, depth);
            args[0] = gen.generateExpression(HazelcastDataType.getRandomType());
            return args;
        }
    },

    // String functions
    ASCII("ascii", HazelcastDataType.INT, HazelcastDataType.TEXT),
    BTRIM("btrim", HazelcastDataType.TEXT, HazelcastDataType.TEXT, HazelcastDataType.TEXT),
    CHR("chr", HazelcastDataType.TEXT, HazelcastDataType.INT),
    CONVERT_FROM("convert_from", HazelcastDataType.TEXT, HazelcastDataType.TEXT, HazelcastDataType.TEXT) {
        @Override
        public HazelcastExpression[] getArguments(HazelcastDataType returnType, HazelcastExpressionGenerator gen,
                                                 int depth) {
            HazelcastExpression[] args = super.getArguments(returnType, gen, depth);
            args[1] = HazelcastConstant.createTextConstant(Randomly.fromOptions("UTF8", "LATIN1"));
            return args;
        }
    },
    // concat
    // segfault
    // BIT_LENGTH("bit_length", PostgresDataType.INT, PostgresDataType.TEXT),
    INITCAP("initcap", HazelcastDataType.TEXT, HazelcastDataType.TEXT),
    LEFT("left", HazelcastDataType.TEXT, HazelcastDataType.INT, HazelcastDataType.TEXT),
    LOWER("lower", HazelcastDataType.TEXT, HazelcastDataType.TEXT),
    MD5("md5", HazelcastDataType.TEXT, HazelcastDataType.TEXT),
    UPPER("upper", HazelcastDataType.TEXT, HazelcastDataType.TEXT),
    // PG_CLIENT_ENCODING("pg_client_encoding", PostgresDataType.TEXT),
    QUOTE_LITERAL("quote_literal", HazelcastDataType.TEXT, HazelcastDataType.TEXT),
    QUOTE_IDENT("quote_ident", HazelcastDataType.TEXT, HazelcastDataType.TEXT),
    REGEX_REPLACE("regex_replace", HazelcastDataType.TEXT, HazelcastDataType.TEXT, HazelcastDataType.TEXT),
    // REPEAT("repeat", PostgresDataType.TEXT, PostgresDataType.TEXT,
    // PostgresDataType.INT),
    REPLACE("replace", HazelcastDataType.TEXT, HazelcastDataType.TEXT, HazelcastDataType.TEXT),
    REVERSE("reverse", HazelcastDataType.TEXT, HazelcastDataType.TEXT),
    RIGHT("right", HazelcastDataType.TEXT, HazelcastDataType.TEXT, HazelcastDataType.INT),
    RPAD("rpad", HazelcastDataType.TEXT, HazelcastDataType.INT, HazelcastDataType.TEXT),
    RTRIM("rtrim", HazelcastDataType.TEXT, HazelcastDataType.TEXT),
    SPLIT_PART("split_part", HazelcastDataType.TEXT, HazelcastDataType.TEXT, HazelcastDataType.INT),
    STRPOS("strpos", HazelcastDataType.INT, HazelcastDataType.TEXT, HazelcastDataType.TEXT),
    SUBSTR("substr", HazelcastDataType.TEXT, HazelcastDataType.TEXT, HazelcastDataType.INT, HazelcastDataType.INT),
    TO_ASCII("to_ascii", HazelcastDataType.TEXT, HazelcastDataType.TEXT),
    TO_HEX("to_hex", HazelcastDataType.INT, HazelcastDataType.TEXT),
    TRANSLATE("translate", HazelcastDataType.TEXT, HazelcastDataType.TEXT, HazelcastDataType.TEXT, HazelcastDataType.TEXT),
    // mathematical functions
    // https://www.postgresql.org/docs/9.5/functions-math.html
//    ABS("abs", HazelcastDataType.REAL, HazelcastDataType.REAL),
//    CBRT("cbrt", HazelcastDataType.REAL, HazelcastDataType.REAL), CEILING("ceiling", HazelcastDataType.REAL), //
//    DEGREES("degrees", HazelcastDataType.REAL), EXP("exp", HazelcastDataType.REAL), LN("ln", HazelcastDataType.REAL),
//    LOG("log", HazelcastDataType.REAL), LOG2("log", HazelcastDataType.REAL, HazelcastDataType.REAL),
//    PI("pi", HazelcastDataType.REAL), POWER("power", HazelcastDataType.REAL, HazelcastDataType.REAL),
//    TRUNC("trunc", HazelcastDataType.REAL, HazelcastDataType.INT),
//    TRUNC2("trunc", HazelcastDataType.REAL, HazelcastDataType.INT, HazelcastDataType.REAL),
//    FLOOR("floor", HazelcastDataType.REAL),

    // trigonometric functions - complete
    // https://www.postgresql.org/docs/12/functions-math.html#FUNCTIONS-MATH-TRIG-TABLE
//    ACOS("acos", HazelcastDataType.REAL), //
//    ACOSD("acosd", HazelcastDataType.REAL), //
//    ASIN("asin", HazelcastDataType.REAL), //
//    ASIND("asind", HazelcastDataType.REAL), //
//    ATAN("atan", HazelcastDataType.REAL), //
//    ATAND("atand", HazelcastDataType.REAL), //
//    ATAN2("atan2", HazelcastDataType.REAL, HazelcastDataType.REAL), //
//    ATAN2D("atan2d", HazelcastDataType.REAL, HazelcastDataType.REAL), //
//    COS("cos", HazelcastDataType.REAL), //
//    COSD("cosd", HazelcastDataType.REAL), //
//    COT("cot", HazelcastDataType.REAL), //
//    COTD("cotd", HazelcastDataType.REAL), //
//    SIN("sin", HazelcastDataType.REAL), //
//    SIND("sind", HazelcastDataType.REAL), //
//    TAN("tan", HazelcastDataType.REAL), //
//    TAND("tand", HazelcastDataType.REAL), //

    // hyperbolic functions - complete
    // https://www.postgresql.org/docs/12/functions-math.html#FUNCTIONS-MATH-HYP-TABLE
//    SINH("sinh", HazelcastDataType.REAL), //
//    COSH("cosh", HazelcastDataType.REAL), //
//    TANH("tanh", HazelcastDataType.REAL), //
//    ASINH("asinh", HazelcastDataType.REAL), //
//    ACOSH("acosh", HazelcastDataType.REAL), //
//    ATANH("atanh", HazelcastDataType.REAL), //

    // https://www.postgresql.org/docs/devel/functions-binarystring.html
    GET_BIT("get_bit", HazelcastDataType.INT, HazelcastDataType.TEXT, HazelcastDataType.INT),
    GET_BYTE("get_byte", HazelcastDataType.INT, HazelcastDataType.TEXT, HazelcastDataType.INT),

    // range functions
    // https://www.postgresql.org/docs/devel/functions-range.html#RANGE-FUNCTIONS-TABLE
//    RANGE_LOWER("lower", HazelcastDataType.INT, HazelcastDataType.RANGE), //
//    RANGE_UPPER("upper", HazelcastDataType.INT, HazelcastDataType.RANGE), //
//    RANGE_ISEMPTY("isempty", HazelcastDataType.BOOLEAN, HazelcastDataType.RANGE), //
//    RANGE_LOWER_INC("lower_inc", HazelcastDataType.BOOLEAN, HazelcastDataType.RANGE), //
//    RANGE_UPPER_INC("upper_inc", HazelcastDataType.BOOLEAN, HazelcastDataType.RANGE), //
//    RANGE_LOWER_INF("lower_inf", HazelcastDataType.BOOLEAN, HazelcastDataType.RANGE), //
//    RANGE_UPPER_INF("upper_inf", HazelcastDataType.BOOLEAN, HazelcastDataType.RANGE), //
//    RANGE_MERGE("range_merge", HazelcastDataType.RANGE, HazelcastDataType.RANGE, HazelcastDataType.RANGE), //

    // https://www.postgresql.org/docs/devel/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE
    GET_COLUMN_SIZE("get_column_size", HazelcastDataType.INT, HazelcastDataType.TEXT);
    // PG_DATABASE_SIZE("pg_database_size", PostgresDataType.INT, PostgresDataType.INT);
    // PG_SIZE_BYTES("pg_size_bytes", PostgresDataType.INT, PostgresDataType.TEXT);

    private String functionName;
    private HazelcastDataType returnType;
    private HazelcastDataType[] argTypes;

    HazelcastFunctionWithUnknownResult(String functionName, HazelcastDataType returnType, HazelcastDataType... indexType) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.argTypes = indexType.clone();

    }

    public boolean isCompatibleWithReturnType(HazelcastDataType t) {
        return t == returnType;
    }

    public HazelcastExpression[] getArguments(HazelcastDataType returnType, HazelcastExpressionGenerator gen, int depth) {
        HazelcastExpression[] args = new HazelcastExpression[argTypes.length];
        for (int i = 0; i < args.length; i++) {
            args[i] = gen.generateExpression(depth, argTypes[i]);
        }
        return args;

    }

    public String getName() {
        return functionName;
    }

    public static List<HazelcastFunctionWithUnknownResult> getSupportedFunctions(HazelcastDataType type) {
        List<HazelcastFunctionWithUnknownResult> functions = new ArrayList<>();
        for (HazelcastFunctionWithUnknownResult func : values()) {
            if (func.isCompatibleWithReturnType(type)) {
                functions.add(func);
            }
        }
        return functions;
    }

}
