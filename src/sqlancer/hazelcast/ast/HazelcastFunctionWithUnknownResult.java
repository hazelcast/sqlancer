package sqlancer.hazelcast.ast;

import sqlancer.Randomly;
import sqlancer.hazelcast.gen.HazelcastExpressionGenerator;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

import java.util.ArrayList;
import java.util.List;

public enum HazelcastFunctionWithUnknownResult {

//    ABBREV("abbrev", HazelcastDataType.VARCHAR, HazelcastDataType.INET),
//    BROADCAST("broadcast", HazelcastDataType.INET, HazelcastDataType.INET),
//    FAMILY("family", HazelcastDataType.INTEGER, HazelcastDataType.INET),
//    HOSTMASK("hostmask", HazelcastDataType.INET, HazelcastDataType.INET),
//    MASKLEN("masklen", HazelcastDataType.INTEGER, HazelcastDataType.INET),
//    NETMASK("netmask", HazelcastDataType.INET, HazelcastDataType.INET),
//    SET_MASKLEN("set_masklen", HazelcastDataType.INET, HazelcastDataType.INET, HazelcastDataType.INTEGER),
//    TEXT("text", HazelcastDataType.VARCHAR, HazelcastDataType.INET),
//    INET_SAME_FAMILY("inet_same_family", HazelcastDataType.BOOLEAN, HazelcastDataType.INET, HazelcastDataType.INET),

    // https://www.postgresql.org/docs/devel/functions-admin.html#FUNCTIONS-ADMIN-SIGNAL-TABLE
    // PG_RELOAD_CONF("pg_reload_conf", PostgresDataType.BOOLEAN), // too much output
    // PG_ROTATE_LOGFILE("pg_rotate_logfile", PostgresDataType.BOOLEAN), prints warning

    // https://www.postgresql.org/docs/devel/functions-info.html#FUNCTIONS-INFO-SESSION-TABLE
    CURRENT_DATABASE("current_database", HazelcastDataType.VARCHAR), // name
    // CURRENT_QUERY("current_query", PostgresDataType.TEXT), // can generate false positives
    CURRENT_SCHEMA("current_schema", HazelcastDataType.VARCHAR), // name
    // CURRENT_SCHEMAS("current_schemas", PostgresDataType.TEXT, PostgresDataType.BOOLEAN),
    INET_CLIENT_PORT("inet_client_port", HazelcastDataType.INTEGER),
    // INET_SERVER_PORT("inet_server_port", PostgresDataType.INT),
    PG_BACKEND_PID("pg_backend_pid", HazelcastDataType.INTEGER),
    PG_CURRENT_LOGFILE("pg_current_logfile", HazelcastDataType.VARCHAR),
    PG_IS_OTHER_TEMP_SCHEMA("pg_is_other_temp_schema", HazelcastDataType.BOOLEAN),
    PG_JIT_AVAILABLE("pg_jit_available", HazelcastDataType.BOOLEAN),
//    PG_NOTIFICATION_QUEUE_USAGE("pg_notification_queue_usage", HazelcastDataType.REAL),
    PG_TRIGGER_DEPTH("pg_trigger_depth", HazelcastDataType.INTEGER), VERSION("version", HazelcastDataType.VARCHAR),

    //
    TO_CHAR("to_char", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR) {
        @Override
        public HazelcastExpression[] getArguments(HazelcastDataType returnType, HazelcastExpressionGenerator gen,
                                                 int depth) {
            HazelcastExpression[] args = super.getArguments(returnType, gen, depth);
            args[0] = gen.generateExpression(HazelcastDataType.getRandomType());
            return args;
        }
    },

    // String functions
    ASCII("ascii", HazelcastDataType.INTEGER, HazelcastDataType.VARCHAR),
    BTRIM("btrim", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    CHR("chr", HazelcastDataType.VARCHAR, HazelcastDataType.INTEGER),
    CONVERT_FROM("convert_from", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR) {
        @Override
        public HazelcastExpression[] getArguments(HazelcastDataType returnType, HazelcastExpressionGenerator gen,
                                                 int depth) {
            HazelcastExpression[] args = super.getArguments(returnType, gen, depth);
            args[1] = HazelcastConstant.createVarcharConstant(Randomly.fromOptions("UTF8", "LATIN1"));
            return args;
        }
    },
    // concat
    // segfault
    // BIT_LENGTH("bit_length", PostgresDataType.INT, PostgresDataType.TEXT),
    INITCAP("initcap", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    LEFT("left", HazelcastDataType.VARCHAR, HazelcastDataType.INTEGER, HazelcastDataType.VARCHAR),
    LOWER("lower", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    MD5("md5", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    UPPER("upper", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    // PG_CLIENT_ENCODING("pg_client_encoding", PostgresDataType.TEXT),
    QUOTE_LITERAL("quote_literal", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    QUOTE_IDENT("quote_ident", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    REGEX_REPLACE("regex_replace", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    // REPEAT("repeat", PostgresDataType.TEXT, PostgresDataType.TEXT,
    // PostgresDataType.INT),
    REPLACE("replace", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    REVERSE("reverse", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    RIGHT("right", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR, HazelcastDataType.INTEGER),
    RPAD("rpad", HazelcastDataType.VARCHAR, HazelcastDataType.INTEGER, HazelcastDataType.VARCHAR),
    RTRIM("rtrim", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    SPLIT_PART("split_part", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR, HazelcastDataType.INTEGER),
    STRPOS("strpos", HazelcastDataType.INTEGER, HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    SUBSTR("substr", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR, HazelcastDataType.INTEGER, HazelcastDataType.INTEGER),
    TO_ASCII("to_ascii", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    TO_HEX("to_hex", HazelcastDataType.INTEGER, HazelcastDataType.VARCHAR),
    TRANSLATE("translate", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    // mathematical functions
    // https://www.postgresql.org/docs/9.5/functions-math.html
//    ABS("abs", HazelcastDataType.REAL, HazelcastDataType.REAL),
//    CBRT("cbrt", HazelcastDataType.REAL, HazelcastDataType.REAL), CEILING("ceiling", HazelcastDataType.REAL), //
//    DEGREES("degrees", HazelcastDataType.REAL), EXP("exp", HazelcastDataType.REAL), LN("ln", HazelcastDataType.REAL),
//    LOG("log", HazelcastDataType.REAL), LOG2("log", HazelcastDataType.REAL, HazelcastDataType.REAL),
//    PI("pi", HazelcastDataType.REAL), POWER("power", HazelcastDataType.REAL, HazelcastDataType.REAL),
//    TRUNC("trunc", HazelcastDataType.REAL, HazelcastDataType.INTEGER),
//    TRUNC2("trunc", HazelcastDataType.REAL, HazelcastDataType.INTEGER, HazelcastDataType.REAL),
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
    GET_BIT("get_bit", HazelcastDataType.INTEGER, HazelcastDataType.VARCHAR, HazelcastDataType.INTEGER),
    GET_BYTE("get_byte", HazelcastDataType.INTEGER, HazelcastDataType.VARCHAR, HazelcastDataType.INTEGER),

    // range functions
    // https://www.postgresql.org/docs/devel/functions-range.html#RANGE-FUNCTIONS-TABLE
//    RANGE_LOWER("lower", HazelcastDataType.INTEGER, HazelcastDataType.RANGE), //
//    RANGE_UPPER("upper", HazelcastDataType.INTEGER, HazelcastDataType.RANGE), //
//    RANGE_ISEMPTY("isempty", HazelcastDataType.BOOLEAN, HazelcastDataType.RANGE), //
//    RANGE_LOWER_INC("lower_inc", HazelcastDataType.BOOLEAN, HazelcastDataType.RANGE), //
//    RANGE_UPPER_INC("upper_inc", HazelcastDataType.BOOLEAN, HazelcastDataType.RANGE), //
//    RANGE_LOWER_INF("lower_inf", HazelcastDataType.BOOLEAN, HazelcastDataType.RANGE), //
//    RANGE_UPPER_INF("upper_inf", HazelcastDataType.BOOLEAN, HazelcastDataType.RANGE), //
//    RANGE_MERGE("range_merge", HazelcastDataType.RANGE, HazelcastDataType.RANGE, HazelcastDataType.RANGE), //

    // https://www.postgresql.org/docs/devel/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE
    GET_COLUMN_SIZE("get_column_size", HazelcastDataType.INTEGER, HazelcastDataType.VARCHAR);
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
