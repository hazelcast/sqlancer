package sqlancer.hazelcast.ast;

import sqlancer.Randomly;
import sqlancer.hazelcast.gen.HazelcastExpressionGenerator;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

import java.util.ArrayList;
import java.util.List;

public enum HazelcastFunctionWithUnknownResult {

    // String functions : https://docs.hazelcast.com/hazelcast/5.0/sql/expressions#string-functions
    ASCII("ascii", HazelcastDataType.INTEGER, HazelcastDataType.VARCHAR),
    BTRIM("btrim", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    INITCAP("initcap", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    LOWER("lower", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    UPPER("upper", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
//    POSITION("position", HazelcastDataType.INTEGER, HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
//    REPEAT("repeat", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR, HazelcastDataType.INTEGER),
    REPLACE("replace", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
//    REVERSE("reverse", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
    RTRIM("rtrim", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR),
//    SUBSTRING("substring", HazelcastDataType.VARCHAR, HazelcastDataType.VARCHAR, HazelcastDataType.INTEGER, HazelcastDataType.INTEGER),

    // mathematical functions : https://docs.hazelcast.com/hazelcast/5.0/sql/expressions#mathematical-functions
    ABS("abs", HazelcastDataType.INTEGER, HazelcastDataType.INTEGER),
    SIGN("sign", HazelcastDataType.INTEGER, HazelcastDataType.INTEGER),
    //    RAND("abs", HazelcastDataType.INTEGER),
    CBRT("cbrt", HazelcastDataType.DOUBLE, HazelcastDataType.DOUBLE),
    SQRT("sqrt", HazelcastDataType.DOUBLE, HazelcastDataType.DOUBLE),
    CEIL("ceil", HazelcastDataType.DOUBLE, HazelcastDataType.DOUBLE), //
    EXP("exp", HazelcastDataType.DOUBLE), //
    TRUNCATE("truncate", HazelcastDataType.DOUBLE, HazelcastDataType.INTEGER),
    LOG("ln", HazelcastDataType.DOUBLE),
    LOG10("log10", HazelcastDataType.DOUBLE, HazelcastDataType.DOUBLE),
    FLOOR("floor", HazelcastDataType.DOUBLE),

    // trigonometric functions : https://docs.hazelcast.com/hazelcast/5.0/sql/expressions#trigonometric-functions
    ACOS("acos", HazelcastDataType.DOUBLE, HazelcastDataType.DOUBLE), //
    ASIN("asin", HazelcastDataType.DOUBLE, HazelcastDataType.DOUBLE), //
    ATAN("atan", HazelcastDataType.DOUBLE, HazelcastDataType.DOUBLE), //
    ATAN2("atan2", HazelcastDataType.DOUBLE, HazelcastDataType.DOUBLE, HazelcastDataType.DOUBLE), //
    COS("cos", HazelcastDataType.DOUBLE, HazelcastDataType.DOUBLE), //
    COT("cot", HazelcastDataType.DOUBLE, HazelcastDataType.DOUBLE), //
    SIN("sin", HazelcastDataType.DOUBLE, HazelcastDataType.DOUBLE), //
    TAN("tan", HazelcastDataType.DOUBLE, HazelcastDataType.DOUBLE);

    private String functionName;
    private HazelcastDataType returnType;
    private HazelcastDataType[] argTypes;

    HazelcastFunctionWithUnknownResult(String functionName, HazelcastDataType returnType, HazelcastDataType... argTypes) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.argTypes = argTypes.clone();

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
