package sqlancer.hazelcast.gen;

import com.hazelcast.sql.impl.QueryException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

import java.util.List;

public final class HazelcastCommon {

    public static ExpectedErrors knownErrors;

    static {
        knownErrors = new ExpectedErrors();
        fillKnownErrors(knownErrors);
        addCommonFetchErrors(knownErrors);
        addCommonExpressionErrors(knownErrors);
        addCommonMathExpressionErrors(knownErrors);
        addGroupingErrors(knownErrors);
    }

    private HazelcastCommon() {
    }

    private static void fillKnownErrors(ExpectedErrors errors) {
        errors.add("Duplicate key");
        errors.add("/ by zero");
        errors.add("Division by zero");
        errors.add("CAST function cannot convert value");
        errors.add("Cannot parse VARCHAR");
        errors.add("SUBSTRING \"start\" operand must be positive");
        HazelcastCommon.addCommonExpressionErrors(errors);
        HazelcastCommon.addCommonMathExpressionErrors(errors);
    }

    private static void addCommonFetchErrors(ExpectedErrors errors) {
        errors.add("GROUP BY position");
    }

    private static void addCommonExpressionErrors(ExpectedErrors errors) {
        errors.add("Duplicate key");
        errors.add("CAST function cannot convert value of type BIGINT to type BOOLEAN");
        errors.add("Cannot parse VARCHAR to");
        addCommonMathExpressionErrors(errors);
    }


    private static void addCommonMathExpressionErrors(ExpectedErrors errors) {
        errors.add("Numeric overflow while converting");
        errors.add("BIGINT overflow");
        errors.add("overflow in '*' operator");
        errors.add("overflow in '+' operator");
        errors.add("Division by zero");
        errors.add("/ by zero");
    }

    private static void addGroupingErrors(ExpectedErrors errors) {
        errors.add("non-integer constant in GROUP BY"); // TODO
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("is not in select list");
        errors.add("aggregate functions are not allowed in GROUP BY");
    }

    public static boolean appendDataType(HazelcastDataType type, StringBuilder sb, boolean allowSerial,
                                         boolean generateOnlyKnown, List<String> opClasses) throws AssertionError {
        boolean serial = false;
        switch (type) {
            case BOOLEAN:
                sb.append("BOOLEAN");
                break;
            case INTEGER:
                // TODO: support BIGINT
                sb.append(Randomly.fromOptions("INTEGER", "BIGINT"));
                break;
            case VARCHAR:
                // TODO: support CHAR (without VAR)
                sb.append("VARCHAR");
                break;
            case DECIMAL:
                sb.append("DECIMAL");
                break;
            case FLOAT:
                sb.append("FLOAT");
                break;
            default:
                throw new AssertionError(type);
        }
        return serial;
    }
    public static Throwable findRootCause(Throwable e) {
        while (e.getCause() != null) {
            if (e instanceof QueryException) {
                return e;
            }
            e = e.getCause();
        }
        return e;
    }
}
