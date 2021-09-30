package sqlancer.hazelcast.gen;

import com.hazelcast.sql.impl.QueryException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

import java.util.List;

public final class HazelcastCommon {

    private HazelcastCommon() {
    }

    public static void fillKnownErrors(ExpectedErrors errors) {
        errors.add("Duplicate key");
        errors.add("/ by zero");
        errors.add("CAST function cannot convert value");
        errors.add("Cannot parse VARCHAR");
        HazelcastCommon.addCommonExpressionErrors(errors);
        HazelcastCommon.addCommonMathExpressionErrors(errors);
    }

    public static void addCommonFetchErrors(ExpectedErrors errors) {
        errors.add("GROUP BY position");
    }

    public static void addCommonTableErrors(ExpectedErrors errors) {
        errors.add("is not commutative"); // exclude
        errors.add("operator requires run-time type coercion"); // exclude
    }

    public static void addCommonExpressionErrors(ExpectedErrors errors) {
        errors.add("Duplicate key");
        errors.add("CAST function cannot convert value of type BIGINT to type BOOLEAN");
        errors.add("Cannot parse VARCHAR to");
        addCommonMathExpressionErrors(errors);
    }


    public static void addCommonMathExpressionErrors(ExpectedErrors errors) {
        errors.add("Numeric overflow while converting");
        errors.add("overflow in '*' operator");
        errors.add("overflow in '+' operator");
        errors.add("Division by zero");
        errors.add("/ by zero");
    }

    public static boolean appendDataType(HazelcastDataType type, StringBuilder sb, boolean allowSerial,
                                         boolean generateOnlyKnown, List<String> opClasses) throws AssertionError {
        boolean serial = false;
        switch (type) {
            case BOOLEAN:
                sb.append("boolean");
                break;
            case INTEGER:
                // TODO: support BIGINT
                sb.append(Randomly.fromOptions("tinyint", "smallint", "integer", "bigint"));
                break;
            case VARCHAR:
                // TODO: support CHAR (without VAR)
                sb.append("VARCHAR");
                //TODO: Support collations
//            if (Randomly.getBoolean() && !HazelcastProvider.generateOnlyKnown) {
//                sb.append(" COLLATE ");
//                sb.append('"');
//                sb.append(Randomly.fromList(opClasses));
//                sb.append('"');
//            }
                break;
            case DECIMAL:
                sb.append("DECIMAL");
                break;
            case FLOAT:
                sb.append("REAL");
                break;
//        case REAL:
//            sb.append("FLOAT");
//            break;
            default:
                throw new AssertionError(type);
        }
        return serial;
    }

    public static void addGroupingErrors(ExpectedErrors errors) {
        errors.add("non-integer constant in GROUP BY"); // TODO
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("is not in select list");
        errors.add("aggregate functions are not allowed in GROUP BY");
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
