package sqlancer.hazelcast.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastVisitor;
import sqlancer.hazelcast.ast.HazelcastExpression;
import sqlancer.hazelcast.HazelcastSchema.HazelcastColumn;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTable;

import java.util.List;
import java.util.stream.Collectors;

public final class HazelcastUpdateGenerator {

    private HazelcastUpdateGenerator() {
    }

    public static SQLQueryAdapter create(HazelcastGlobalState globalState) {
        HazelcastTable randomTable = globalState.getSchema().getRandomTable(t -> t.isInsertable());
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(randomTable.getName());
        sb.append(" SET ");
        ExpectedErrors errors = ExpectedErrors.from("conflicting key value violates exclusion constraint",
                "reached maximum value of sequence", "violates foreign key constraint", "violates not-null constraint",
                "violates unique constraint", "out of range", "cannot cast", "must be type boolean", "is not unique",
                " bit string too long", "can only be updated to DEFAULT", "division by zero",
                "You might need to add explicit type casts.", "invalid regular expression",
                "View columns that are not columns of their base relation are not updatable");
        errors.add("multiple assignments to same column"); // view whose columns refer to a column in the referenced
                                                           // table multiple times
        errors.add("new row violates check option for view");
        final String keyColumn = "__key";   // __key column cannot be updated
        List<HazelcastColumn> columns = randomTable.getRandomNonEmptyColumnSubset().stream()
                .filter(column -> !column.getName().equals(keyColumn)).collect(Collectors.toList());
        HazelcastCommon.addCommonInsertUpdateErrors(errors);
        //Skip running UPDATE query if table has only __key column
        if (columns.size() < 1) {
            return new SQLQueryAdapter("", errors, false);
        }

        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            HazelcastColumn column = columns.get(i);
            sb.append(column.getName());
            sb.append(" = ");
//            if (!Randomly.getBoolean()) {
                HazelcastExpression constant = HazelcastExpressionGenerator.generateConstant(globalState.getRandomly(),
                        column.getType());
                sb.append(HazelcastVisitor.asString(constant));
//            } else if (Randomly.getBoolean()) {
//                sb.append("DEFAULT");
//            }
//            } else {
//                sb.append("(");
//                HazelcastExpression expr = HazelcastExpressionGenerator.generateExpression(globalState,
//                        randomTable.getColumns(), column.getType());
//                // caused by casts
//                sb.append(HazelcastVisitor.asString(expr));
//                sb.append(")");
//            }
        }
        errors.add("invalid input syntax for ");
        errors.add("operator does not exist: text = boolean");
        errors.add("violates check constraint");
        errors.add("could not determine which collation to use for string comparison");
        errors.add("but expression is of type");
        HazelcastCommon.addCommonExpressionErrors(errors);
        if (!Randomly.getBooleanWithSmallProbability()) {
            sb.append(" WHERE ");
            HazelcastExpression where = HazelcastExpressionGenerator.generateExpression(globalState,
                    randomTable.getColumns(), HazelcastDataType.BOOLEAN);
            sb.append(HazelcastVisitor.asString(where));
        }

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
