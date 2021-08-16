package sqlancer.hazelcast.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastVisitor;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTable;

public final class HazelcastDeleteGenerator {

    private HazelcastDeleteGenerator() {
    }

    public static SQLQueryAdapter create(HazelcastGlobalState globalState) {
        HazelcastTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("violates foreign key constraint");
        errors.add("violates not-null constraint");
        errors.add("could not determine which collation to use for string comparison");
        StringBuilder sb = new StringBuilder("DELETE FROM");
//        if (Randomly.getBoolean()) {
//            sb.append(" ONLY");
//        }
        sb.append(" ");
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(HazelcastVisitor.asString(HazelcastExpressionGenerator.generateExpression(globalState,
                    table.getColumns(), HazelcastDataType.BOOLEAN)));
        }
        if (Randomly.getBoolean()) {
            sb.append(" RETURNING ");
            sb.append(HazelcastVisitor
                    .asString(HazelcastExpressionGenerator.generateExpression(globalState, table.getColumns())));
        }
        HazelcastCommon.addCommonExpressionErrors(errors);
        errors.add("out of range");
        errors.add("cannot cast");
        errors.add("invalid input syntax for");
        errors.add("division by zero");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
