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

        StringBuilder sb = new StringBuilder("DELETE FROM");
        sb.append(" ");
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(HazelcastVisitor.asString(HazelcastExpressionGenerator.generateExpression(globalState,
                    table.getColumns(), HazelcastDataType.BOOLEAN)));
        }
        return new SQLQueryAdapter(sb.toString(), HazelcastCommon.knownErrors);
    }

}
