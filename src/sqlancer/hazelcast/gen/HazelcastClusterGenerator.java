package sqlancer.hazelcast.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTable;

public final class HazelcastClusterGenerator {

    private HazelcastClusterGenerator() {
    }

    public static SQLQueryAdapter create(HazelcastGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("there is no previously clustered index for table");
        errors.add("cannot cluster a partitioned table");
        errors.add("access method does not support clustering");
        StringBuilder sb = new StringBuilder("CLUSTER ");
        if (Randomly.getBoolean()) {
            HazelcastTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
            sb.append(table.getName());
            if (Randomly.getBoolean() && !table.getIndexes().isEmpty()) {
                sb.append(" USING ");
                sb.append(table.getRandomIndex().getIndexName());
                errors.add("cannot cluster on partial index");
            }
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
