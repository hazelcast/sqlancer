package sqlancer.hazelcast.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTable;

import java.util.stream.Collectors;

public final class HazelcastAnalyzeGenerator {

    private HazelcastAnalyzeGenerator() {
    }

    public static SQLQueryAdapter create(HazelcastGlobalState globalState) {
        HazelcastTable table = globalState.getSchema().getRandomTable();
        StringBuilder sb = new StringBuilder("ANALYZE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(table.getName());
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
                        .collect(Collectors.joining(", ")));
                sb.append(")");
            }
        }
        // FIXME: bug in postgres?
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("deadlock"));
    }

}
