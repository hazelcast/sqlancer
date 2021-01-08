package sqlancer.hazelcast.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastSchema.HazelcastColumn;
import sqlancer.hazelcast.HazelcastSchema.HazelcastStatisticsObject;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTable;

import java.util.List;
import java.util.stream.Collectors;

public final class HazelcastStatisticsGenerator {

    private HazelcastStatisticsGenerator() {
    }

    public static SQLQueryAdapter insert(HazelcastGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE STATISTICS ");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS");
        }
        HazelcastTable randomTable = globalState.getSchema().getRandomTable(t -> !t.isView()); // TODO materialized view
        if (randomTable.getColumns().size() < 2) {
            throw new IgnoreMeException();
        }
        sb.append(" ");
        sb.append(getNewStatisticsName(randomTable));
        if (Randomly.getBoolean()) {
            sb.append(" (");
            List<String> statsSubset;
            statsSubset = Randomly.nonEmptySubset("ndistinct", "dependencies", "mcv");
            sb.append(statsSubset.stream().collect(Collectors.joining(", ")));
            sb.append(")");
        }

        List<HazelcastColumn> randomColumns = randomTable.getRandomNonEmptyColumnSubset(
                globalState.getRandomly().getInteger(2, randomTable.getColumns().size()));
        sb.append(" ON ");
        sb.append(randomColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(" FROM ");
        sb.append(randomTable.getName());
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("cannot have more than 8 columns in statistics"),
                true);
    }

    public static SQLQueryAdapter remove(HazelcastGlobalState globalState) {
        StringBuilder sb = new StringBuilder("DROP STATISTICS ");
        HazelcastTable randomTable = globalState.getSchema().getRandomTable();
        List<HazelcastStatisticsObject> statistics = randomTable.getStatistics();
        if (statistics.isEmpty()) {
            throw new IgnoreMeException();
        }
        sb.append(Randomly.fromList(statistics).getName());
        return new SQLQueryAdapter(sb.toString(), true);
    }

    private static String getNewStatisticsName(HazelcastTable randomTable) {
        List<HazelcastStatisticsObject> statistics = randomTable.getStatistics();
        int i = 0;
        while (true) {
            String candidateName = "s" + i;
            if (!statistics.stream().anyMatch(stat -> stat.getName().contentEquals(candidateName))) {
                return candidateName;
            }
            i++;
        }
    }

}
