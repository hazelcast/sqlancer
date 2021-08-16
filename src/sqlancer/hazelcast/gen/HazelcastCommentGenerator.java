package sqlancer.hazelcast.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTable;

/**
 * @see https://www.postgresql.org/docs/devel/sql-comment.html
 */
public final class HazelcastCommentGenerator {

    private HazelcastCommentGenerator() {
    }

    private enum Action {
        INDEX, COLUMN, STATISTICS, TABLE
    }

    public static SQLQueryAdapter generate(HazelcastGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("COMMENT ON ");
        Action type = Randomly.fromOptions(Action.values());
        HazelcastTable randomTable = globalState.getSchema().getRandomTable();
        switch (type) {
        case INDEX:
            sb.append("INDEX ");
            if (randomTable.getIndexes().isEmpty()) {
                throw new IgnoreMeException();
            } else {
                sb.append(randomTable.getRandomIndex().getIndexName());
            }
            break;
        case COLUMN:
            sb.append("COLUMN ");
            sb.append(randomTable.getRandomColumn().getFullQualifiedName());
            break;
        case STATISTICS:
            sb.append("STATISTICS ");
            if (randomTable.getStatistics().isEmpty()) {
                throw new IgnoreMeException();
            } else {
                sb.append(randomTable.getStatistics().get(0).getName());
            }
            break;
        case TABLE:
            sb.append("TABLE ");
            if (randomTable.isView()) {
                throw new IgnoreMeException();
            }
            sb.append(randomTable.getName());
            break;
        default:
            throw new AssertionError(type);
        }
        sb.append(" IS ");
        if (Randomly.getBoolean()) {
            sb.append("NULL");
        } else {
            sb.append("'");
            sb.append(globalState.getRandomly().getString().replace("'", "''"));
            sb.append("'");
        }
        return new SQLQueryAdapter(sb.toString());
    }

}
