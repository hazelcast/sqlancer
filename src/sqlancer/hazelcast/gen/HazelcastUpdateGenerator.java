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

import static sqlancer.hazelcast.gen.HazelcastExpressionGenerator.*;

public final class HazelcastUpdateGenerator {

    private HazelcastUpdateGenerator() {
    }

    public static SQLQueryAdapter create(HazelcastGlobalState globalState) {
        HazelcastTable randomTable = globalState.getSchema().getRandomTable(t -> t.isInsertable());
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(randomTable.getName());
        sb.append(" SET ");
        final String keyColumn = "__key";   // __key column cannot be updated
        List<HazelcastColumn> columns = randomTable.getRandomNonEmptyColumnSubset().stream()
                .filter(column -> !column.getName().equals(keyColumn)).collect(Collectors.toList());
        //Skip running UPDATE query if table has only __key column
        if (columns.size() < 1) {
            return new SQLQueryAdapter("", HazelcastCommon.knownErrors, false);
        }

        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            HazelcastColumn column = columns.get(i);
            sb.append(column.getName());
            sb.append(" = ");
            HazelcastExpression constant = generateConstant(globalState.getRandomly(), column.getType());
            sb.append(HazelcastVisitor.asString(constant));
        }
        if (!Randomly.getBooleanWithSmallProbability()) {
            sb.append(" WHERE ");
            HazelcastExpression where = generateExpression(globalState,
                    randomTable.getColumns(), HazelcastDataType.BOOLEAN);
            sb.append(HazelcastVisitor.asString(where));
        }

        return new SQLQueryAdapter(sb.toString(), HazelcastCommon.knownErrors, true);
    }

}
