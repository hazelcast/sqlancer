package sqlancer.hazelcast.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastSchema;
import sqlancer.hazelcast.HazelcastVisitor;
import sqlancer.hazelcast.ast.HazelcastExpression;
import sqlancer.hazelcast.HazelcastSchema.HazelcastColumn;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTable;

import java.util.List;
import java.util.stream.Collectors;

import static sqlancer.Main.QueryManager.incrementInsertQueryTryout;
import static sqlancer.hazelcast.gen.HazelcastExpressionGenerator.*;

public final class HazelcastInsertGenerator {

    private HazelcastInsertGenerator() {
    }

    public static SQLQueryAdapter insert(HazelcastGlobalState globalState) {
        HazelcastTable table = globalState.getSchema(false).getRandomTable(HazelcastTable::isInsertable);

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(table.getName());
        List<HazelcastColumn> columns = table.getRandomNonEmptyColumnSubset();
        addKeyColumnIfNotInTheList(columns);
        sb.append("(");
        sb.append(columns.stream().map(AbstractTableColumn::getName).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES");

        if (globalState.getDmbsSpecificOptions().allowBulkInsert && Randomly.getBooleanWithSmallProbability()) {
            StringBuilder sbRowValue = new StringBuilder();
            sbRowValue.append("(");
            for (int i = 0; i < columns.size(); i++) {
                if (i != 0) {
                    sbRowValue.append(", ");
                }
                sbRowValue.append(HazelcastVisitor.asString(
                        generateConstant(globalState.getRandomly(), columns.get(i).getType()))
                );
            }
            sbRowValue.append(")");

            int n = (int) Randomly.getNotCachedInteger(100, 1000);
            for (int i = 0; i < n; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(sbRowValue);
            }
        } else {
            int n = Randomly.smallNumber() + 1;
            for (int i = 0; i < n; i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                insertRow(globalState, sb, columns);
            }
        }
        incrementInsertQueryTryout();
        return new SQLQueryAdapter(sb.toString(), HazelcastCommon.knownErrors);
    }

    private static void insertRow(HazelcastGlobalState globalState, StringBuilder sb, List<HazelcastColumn> columns) {
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            HazelcastExpression generateConstant;
            if (columns.get(i).getName().equals("__key") || Randomly.getBoolean()) {
                generateConstant = generateConstant(globalState.getRandomly(), columns.get(i).getType());
            } else {
                generateConstant = new HazelcastExpressionGenerator(globalState)
                        .generateExpression(columns.get(i).getType());
            }
            sb.append(HazelcastVisitor.asString(generateConstant));
        }
        sb.append(")");
    }

    /**
     * In any INSERT expression we need to specify __key
     *
     * @param input
     */
    private static void addKeyColumnIfNotInTheList(List<HazelcastColumn> input) {
        if (input.stream().noneMatch(column -> column.getName().equals("__key"))) {
            input.add(0, new HazelcastColumn("__key", HazelcastSchema.HazelcastDataType.INTEGER));
        }
    }

}
