package sqlancer.hazelcast.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastSchema;
import sqlancer.hazelcast.HazelcastVisitor;
import sqlancer.hazelcast.ast.HazelcastExpression;
import sqlancer.hazelcast.HazelcastSchema.HazelcastColumn;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class HazelcastInsertGenerator {

    private HazelcastInsertGenerator() {
    }

    public static SQLQueryAdapter insert(HazelcastGlobalState globalState) {
        HazelcastTable table = globalState.getSchema().getRandomTable(t -> t.isInsertable());
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("cannot insert into column");
        HazelcastCommon.addCommonExpressionErrors(errors);
        HazelcastCommon.addCommonInsertUpdateErrors(errors);
        HazelcastCommon.addCommonExpressionErrors(errors);
        errors.add("multiple assignments to same column");
        errors.add("violates foreign key constraint");
        errors.add("value too long for type character varying");
        errors.add("conflicting key value violates exclusion constraint");
        errors.add("violates not-null constraint");
        errors.add("current transaction is aborted");
        errors.add("bit string too long");
        errors.add("new row violates check option for view");
        errors.add("reached maximum value of sequence");
        errors.add("but expression is of type");
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(table.getName());
        List<HazelcastColumn> columns = table.getRandomNonEmptyColumnSubset();
        addKeyColumnIfNotInTheList(columns);
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
//        if (Randomly.getBooleanWithRatherLowProbability()) {
//            sb.append(" OVERRIDING");
//            sb.append(" ");
//            sb.append(Randomly.fromOptions("SYSTEM", "USER"));
//            sb.append(" VALUE");
//        }
        sb.append(" VALUES");

        if (globalState.getDmbsSpecificOptions().allowBulkInsert && Randomly.getBooleanWithSmallProbability()) {
            StringBuilder sbRowValue = new StringBuilder();
            sbRowValue.append("(");
            for (int i = 0; i < columns.size(); i++) {
                if (i != 0) {
                    sbRowValue.append(", ");
                }
                sbRowValue.append(HazelcastVisitor.asString(HazelcastExpressionGenerator
                        .generateConstant(globalState.getRandomly(), columns.get(i).getType())));
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
                insertRow(globalState, sb, columns, n == 1);
            }
        }
//        if (Randomly.getBooleanWithRatherLowProbability()) {
//            sb.append(" ON CONFLICT ");
//            if (Randomly.getBoolean()) {
//                sb.append("(");
//                sb.append(table.getRandomColumn().getName());
//                sb.append(")");
//                errors.add("there is no unique or exclusion constraint matching the ON CONFLICT specification");
//            }
//            sb.append(" DO NOTHING");
//        }
        errors.add("duplicate key value violates unique constraint");
        errors.add("identity column defined as GENERATED ALWAYS");
        errors.add("out of range");
        errors.add("violates check constraint");
        errors.add("no partition of relation");
        errors.add("invalid input syntax");
        errors.add("division by zero");
        errors.add("violates foreign key constraint");
        errors.add("data type unknown");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static void insertRow(HazelcastGlobalState globalState, StringBuilder sb, List<HazelcastColumn> columns,
                                  boolean canBeDefault) {
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            HazelcastExpression generateConstant;
            if (Randomly.getBoolean()) {
                generateConstant = HazelcastExpressionGenerator.generateConstant(globalState.getRandomly(),
                        columns.get(i).getType());
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
     * @param input
     */
    private static void addKeyColumnIfNotInTheList(List<HazelcastColumn> input) {
        if(input.stream().noneMatch(column -> column.getName().equals("__key"))) {
            input.add(new HazelcastColumn("__key", HazelcastSchema.HazelcastDataType.INTEGER));
        }
    }

}
