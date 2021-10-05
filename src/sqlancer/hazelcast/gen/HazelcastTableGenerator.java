package sqlancer.hazelcast.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastSchema;
import sqlancer.hazelcast.HazelcastVisitor;
import sqlancer.hazelcast.ast.HazelcastExpression;
import sqlancer.hazelcast.HazelcastSchema.HazelcastColumn;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class HazelcastTableGenerator {

    private final String tableName;
    private final StringBuilder sb = new StringBuilder();
    private final HazelcastSchema newSchema;
    private final List<HazelcastColumn> columnsToBeAdded = new ArrayList<>();
    protected final ExpectedErrors errors = new ExpectedErrors();
    private final HazelcastTable table;
    private final boolean generateOnlyKnown;
    private final HazelcastGlobalState globalState;

    public HazelcastTableGenerator(String tableName, HazelcastSchema newSchema, boolean generateOnlyKnown,
                                   HazelcastGlobalState globalState) {
        this.tableName = tableName;
        this.newSchema = newSchema;
        this.generateOnlyKnown = generateOnlyKnown;
        this.globalState = globalState;
        table = new HazelcastTable(tableName, columnsToBeAdded, null, false);
        errors.add("Division by zero");
        errors.add("BIGINT overflow");
        errors.add("Duplicate key");
        errors.add("CAST function cannot convert value of type INTEGER to type BOOLEAN");
        HazelcastCommon.addCommonExpressionErrors(errors);
        HazelcastCommon.addCommonTableErrors(errors);
    }

    public static SQLQueryAdapter generate(String tableName, HazelcastSchema newSchema, boolean generateOnlyKnown,
                                           HazelcastGlobalState globalState) {
        return new HazelcastTableGenerator(tableName, newSchema, generateOnlyKnown, globalState).generate();
    }

    private SQLQueryAdapter generate() {
        sb.append("CREATE MAPPING");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS");
        }
        sb.append(" ");
        sb.append(tableName);
        sb.append(" ");

        createStandard();

        sb.append(" ");

        addType();
        addOptions();

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private void addType() {
        sb.append("TYPE IMap");
        //TODO: Add other: Kafka, File
        sb.append(" ");
    }

    private void addOptions() {
        //TODO: Add options for other TYPEs
        sb.append("OPTIONS ( " +
                "'keyFormat'='bigint', " +
                "'valueFormat'='json-flat')");
    }

    private void createStandard() throws AssertionError {
        sb.append("(");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String name = DBMSCommon.createColumnName(i);
            createColumn(name);
        }
        sb.append(")");
    }

    private void createColumn(String name) throws AssertionError {
        sb.append(name);
        sb.append(" ");
        HazelcastDataType type = HazelcastDataType.getRandomType();
        List<String> collates = null;
        boolean serial = HazelcastCommon.appendDataType(type, sb, false, generateOnlyKnown, collates);
        HazelcastColumn c = new HazelcastColumn(name, type);
        c.setTable(table);
        columnsToBeAdded.add(c);
        sb.append(" ");
    }

    ;
}
