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
    private boolean columnCanHavePrimaryKey;
    private boolean columnHasPrimaryKey;
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
        errors.add("invalid input syntax for");
        errors.add("is not unique");
        errors.add("integer out of range");
        errors.add("division by zero");
        errors.add("cannot create partitioned table as inheritance child");
        errors.add("cannot cast");
        errors.add("ERROR: functions in index expression must be marked IMMUTABLE");
        errors.add("functions in partition key expression must be marked IMMUTABLE");
        errors.add("functions in index predicate must be marked IMMUTABLE");
        errors.add("has no default operator class for access method");
        errors.add("does not exist for access method");
        errors.add("does not accept data type");
        errors.add("but default expression is of type text");
        errors.add("has pseudo-type unknown");
        errors.add("no collation was derived for partition key column");
        errors.add("inherits from generated column but specifies identity");
        errors.add("inherits from generated column but specifies default");
        HazelcastCommon.addCommonExpressionErrors(errors);
        HazelcastCommon.addCommonTableErrors(errors);
    }

    public static SQLQueryAdapter generate(String tableName, HazelcastSchema newSchema, boolean generateOnlyKnown,
                                           HazelcastGlobalState globalState) {
        return new HazelcastTableGenerator(tableName, newSchema, generateOnlyKnown, globalState).generate();
    }

    private SQLQueryAdapter generate() {
        columnCanHavePrimaryKey = true;
        sb.append("CREATE MAPPING");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS");
        }
        sb.append(" ");
        sb.append(tableName);
        sb.append(" ");

        //TODO: Like is not supported
//        if (Randomly.getBoolean() && !newSchema.getDatabaseTables().isEmpty()) {
//            createLike();
//        } else {
//            createStandard();
//        }

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
        if (Randomly.getBoolean()) {
            errors.add("constraints on temporary tables may reference only temporary tables");
            errors.add("constraints on unlogged tables may reference only permanent or unlogged tables");
            errors.add("constraints on permanent tables may reference only permanent tables");
            errors.add("cannot be implemented");
            errors.add("there is no unique constraint matching given keys for referenced table");
            errors.add("cannot reference partitioned table");
            errors.add("unsupported ON COMMIT and foreign key combination");
            errors.add("ERROR: invalid ON DELETE action for foreign key constraint containing generated column");
            errors.add("exclusion constraints are not supported on partitioned tables");
//            HazelcastCommon.addTableConstraints(columnHasPrimaryKey, sb, table, globalState, errors);
        }
        sb.append(")");
        //TODO: Enable PARTITION BY
//        generatePartitionBy();
//        HazelcastCommon.generateWith(sb, globalState, errors);
    }

    private void createLike() {
        sb.append("(");
        sb.append("LIKE ");
        sb.append(newSchema.getRandomTable().getName());
        if (Randomly.getBoolean()) {
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                String option = Randomly.fromOptions("DEFAULTS", "CONSTRAINTS", "INDEXES", "STORAGE", "COMMENTS",
                        "GENERATED", "IDENTITY", "STATISTICS", "STORAGE", "ALL");
                sb.append(" ");
                sb.append(Randomly.fromOptions("INCLUDING", "EXCLUDING"));
                sb.append(" ");
                sb.append(option);
            }
        }
        sb.append(")");
    }

    private void createColumn(String name) throws AssertionError {
        sb.append(name);
        sb.append(" ");
        HazelcastDataType type = HazelcastDataType.getRandomType();
        //TODO: This needs to be able to retrieve collations
        //https://database.guide/how-to-return-a-list-of-available-collations-in-postgresql/
//        List<String> collates  = globalState.getCollates();
        List<String> collates  = null;
        boolean serial = HazelcastCommon.appendDataType(type, sb, false, generateOnlyKnown, collates);
        HazelcastColumn c = new HazelcastColumn(name, type);
        c.setTable(table);
        columnsToBeAdded.add(c);
        sb.append(" ");
//        if (Randomly.getBoolean()) {
//            createColumnConstraint(type, serial);
//        }
    }

    private void generatePartitionBy() {
        if (Randomly.getBoolean()) {
            return;
        }
        sb.append(" PARTITION BY ");
        // TODO "RANGE",
        String partitionOption = Randomly.fromOptions("RANGE", "LIST", "HASH");
        sb.append(partitionOption);
        sb.append("(");
        errors.add("unrecognized parameter");
        errors.add("cannot use constant expression");
        errors.add("cannot add NO INHERIT constraint to partitioned table");
        errors.add("unrecognized parameter");
        errors.add("unsupported PRIMARY KEY constraint with partition key definition");
        errors.add("which is part of the partition key.");
        errors.add("unsupported UNIQUE constraint with partition key definition");
        errors.add("does not accept data type");
        int n = partitionOption.contentEquals("LIST") ? 1 : Randomly.smallNumber() + 1;
        HazelcastCommon.addCommonExpressionErrors(errors);
        for (int i = 0; i < n; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("(");
            HazelcastExpression expr = HazelcastExpressionGenerator.generateExpression(globalState, columnsToBeAdded);
            sb.append(HazelcastVisitor.asString(expr));
            sb.append(")");
            if (Randomly.getBoolean()) {
                sb.append(globalState.getRandomOpclass());
                errors.add("does not exist for access method");
            }
        }
        sb.append(")");
    }

    private void generateInherits() {
        if (Randomly.getBoolean() && !newSchema.getDatabaseTables().isEmpty()) {
            sb.append(" INHERITS(");
            sb.append(newSchema.getDatabaseTablesRandomSubsetNotEmpty().stream().map(t -> t.getName())
                    .collect(Collectors.joining(", ")));
            sb.append(")");
            errors.add("has a type conflict");
            errors.add("has a generation conflict");
            errors.add("cannot create partitioned table as inheritance child");
            errors.add("cannot inherit from temporary relation");
            errors.add("cannot inherit from partitioned table");
            errors.add("has a collation conflict");
        }
    }

    private enum ColumnConstraint {
        NULL_OR_NOT_NULL, UNIQUE, PRIMARY_KEY, DEFAULT, CHECK, GENERATED
    };

    private void createColumnConstraint(HazelcastDataType type, boolean serial) {
        List<ColumnConstraint> constraintSubset = Randomly.nonEmptySubset(ColumnConstraint.values());
        if (Randomly.getBoolean()) {
            // make checks constraints less likely
            constraintSubset.remove(ColumnConstraint.CHECK);
        }
        if (!columnCanHavePrimaryKey || columnHasPrimaryKey) {
            constraintSubset.remove(ColumnConstraint.PRIMARY_KEY);
        }
        if (constraintSubset.contains(ColumnConstraint.GENERATED)
                && constraintSubset.contains(ColumnConstraint.DEFAULT)) {
            // otherwise: ERROR: both default and identity specified for column
            constraintSubset.remove(Randomly.fromOptions(ColumnConstraint.GENERATED, ColumnConstraint.DEFAULT));
        }
        if (constraintSubset.contains(ColumnConstraint.GENERATED) && type != HazelcastDataType.INTEGER) {
            // otherwise: ERROR: identity column type must be smallint, integer, or bigint
            constraintSubset.remove(ColumnConstraint.GENERATED);
        }
        if (serial) {
            constraintSubset.remove(ColumnConstraint.GENERATED);
            constraintSubset.remove(ColumnConstraint.DEFAULT);
            constraintSubset.remove(ColumnConstraint.NULL_OR_NOT_NULL);

        }
        for (ColumnConstraint c : constraintSubset) {
            sb.append(" ");
            switch (c) {
            case NULL_OR_NOT_NULL:
                sb.append(Randomly.fromOptions("NOT NULL", "NULL"));
                break;
            case UNIQUE:
                sb.append("UNIQUE");
                break;
            case PRIMARY_KEY:
                sb.append("PRIMARY KEY");
                columnHasPrimaryKey = true;
                break;
            case DEFAULT:
                sb.append("DEFAULT");
                sb.append(" (");
                sb.append(HazelcastVisitor.asString(HazelcastExpressionGenerator.generateExpression(globalState, type)));
                sb.append(")");
                // CREATE TEMPORARY TABLE t1(c0 smallint DEFAULT ('566963878'));
                errors.add("out of range");
                errors.add("is a generated column");
                break;
            case CHECK:
                sb.append("CHECK (");
                sb.append(HazelcastVisitor.asString(HazelcastExpressionGenerator.generateExpression(globalState,
                        columnsToBeAdded, HazelcastDataType.BOOLEAN)));
                sb.append(")");
                if (Randomly.getBoolean()) {
                    sb.append(" NO INHERIT");
                }
                errors.add("out of range");
                break;
            case GENERATED:
                sb.append("GENERATED ");
                if (Randomly.getBoolean()) {
                    sb.append(" ALWAYS AS (");
                    sb.append(HazelcastVisitor.asString(
                            HazelcastExpressionGenerator.generateExpression(globalState, columnsToBeAdded, type)));
                    sb.append(") STORED");
                    errors.add("A generated column cannot reference another generated column.");
                    errors.add("cannot use generated column in partition key");
                    errors.add("generation expression is not immutable");
                    errors.add("cannot use column reference in DEFAULT expression");
                } else {
                    sb.append(Randomly.fromOptions("ALWAYS", "BY DEFAULT"));
                    sb.append(" AS IDENTITY");
                }
                break;
            default:
                throw new AssertionError(sb);
            }
        }
    }

}
