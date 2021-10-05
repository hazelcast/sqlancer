package sqlancer.hazelcast;

import com.hazelcast.sql.SqlRow;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.*;
import sqlancer.hazelcast.ast.HazelcastConstant;
import sqlancer.hazelcast.ast.HazelcastConstants;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static sqlancer.hazelcast.HazelcastGlobalState.executeStatementSilently;

public class HazelcastSchema extends AbstractSchema<HazelcastGlobalState, HazelcastSchema.HazelcastTable> {

    private final String databaseName;

    public enum HazelcastDataType {
        //        TINYINT,
//        SMALLINT,
        INTEGER,
        BOOLEAN,
        VARCHAR,
        DECIMAL,
        FLOAT,
        DOUBLE;

        public static HazelcastDataType getRandomType() {
            List<HazelcastDataType> dataTypes = new ArrayList<>(Arrays.asList(INTEGER, BOOLEAN, VARCHAR));
            if (HazelcastProvider.generateOnlyKnown) {
//                dataTypes.remove(HazelcastDataType.TINYINT);
//                dataTypes.remove(HazelcastDataType.SMALLINT);
                dataTypes.remove(HazelcastDataType.DECIMAL);
                dataTypes.remove(HazelcastDataType.FLOAT);
                dataTypes.remove(HazelcastDataType.DOUBLE);
            }
            return Randomly.fromList(dataTypes);
        }
    }

    public static class HazelcastColumn extends AbstractTableColumn<HazelcastTable, HazelcastDataType> {

        public HazelcastColumn(String name, HazelcastDataType columnType) {
            super(name, null, columnType);
        }

        public static HazelcastColumn createDummy(String name) {
            return new HazelcastColumn(name, HazelcastDataType.INTEGER);
        }
    }

    public static class HazelcastTables extends AbstractTables<HazelcastTable, HazelcastColumn> {

        public HazelcastTables(List<HazelcastTable> tables) {
            super(tables);
        }

        @Override
        public String tableNamesAsString() {
            return getUsedTables().stream().map(AbstractTable::getName).collect(Collectors.joining(", "));
        }

        @Override
        public String columnNamesAsString(Function<HazelcastColumn, String> function) {
            return getUsedColumns().stream().map(function).collect(Collectors.joining(", "));
        }

        public HazelcastRowValue getRandomRowValue(SQLConnection con) {
            Set<HazelcastTable> nonEmptyMaps = new HashSet<>();

            try (Statement s = con.createStatement()) {
                for (HazelcastTable table : getTables()) {
                    String query = String.format("SELECT COUNT(*) FROM %s", table.getName());

                    ResultSet resultSet = s.executeQuery(query);
                    if (resultSet.next()) {
                        if (resultSet.getLong(1) > 0) {
                            nonEmptyMaps.add(table);
                        }
                    }
                }
            } catch (SQLException e) {
                System.err.println("EXCEPTION DURING RANDOM ROW SELECTION.");
                e.printStackTrace();
            }

            assert !nonEmptyMaps.isEmpty() : "Non-empty maps must exist. Overall maps count : " + getTables().size();

            this.usedTables = new ArrayList<>(nonEmptyMaps);
            this.usedColumns = getColumns()
                    .stream()
                    .filter(c -> nonEmptyMaps.contains(c.getTable()))
                    .collect(toList());

            String randomRow = String.format("SELECT %s FROM %s ORDER BY RAND() LIMIT 1", columnNamesAsString(
                            c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    tableNamesAsString());

            Map<HazelcastColumn, HazelcastConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getUsedColumns().size(); i++) {
                    HazelcastColumn column = getUsedColumns().get(i);
                    String columnName = column.getTable().getName() + column.getName();
                    int columnIndex = randomRowValues.findColumn(columnName) + 1;
                    assert columnIndex == i + 1;
                    HazelcastConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = HazelcastConstants.createNullConstant();
                    } else {
                        switch (column.getType()) {
                            case INTEGER:
                                constant = HazelcastConstants.createIntConstant(randomRowValues.getInt(columnIndex));
                                break;
                            case FLOAT:
                                constant = HazelcastConstants.createFloatConstant(randomRowValues.getFloat(columnIndex));
                                break;
                            case DOUBLE:
                                constant = HazelcastConstants.createDoubleConstant(randomRowValues.getDouble(columnIndex));
                                break;
                            case BOOLEAN:
                                constant = HazelcastConstants.createBooleanConstant(randomRowValues.getBoolean(columnIndex));
                                break;
                            case VARCHAR:
                                constant = HazelcastConstants.createVarcharConstant(randomRowValues.getString(columnIndex));
                                break;
                            default:
                                throw new IgnoreMeException();
                        }
                    }
                    values.put(column, constant);
                }
                if (randomRowValues.next()) throw new AssertionError();
                return new HazelcastRowValue(this, values);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new IgnoreMeException();
            }

        }
    }

    public static HazelcastDataType getColumnType(String typeString) {
        switch (typeString.toLowerCase()) {
            case "tinyint":
//                return HazelcastDataType.TINYINT;
            case "smallint":
//                return HazelcastDataType.SMALLINT;
            case "integer":
            case "bigint": // TODO: support it as a separate type
                return HazelcastDataType.INTEGER;
            case "boolean":
                return HazelcastDataType.BOOLEAN;
            case "varchar":
                return HazelcastDataType.VARCHAR;
            case "numeric":
                return HazelcastDataType.DECIMAL;
            case "double precision":
                return HazelcastDataType.FLOAT;
            default:
                throw new AssertionError(typeString);
        }
    }

    public static class HazelcastRowValue extends AbstractRowValue<HazelcastTables, HazelcastColumn, HazelcastConstant> {
        protected HazelcastRowValue(HazelcastTables tables, Map<HazelcastColumn, HazelcastConstant> values) {
            super(tables, values);
        }

        @Override
        public String getRowValuesAsString() {
            List<HazelcastColumn> columnsToCheck = getTable().getColumns();
            return getRowValuesAsString(columnsToCheck);
        }

        @Override
        public String getRowValuesAsString(List<HazelcastColumn> columnsToCheck) {
            StringBuilder sb = new StringBuilder();
            Map<HazelcastColumn, HazelcastConstant> expectedValues = getValues();
            for (int i = 0; i < columnsToCheck.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                HazelcastConstant expectedColumnValue = expectedValues.get(columnsToCheck.get(i));
                sb.append(expectedColumnValue);
            }
            return sb.toString();
        }

        @Override
        public String asStringGroupedByTables() {
            StringBuilder sb = new StringBuilder();
            List<HazelcastColumn> columnList = new ArrayList<>(getValues().keySet());
            List<AbstractTable<?, ?, ?>> tableList = columnList.stream().map(AbstractTableColumn::getTable).distinct().sorted()
                    .collect(Collectors.toList());
            for (int j = 0; j < tableList.size(); j++) {
                if (j != 0) {
                    sb.append("\n");
                }
                AbstractTable<?, ?, ?> t = tableList.get(j);
                sb.append("-- ").append(t.getName()).append("\n");
                List<HazelcastColumn> columnsForTable = columnList.stream().filter(c -> c.getTable().equals(t))
                        .collect(Collectors.toList());
                for (int i = 0; i < columnsForTable.size(); i++) {
                    if (i != 0) {
                        sb.append("\n");
                    }
                    sb.append("--\t");
                    sb.append(columnsForTable.get(i));
                    sb.append("=");
                    sb.append(getValues().get(columnsForTable.get(i)));
                }
            }
            return sb.toString();
        }
    }

    public static class HazelcastTable
            extends AbstractRelationalTable<HazelcastColumn, HazelcastIndex, HazelcastGlobalState> {
        private final boolean isInsertable;

        public HazelcastTable(String tableName, List<HazelcastColumn> columns,
                              List<HazelcastIndex> indexes, boolean isInsertable) {
            super(tableName, columns, indexes, false);
            this.isInsertable = isInsertable;
        }

        public boolean isInsertable() {
            return isInsertable;
        }
    }

    public static final class HazelcastIndex extends TableIndex {

        private HazelcastIndex(String indexName) {
            super(indexName);
        }

        public static HazelcastIndex create(String indexName) {
            return new HazelcastIndex(indexName);
        }

        @Override
        public String getIndexName() {
            if (super.getIndexName().contentEquals("PRIMARY")) {
                return "`PRIMARY`";
            } else {
                return super.getIndexName();
            }
        }

    }

    public static HazelcastSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        try {
            List<HazelcastTable> databaseTables = new ArrayList<>();

            Iterator<SqlRow> sqlRowIterator = executeStatementSilently("SELECT * FROM information_schema.mappings;").iterator();
            while (sqlRowIterator.hasNext()) {
                SqlRow sqlRow = sqlRowIterator.next();
                String tableName = sqlRow.getObject("table_name");

                List<HazelcastColumn> databaseColumns = getTableColumns(con, tableName);
                List<HazelcastIndex> indexes = new ArrayList<>();
                boolean isInsertable = databaseColumns.size() > 1;
                HazelcastTable t = new HazelcastTable(
                        tableName,
                        databaseColumns,
                        indexes,
                        isInsertable
                );
                for (HazelcastColumn c : databaseColumns) {
                    c.setTable(t);
                }
                databaseTables.add(t);
            }
            return new HazelcastSchema(databaseTables, databaseName);
        } catch (SQLIntegrityConstraintViolationException e) {
            throw new AssertionError(e);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new AssertionError(e);
        }
    }

    protected static List<HazelcastColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<HazelcastColumn> columns = new ArrayList<>();
        Iterator<SqlRow> iterator = executeStatementSilently(
                "SELECT column_name, data_type from information_schema.columns where table_name = '"
                        + tableName + "' ORDER BY column_name").iterator();
        while (iterator.hasNext()) {
            SqlRow sqlRow = iterator.next();
            String columnName = sqlRow.getObject("column_name");
            String dataType = sqlRow.getObject("data_type");
            HazelcastColumn c = new HazelcastColumn(columnName, getColumnType(dataType));
            columns.add(c);
        }
        if (columns.size() > 0 && columns.stream().noneMatch(c -> c.getName().equals("__key"))) {
            columns.add(0, new HazelcastColumn("__key", HazelcastDataType.INTEGER));
        }
        return columns;
    }

    public HazelcastSchema(List<HazelcastTable> databaseTables, String databaseName) {
        super(databaseTables);
        this.databaseName = databaseName;
    }

    public HazelcastTables getRandomTableNonEmptyTables() {
        return new HazelcastTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public String getDatabaseName() {
        return databaseName;
    }
}
