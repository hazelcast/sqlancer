package sqlancer.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.*;
import sqlancer.hazelcast.ast.HazelcastConstant;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.*;

public class HazelcastSchema extends AbstractSchema<HazelcastGlobalState, HazelcastSchema.HazelcastTable> {

    private final String databaseName;

    public enum HazelcastDataType {
        INT,
        SMALLINT,
        BOOLEAN,
        TEXT,
        DECIMAL,
        FLOAT,
        REAL,
        RANGE,
        BIT,
        INET;

//        TODO: Add more data types
        public static HazelcastDataType getRandomType() {
            List<HazelcastDataType> dataTypes = new ArrayList<>(Arrays.asList(INT, BOOLEAN, TEXT));
//            if (HazelcastProvider.generateOnlyKnown) {
//                dataTypes.remove(HazelcastDataType.DECIMAL);
//                dataTypes.remove(HazelcastDataType.FLOAT);
//                dataTypes.remove(HazelcastDataType.REAL);
//                dataTypes.remove(HazelcastDataType.INET);
//                dataTypes.remove(HazelcastDataType.RANGE);
//                dataTypes.remove(HazelcastDataType.BIT);
//            }
            return Randomly.fromList(dataTypes);
        }
    }

    public static class HazelcastColumn extends AbstractTableColumn<HazelcastTable, HazelcastDataType> {

        public HazelcastColumn(String name, HazelcastDataType columnType) {
            super(name, null, columnType);
        }

        public static HazelcastColumn createDummy(String name) {
            return new HazelcastColumn(name, HazelcastDataType.INT);
        }

    }

    public static class HazelcastTables extends AbstractTables<HazelcastTable, HazelcastColumn> {

        public HazelcastTables(List<HazelcastTable> tables) {
            super(tables);
        }

        public HazelcastRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String randomRow = String.format("SELECT %s FROM %s ORDER BY RANDOM() LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    // columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." +
                    // c.getName() + ")")
                    tableNamesAsString());
            Map<HazelcastColumn, HazelcastConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    HazelcastColumn column = getColumns().get(i);
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    HazelcastConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = HazelcastConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                            case INT:
                                constant = HazelcastConstant.createIntConstant(randomRowValues.getLong(columnIndex));
                                break;
                            case BOOLEAN:
                                constant = HazelcastConstant.createBooleanConstant(randomRowValues.getBoolean(columnIndex));
                                break;
                            case TEXT:
                                constant = HazelcastConstant.createTextConstant(randomRowValues.getString(columnIndex));
                                break;
                            default:
                                throw new IgnoreMeException();
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new HazelcastRowValue(this, values);
            } catch (SQLException e) {
                throw new IgnoreMeException();
            }

        }

    }

    public static HazelcastDataType getColumnType(String typeString) {
        switch (typeString.toLowerCase()) {
            case "smallint":
                return HazelcastDataType.SMALLINT;
            case "integer":
            case "bigint":
                return HazelcastDataType.INT;
            case "boolean":
                return HazelcastDataType.BOOLEAN;
            case "text":
            case "character":
            case "character varying":
            case "name":
            case "varchar":
                return HazelcastDataType.TEXT;
            case "numeric":
                return HazelcastDataType.DECIMAL;
            case "double precision":
                return HazelcastDataType.FLOAT;
//            case "real":
//                return HazelcastDataType.REAL;
//            case "int4range":
//                return HazelcastDataType.RANGE;
//            case "bit":
//            case "bit varying":
//                return HazelcastDataType.BIT;
//            case "inet":
//                return HazelcastDataType.INET;
            default:
                throw new AssertionError(typeString);
        }
    }

    public static class HazelcastRowValue extends AbstractRowValue<HazelcastTables, HazelcastColumn, HazelcastConstant> {

        protected HazelcastRowValue(HazelcastTables tables, Map<HazelcastColumn, HazelcastConstant> values) {
            super(tables, values);
        }

    }

    public static class HazelcastTable
            extends AbstractRelationalTable<HazelcastColumn, HazelcastIndex, HazelcastGlobalState> {

        public enum TableType {
            STANDARD, TEMPORARY
        }

        private final TableType tableType;
        private final List<HazelcastStatisticsObject> statistics;
        private final boolean isInsertable;

        public HazelcastTable(String tableName, List<HazelcastColumn> columns, List<HazelcastIndex> indexes,
                              TableType tableType, List<HazelcastStatisticsObject> statistics, boolean isView, boolean isInsertable) {
            super(tableName, columns, indexes, isView);
            this.statistics = statistics;
            this.isInsertable = isInsertable;
            this.tableType = tableType;
        }

        public List<HazelcastStatisticsObject> getStatistics() {
            return statistics;
        }

        public TableType getTableType() {
            return tableType;
        }

        public boolean isInsertable() {
            return isInsertable;
        }

    }

    public static final class HazelcastStatisticsObject {
        private final String name;

        public HazelcastStatisticsObject(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
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
//            int mappings = 0;
//            try {
//                Iterator<SqlRow> iterator = HazelcastGlobalState.executeStatementSilently("SELECT * FROM information_schema.mappings;").iterator();
//                while(iterator.hasNext()) {
//                    SqlRow sqlRow = iterator.next();
//                    mappings++;
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            System.out.println("Current created mappings: " + mappings);

            Iterator<SqlRow> sqlRowIterator = HazelcastGlobalState.executeStatementSilently("SELECT * FROM information_schema.mappings;").iterator();
            while (sqlRowIterator.hasNext()) {
                SqlRow sqlRow = sqlRowIterator.next();
                String tableName = sqlRow.getObject("table_name");
                String tableTypeSchema = sqlRow.getObject("table_schema");
                //TODO: Temporarily hardcoded to 'true'. Find out how to get proper value.
                boolean isView = tableName.startsWith("v"); // tableTypeStr.contains("VIEW") ||
                // tableTypeStr.contains("LOCAL TEMPORARY") &&
                // !isInsertable;
                HazelcastTable.TableType tableType = getTableType(tableTypeSchema);
                List<HazelcastColumn> databaseColumns = getTableColumns(con, tableName);
//                        List<HazelcastIndex> indexes = getIndexes(con, tableName);
                List<HazelcastIndex> indexes = new ArrayList<>();
//                        List<HazelcastStatisticsObject> statistics = getStatistics(con);
                boolean isInsertable = databaseColumns.size() > 1;
                List<HazelcastStatisticsObject> statistics = new ArrayList<>();
                HazelcastTable t = new HazelcastTable(tableName, databaseColumns, indexes, tableType, statistics,
                        isView, isInsertable);
                for (HazelcastColumn c : databaseColumns) {
                    c.setTable(t);
                }
                databaseTables.add(t);
            }
//            try (Statement s = con.createStatement()) {
//                try (ResultSet rs = s.executeQuery(
//                     "SELECT * FROM information_schema.mappings;")
//                ) {
//                    while (rs.next()) {
//                        String tableName = rs.getString("mapping_name");
//                        String tableTypeSchema = rs.getString("mapping_schema");
//                        //TODO: Temporarily hardcoded to 'true'. Find out how to get proper value.
//                        boolean isInsertable = true;
//                        boolean isView = tableName.startsWith("v"); // tableTypeStr.contains("VIEW") ||
//                        // tableTypeStr.contains("LOCAL TEMPORARY") &&
//                        // !isInsertable;
//                        HazelcastTable.TableType tableType = getTableType(tableTypeSchema);
//                        List<HazelcastColumn> databaseColumns = getTableColumns(con, tableName);
////                        List<HazelcastIndex> indexes = getIndexes(con, tableName);
//                        List<HazelcastIndex> indexes = new ArrayList<>();
////                        List<HazelcastStatisticsObject> statistics = getStatistics(con);
//                        List<HazelcastStatisticsObject> statistics = new ArrayList<>();
//                        HazelcastTable t = new HazelcastTable(tableName, databaseColumns, indexes, tableType, statistics,
//                                isView, isInsertable);
//                        for (HazelcastColumn c : databaseColumns) {
//                            c.setTable(t);
//                        }
//                        databaseTables.add(t);
//                    }
//                }
//            }
            return new HazelcastSchema(databaseTables, databaseName);
        } catch (SQLIntegrityConstraintViolationException e) {
            throw new AssertionError(e);
        }
    }

    protected static List<HazelcastStatisticsObject> getStatistics(SQLConnection con) throws SQLException {
        List<HazelcastStatisticsObject> statistics = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT stxname FROM pg_statistic_ext ORDER BY stxname;")) {
                while (rs.next()) {
                    statistics.add(new HazelcastStatisticsObject(rs.getString("stxname")));
                }
            }
        }
        return statistics;
    }

    protected static HazelcastTable.TableType getTableType(String tableTypeStr) throws AssertionError {
        HazelcastTable.TableType tableType;
        if (tableTypeStr.contentEquals("public")) {
            tableType = HazelcastTable.TableType.STANDARD;
        } else if (tableTypeStr.startsWith("pg_temp")) {
            tableType = HazelcastTable.TableType.TEMPORARY;
        } else {
            throw new AssertionError(tableTypeStr);
        }
        return tableType;
    }

    protected static List<HazelcastIndex> getIndexes(SQLConnection con, String tableName) throws SQLException {
        List<HazelcastIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String
                    .format("SELECT indexname FROM pg_indexes WHERE tablename='%s' ORDER BY indexname;", tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("indexname");
                    if (DBMSCommon.matchesIndexName(indexName)) {
                        indexes.add(HazelcastIndex.create(indexName));
                    }
                }
            }
        }
        return indexes;
    }

    protected static List<HazelcastColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<HazelcastColumn> columns = new ArrayList<>();
        Iterator<SqlRow> iterator = HazelcastGlobalState.executeStatementSilently("select column_name, data_type from information_schema.columns where table_name = '"
                + tableName + "' ORDER BY column_name").iterator();
        while (iterator.hasNext()) {
            SqlRow sqlRow = iterator.next();
            String columnName = sqlRow.getObject("column_name");
            String dataType = sqlRow.getObject("data_type");
            HazelcastColumn c = new HazelcastColumn(columnName, getColumnType(dataType));
            columns.add(c);
        }

//        try (Statement s = con.createStatement()) {
//            try (ResultSet rs = s
//                    .executeQuery("select column_name, data_type from information_schema.columns where table_name = '"
//                            + tableName + "' ORDER BY column_name")) {
//                while (rs.next()) {
//                    String columnName = rs.getString("table_name");
//                    String dataType = rs.getString("data_type");
//                    HazelcastColumn c = new HazelcastColumn(columnName, getColumnType(dataType));
//                    columns.add(c);
//                }
//            }
//        }
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
