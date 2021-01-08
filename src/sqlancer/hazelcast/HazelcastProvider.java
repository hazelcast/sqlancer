package sqlancer.hazelcast;

import sqlancer.*;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.hazelcast.gen.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

// EXISTS
// IN
public class HazelcastProvider extends SQLProviderAdapter<HazelcastGlobalState, HazelcastOptions> {

    /**
     * Generate only data types and expressions that are understood by PQS.
     */
    public static boolean generateOnlyKnown;

    protected String entryURL;
    protected String username;
    protected String password;
    protected String entryPath;
    protected String host;
    protected String testURL;
    protected String databaseName;
    protected String createDatabaseCommand;

    public HazelcastProvider() {
        super(HazelcastGlobalState.class, HazelcastOptions.class);
    }

    protected HazelcastProvider(Class<HazelcastGlobalState> globalClass, Class<HazelcastOptions> optionClass) {
        super(globalClass, optionClass);
    }

    public enum Action implements AbstractAction<HazelcastGlobalState> {
//        ANALYZE(HazelcastAnalyzeGenerator::create), //
        ALTER_TABLE(g -> HazelcastAlterTableGenerator.create(g.getSchema().getRandomTable(t -> !t.isView()), g,
                generateOnlyKnown)), //
//        CLUSTER(HazelcastClusterGenerator::create), //
//        COMMIT(g -> {
//            SQLQueryAdapter query;
//            if (Randomly.getBoolean()) {
//                query = new SQLQueryAdapter("COMMIT", true);
//            } else if (Randomly.getBoolean()) {
//                query = HazelcastTransactionGenerator.executeBegin();
//            } else {
//                query = new SQLQueryAdapter("ROLLBACK", true);
//            }
//            return query;
//        }), //
//        CREATE_STATISTICS(HazelcastStatisticsGenerator::insert), //
//        DROP_STATISTICS(HazelcastStatisticsGenerator::remove), //
        DELETE(HazelcastDeleteGenerator::create), //
//        DISCARD(HazelcastDiscardGenerator::create), //
//        DROP_INDEX(HazelcastDropIndexGenerator::create), //
        INSERT(HazelcastInsertGenerator::insert), //
        UPDATE(HazelcastUpdateGenerator::create), //
//        TRUNCATE(HazelcastTruncateGenerator::create), //
//        VACUUM(HazelcastVacuumGenerator::create), //
//        REINDEX(HazelcastReindexGenerator::create), //
//        SET(HazelcastSetGenerator::create), //
//        CREATE_INDEX(HazelcastIndexGenerator::generate), //
//        SET_CONSTRAINTS((g) -> {
//            StringBuilder sb = new StringBuilder();
//            sb.append("SET CONSTRAINTS ALL ");
//            sb.append(Randomly.fromOptions("DEFERRED", "IMMEDIATE"));
//            return new SQLQueryAdapter(sb.toString());
//        }), //
//        RESET_ROLE((g) -> new SQLQueryAdapter("RESET ROLE")), //
//        COMMENT_ON(HazelcastCommentGenerator::generate), //
        RESET((g) -> new SQLQueryAdapter("RESET ALL") /*
                                                       * https://www.postgresql.org/docs/devel/sql-reset.html TODO: also
                                                       * configuration parameter
                                                       */);//
//        NOTIFY(HazelcastNotifyGenerator::createNotify), //
//        LISTEN((g) -> HazelcastNotifyGenerator.createListen()), //
//        UNLISTEN((g) -> HazelcastNotifyGenerator.createUnlisten()), //
//        CREATE_SEQUENCE(HazelcastSequenceGenerator::createSequence), //
//        CREATE_VIEW(HazelcastViewGenerator::create);

        private final SQLQueryProvider<HazelcastGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<HazelcastGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(HazelcastGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    protected static int mapActions(HazelcastGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed;
        switch (a) {
//        case CREATE_INDEX:
//        case CLUSTER:
//            nrPerformed = r.getInteger(0, 3);
//            break;
//        case CREATE_STATISTICS:
//            nrPerformed = r.getInteger(0, 5);
//            break;
//        case DISCARD:
//        case DROP_INDEX:
//            nrPerformed = r.getInteger(0, 5);
//            break;
//        case COMMIT:
//            nrPerformed = r.getInteger(0, 0);
//            break;
        case ALTER_TABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
//        case REINDEX:
        case RESET:
            nrPerformed = r.getInteger(0, 3);
            break;
        case DELETE:
//        case RESET_ROLE:
//        case SET:
//            nrPerformed = r.getInteger(0, 5);
//            break;
//        case ANALYZE:
//            nrPerformed = r.getInteger(0, 3);
//            break;
//        case VACUUM:
//        case SET_CONSTRAINTS:
//        case COMMENT_ON:
//        case NOTIFY:
//        case LISTEN:
//        case UNLISTEN:
//        case CREATE_SEQUENCE:
//        case DROP_STATISTICS:
//        case TRUNCATE:
//            nrPerformed = r.getInteger(0, 2);
//            break;
//        case CREATE_VIEW:
//            nrPerformed = r.getInteger(0, 2);
//            break;
        case UPDATE:
            nrPerformed = r.getInteger(0, 10);
            break;
        case INSERT:
            nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            break;
        default:
            throw new AssertionError(a);
        }
        return nrPerformed;

    }

    @Override
    public void generateDatabase(HazelcastGlobalState globalState) throws Exception {
//        readFunctions(globalState);
        createTables(globalState, Randomly.fromOptions(4, 5, 6));
        prepareTables(globalState);
    }

    @Override
    public SQLConnection createDatabase(HazelcastGlobalState globalState) throws Exception {
        final String CONN_STRING = "jdbc:hazelcast://localhost:5701";
        final String ENTRY_DATABASE_NAME = "hazelcast";
        databaseName = globalState.getDatabaseName();
        Class.forName("com.hazelcast.jdbc.Driver");
        Connection con = DriverManager.getConnection(CONN_STRING);
        return new SQLConnection(con);
    }

    protected void readFunctions(HazelcastGlobalState globalState) throws SQLException {
        SQLQueryAdapter query = new SQLQueryAdapter("SELECT proname, provolatile FROM pg_proc;");
        SQLancerResultSet rs = query.executeAndGet(globalState);
        while (rs.next()) {
            String functionName = rs.getString(1);
            Character functionType = rs.getString(2).charAt(0);
            globalState.addFunctionAndType(functionName, functionType);
        }
    }

    protected void createTables(HazelcastGlobalState globalState, int numTables) throws Exception {
//        HazelcastGlobalState.executeStatement("CREATE MAPPING t0_etOvcRslYx" + System.currentTimeMillis() +
//                " (c0 VARCHAR , c1 VARCHAR , c2 boolean , c3 boolean ) " +
//                "TYPE IMap OPTIONS ( 'keyFormat'='bigint', 'valueFormat'='json');");
        do{
            try {
                String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                SQLQueryAdapter createTable = HazelcastTableGenerator.generate(tableName, globalState.getSchema(),
                        generateOnlyKnown, globalState);
                HazelcastGlobalState.executeStatement(createTable.getQueryString());
                globalState.getManager().incrementCreateQueryCount();
            } catch (IgnoreMeException e) {
                e.printStackTrace();
            }
        } while (globalState.getSchema().getDatabaseTables().size() < numTables);
    }

    protected void prepareTables(HazelcastGlobalState globalState) throws Exception {
        StatementExecutor<HazelcastGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                HazelcastProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
        globalState.executeStatement(new SQLQueryAdapter("COMMIT", true));
        globalState.executeStatement(new SQLQueryAdapter("SET SESSION statement_timeout = 5000;\n"));
    }

    private String getCreateDatabaseCommand(HazelcastGlobalState state) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE DATABASE " + databaseName + " ");
        if (Randomly.getBoolean() && ((HazelcastOptions) state.getDmbsSpecificOptions()).testCollations) {
            if (Randomly.getBoolean()) {
                sb.append("WITH ENCODING '");
                sb.append(Randomly.fromOptions("utf8"));
                sb.append("' ");
            }
            for (String lc : Arrays.asList("LC_COLLATE", "LC_CTYPE")) {
                if (!state.getCollates().isEmpty() && Randomly.getBoolean()) {
                    sb.append(String.format(" %s = '%s'", lc, Randomly.fromList(state.getCollates())));
                }
            }
            sb.append(" TEMPLATE template0");
        }
        return sb.toString();
    }

    @Override
    public String getDBMSName() {
        return "hazelcast";
    }

}
