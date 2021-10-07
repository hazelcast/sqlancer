package sqlancer.hazelcast;

import sqlancer.*;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
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
        INSERT(HazelcastInsertGenerator::insert), //
        DELETE(HazelcastDeleteGenerator::create), //
        UPDATE(HazelcastUpdateGenerator::create); //

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
        int nrPerformed;
        switch (a) {
            case DELETE:
            case UPDATE:
                nrPerformed = globalState.getOptions().getMaxNumberUpdates();
                break;
            case INSERT:
                nrPerformed = globalState.getOptions().getMaxNumberInserts();
                break;
            default:
                throw new AssertionError(a);
        }
        return nrPerformed;

    }

    @Override
    public void generateDatabase(HazelcastGlobalState globalState) throws Exception {
        createTables(globalState, Randomly.fromOptions(10, 12, 14));
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

    protected void createTables(HazelcastGlobalState globalState, int numTables) throws Exception {
        do {
            try {
                String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                SQLQueryAdapter createTable = HazelcastTableGenerator.generate(tableName, globalState.getSchema(),
                        generateOnlyKnown, globalState);
                System.out.println(createTable.getQueryString());
                HazelcastGlobalState.executeStatement(createTable.getQueryString());
                globalState.getManager().incrementCreateQueryCount();
            } catch (IgnoreMeException e) {
                System.err.println("UNEXPECTED IgnoreMeException DURING CREATE MAPPING STATEMENT EXECUTION.");
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
    }

    @Override
    public String getDBMSName() {
        return "hazelcast";
    }

}
