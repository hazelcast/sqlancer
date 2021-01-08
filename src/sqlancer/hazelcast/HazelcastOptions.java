package sqlancer.hazelcast;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.hazelcast.oracle.HazelcastNoRECOracle;
import sqlancer.hazelcast.oracle.HazelcastPivotedQuerySynthesisOracle;
import sqlancer.hazelcast.oracle.tlp.HazelcastTLPAggregateOracle;
import sqlancer.hazelcast.oracle.tlp.HazelcastTLPHavingOracle;
import sqlancer.hazelcast.oracle.tlp.HazelcastTLPWhereOracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Parameters
public class HazelcastOptions implements DBMSSpecificOptions<HazelcastOptions.HazelcastOracleFactory> {

    @Parameter(names = "--bulk-insert", description = "Specifies whether INSERT statements should be issued in bulk", arity = 1)
    public boolean allowBulkInsert;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for PostgreSQL")
    public List<HazelcastOracleFactory> oracle = Arrays.asList(HazelcastOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--test-collations", description = "Specifies whether to test different collations", arity = 1)
    public boolean testCollations = true;

    @Parameter(names = "--connection-url", description = "Specifies the URL for connecting to the PostgreSQL server", arity = 1)
    public String connectionURL = "postgresql://localhost:5432/test";

    public enum HazelcastOracleFactory implements OracleFactory<HazelcastGlobalState> {
        NOREC {
            @Override
            public TestOracle create(HazelcastGlobalState globalState) throws SQLException {
                return new HazelcastNoRECOracle(globalState);
            }
        },
        PQS {
            @Override
            public TestOracle create(HazelcastGlobalState globalState) throws SQLException {
                return new HazelcastPivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        },
        HAVING {

            @Override
            public TestOracle create(HazelcastGlobalState globalState) throws SQLException {
                return new HazelcastTLPHavingOracle(globalState);
            }

        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(HazelcastGlobalState globalState) throws SQLException {
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new HazelcastTLPWhereOracle(globalState));
                oracles.add(new HazelcastTLPHavingOracle(globalState));
                oracles.add(new HazelcastTLPAggregateOracle(globalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        };

    }

    @Override
    public List<HazelcastOracleFactory> getTestOracleFactory() {
        return oracle;
    }

}
