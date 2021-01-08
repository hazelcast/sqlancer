package sqlancer.hazelcast.oracle.tlp;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastVisitor;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HazelcastTLPWhereOracle extends HazelcastTLPBase {

    public HazelcastTLPWhereOracle(HazelcastGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        whereCheck();
    }

    protected void whereCheck() throws SQLException {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        String originalQueryString = HazelcastVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setOrderByExpressions(Collections.emptyList());
        select.setWhereClause(predicate);
        String firstQueryString = HazelcastVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = HazelcastVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = HazelcastVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, Randomly.getBoolean(), state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }
}
