package sqlancer.hazelcast.oracle.tlp;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastVisitor;
import sqlancer.hazelcast.ast.HazelcastExpression;
import sqlancer.hazelcast.gen.HazelcastCommon;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class HazelcastTLPHavingOracle extends HazelcastTLPBase {

    public HazelcastTLPHavingOracle(HazelcastGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        havingCheck();
    }

    protected void havingCheck() throws SQLException {
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(HazelcastDataType.BOOLEAN));
        }
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        select.setHavingClause(null);
        String originalQueryString = HazelcastVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        select.setHavingClause(predicate);
        String firstQueryString = HazelcastVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = HazelcastVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = HazelcastVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    protected HazelcastExpression generatePredicate() {
        return gen.generateHavingClause();
    }

    @Override
    List<HazelcastExpression> generateFetchColumns() {
        List<HazelcastExpression> expressions = gen.allowAggregates(true)
                .generateExpressions(Randomly.smallNumber() + 1);
        gen.allowAggregates(false);
        return expressions;
    }

}
