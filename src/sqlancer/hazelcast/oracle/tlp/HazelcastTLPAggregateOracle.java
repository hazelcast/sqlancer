package sqlancer.hazelcast.oracle.tlp;

import org.postgresql.util.PSQLException;
import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastVisitor;
import sqlancer.hazelcast.ast.*;
import sqlancer.hazelcast.gen.HazelcastCommon;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.ast.HazelcastAggregate.HazelcastAggregateFunction;
import sqlancer.hazelcast.ast.HazelcastPostfixOperation.PostfixOperator;
import sqlancer.hazelcast.ast.HazelcastPrefixOperation.PrefixOperator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HazelcastTLPAggregateOracle extends HazelcastTLPBase implements TestOracle {

    private String firstResult;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    public HazelcastTLPAggregateOracle(HazelcastGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        aggregateCheck();
    }

    protected void aggregateCheck() throws SQLException {
        HazelcastAggregateFunction aggregateFunction = Randomly.fromOptions(
                HazelcastAggregateFunction.MAX,
                HazelcastAggregateFunction.MIN,
                HazelcastAggregateFunction.SUM,
                HazelcastAggregateFunction.COUNT
        );
        HazelcastAggregate aggregate = gen.generateArgsForAggregate(aggregateFunction.getRandomReturnType(),
                aggregateFunction);
        List<HazelcastExpression> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add(gen.generateAggregate());
        }
        select.setFetchColumns(Arrays.asList(aggregate));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        originalQuery = HazelcastVisitor.asString(select);
        firstResult = getAggregateResult(originalQuery);
        metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        secondResult = getAggregateResult(metamorphicQuery);

        String queryFormatString = "-- %s;\n-- result: %s";
        String firstQueryString = String.format(queryFormatString, originalQuery, firstResult);
        String secondQueryString = String.format(queryFormatString, metamorphicQuery, secondResult);
        state.getState().getLocalState().log(String.format("%s\n%s", firstQueryString, secondQueryString));
        if (firstResult == null && secondResult != null || firstResult != null && secondResult == null
                || firstResult != null && !firstResult.contentEquals(secondResult)
                && !ComparatorHelper.isEqualDouble(firstResult, secondResult)) {
            if (secondResult != null && secondResult.contains("Inf")) {
                throw new IgnoreMeException(); // FIXME: average computation
            }
            String assertionMessage = String.format("the results mismatch!\n%s\n%s", firstQueryString,
                    secondQueryString);
            throw new AssertionError(assertionMessage);
        }
    }

    private String createMetamorphicUnionQuery(HazelcastSelect select, HazelcastAggregate aggregate,
                                               List<HazelcastExpression> from) {
        String metamorphicQuery;
        HazelcastExpression whereClause = gen.generateExpression(HazelcastDataType.BOOLEAN);
        HazelcastExpression negatedClause = new HazelcastPrefixOperation(whereClause, PrefixOperator.NOT);
        HazelcastExpression notNullClause = new HazelcastPostfixOperation(whereClause, PostfixOperator.IS_NULL);
        List<HazelcastExpression> mappedAggregate = mapped(aggregate);
        HazelcastSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinClauses());
        HazelcastSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinClauses());
        HazelcastSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinClauses());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate) + " FROM (";
        metamorphicQuery += HazelcastVisitor.asString(leftSelect) + " UNION ALL "
                + HazelcastVisitor.asString(middleSelect) + " UNION ALL " + HazelcastVisitor.asString(rightSelect);
        metamorphicQuery += ") as asdf";
        return metamorphicQuery;
    }

    private String getAggregateResult(String queryString) throws SQLException {
        // log TLP Aggregate SELECT queries on the current log file
        if (state.getOptions().logEachSelect()) {
            // TODO: refactor me
            state.getLogger().writeCurrent(queryString);
            try {
                state.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        String resultString;
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
        try (SQLancerResultSet result = q.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            if (!result.next()) {
                resultString = null;
            } else {
                resultString = result.getString(1);
            }
        } catch (PSQLException e) {
            throw new AssertionError(queryString, e);
        }
        return resultString;
    }

    private List<HazelcastExpression> mapped(HazelcastAggregate aggregate) {
        switch (aggregate.getFunction()) {
            case SUM:
            case COUNT:
//        case BIT_AND:
//        case BIT_OR:
//        case BOOL_AND:
//        case BOOL_OR:
            case MAX:
            case MIN:
                return aliasArgs(Arrays.asList(aggregate));
            // case AVG:
            //// List<PostgresExpression> arg = Arrays.asList(new
            // PostgresCast(aggregate.getExpr().get(0),
            // PostgresDataType.DECIMAL.get()));
            // PostgresAggregate sum = new PostgresAggregate(PostgresAggregateFunction.SUM,
            // aggregate.getExpr());
            // PostgresCast count = new PostgresCast(
            // new PostgresAggregate(PostgresAggregateFunction.COUNT, aggregate.getExpr()),
            // PostgresDataType.DECIMAL.get());
            //// PostgresBinaryArithmeticOperation avg = new
            // PostgresBinaryArithmeticOperation(sum, count,
            // PostgresBinaryArithmeticOperator.DIV);
            // return aliasArgs(Arrays.asList(sum, count));
            default:
                throw new AssertionError(aggregate.getFunction());
        }
    }

    private List<HazelcastExpression> aliasArgs(List<HazelcastExpression> originalAggregateArgs) {
        List<HazelcastExpression> args = new ArrayList<>();
        int i = 0;
        for (HazelcastExpression expr : originalAggregateArgs) {
            args.add(new HazelcastAlias(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(HazelcastAggregate aggregate) {
        switch (aggregate.getFunction()) {
            // case AVG:
            // return "SUM(agg0::DECIMAL)/SUM(agg1)::DECIMAL";
            case COUNT:
                return HazelcastAggregateFunction.SUM.toString() + "(agg0)";
            default:
                return aggregate.getFunction().toString() + "(agg0)";
        }
    }

    private HazelcastSelect getSelect(List<HazelcastExpression> aggregates, List<HazelcastExpression> from,
                                      HazelcastExpression whereClause, List<HazelcastJoin> joinList) {
        HazelcastSelect leftSelect = new HazelcastSelect();
        leftSelect.setFetchColumns(aggregates);
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        leftSelect.setJoinClauses(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            leftSelect.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        return leftSelect;
    }

}
