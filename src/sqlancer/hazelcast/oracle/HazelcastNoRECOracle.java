package sqlancer.hazelcast.oracle;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.hazelcast.HazelcastCompoundDataType;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastSchema;
import sqlancer.hazelcast.HazelcastVisitor;
import sqlancer.hazelcast.ast.*;
import sqlancer.hazelcast.gen.HazelcastCommon;
import sqlancer.hazelcast.gen.HazelcastExpressionGenerator;
import sqlancer.hazelcast.oracle.tlp.HazelcastTLPBase;
import sqlancer.hazelcast.HazelcastSchema.HazelcastColumn;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTable;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTables;
import sqlancer.hazelcast.ast.HazelcastJoin.HazelcastJoinType;
import sqlancer.hazelcast.ast.HazelcastSelect.HazelcastFromTable;
import sqlancer.hazelcast.ast.HazelcastSelect.HazelcastSubquery;
import sqlancer.hazelcast.ast.HazelcastSelect.SelectType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HazelcastNoRECOracle extends NoRECBase<HazelcastGlobalState> implements TestOracle {

    private final HazelcastSchema s;

    public HazelcastNoRECOracle(HazelcastGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        HazelcastCommon.addCommonExpressionErrors(errors);
        HazelcastCommon.addCommonFetchErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        HazelcastTables randomTables = s.getRandomTableNonEmptyTables();
        List<HazelcastColumn> columns = randomTables.getColumns();
        HazelcastExpression randomWhereCondition = getRandomWhereCondition(columns);
        List<HazelcastTable> tables = randomTables.getTables();

        List<HazelcastJoin> joinStatements = getJoinStatements(state, columns, tables);
        List<HazelcastExpression> fromTables = tables.stream().map(t -> new HazelcastFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList());
        int secondCount = getUnoptimizedQueryCount(fromTables, randomWhereCondition, joinStatements);
        int firstCount = getOptimizedQueryCount(fromTables, columns, randomWhereCondition, joinStatements);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            String queryFormatString = "-- %s;\n-- count: %d";
            String firstQueryStringWithCount = String.format(queryFormatString, optimizedQueryString, firstCount);
            String secondQueryStringWithCount = String.format(queryFormatString, unoptimizedQueryString, secondCount);
            state.getState().getLocalState()
                    .log(String.format("%s\n%s", firstQueryStringWithCount, secondQueryStringWithCount));
            String assertionMessage = String.format("the counts mismatch (%d and %d)!\n%s\n%s", firstCount, secondCount,
                    firstQueryStringWithCount, secondQueryStringWithCount);
            throw new AssertionError(assertionMessage);
        }
    }

    public static List<HazelcastJoin> getJoinStatements(HazelcastGlobalState globalState, List<HazelcastSchema.HazelcastColumn> columns,
                                                        List<HazelcastTable> tables) {
        List<HazelcastJoin> joinStatements = new ArrayList<>();
        HazelcastExpressionGenerator gen = new HazelcastExpressionGenerator(globalState).setColumns(columns);
        for (int i = 1; i < tables.size(); i++) {
            HazelcastExpression joinClause = gen.generateExpression(HazelcastDataType.BOOLEAN);
            HazelcastTable table = Randomly.fromList(tables);
            tables.remove(table);
            HazelcastJoinType options = HazelcastJoinType.getRandom();
            HazelcastJoin j = new HazelcastJoin(new HazelcastFromTable(table, Randomly.getBoolean()), joinClause, options);
            joinStatements.add(j);
        }
        // JOIN subqueries
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            HazelcastTables subqueryTables = globalState.getSchema().getRandomTableNonEmptyTables();
            HazelcastSubquery subquery = HazelcastTLPBase.createSubquery(globalState, String.format("sub%d", i),
                    subqueryTables);
            HazelcastExpression joinClause = gen.generateExpression(HazelcastDataType.BOOLEAN);
            HazelcastJoinType options = HazelcastJoinType.getRandom();
            HazelcastJoin j = new HazelcastJoin(subquery, joinClause, options);
            joinStatements.add(j);
        }
        return joinStatements;
    }

    private HazelcastExpression getRandomWhereCondition(List<HazelcastColumn> columns) {
        return new HazelcastExpressionGenerator(state).setColumns(columns).generateExpression(HazelcastDataType.BOOLEAN);
    }

    private int getUnoptimizedQueryCount(List<HazelcastExpression> fromTables, HazelcastExpression randomWhereCondition,
                                         List<HazelcastJoin> joinStatements) throws SQLException {
        HazelcastSelect select = new HazelcastSelect();
        HazelcastCastOperation isTrue = new HazelcastCastOperation(randomWhereCondition,
                HazelcastCompoundDataType.create(HazelcastDataType.INT));
        HazelcastPostfixText asText = new HazelcastPostfixText(isTrue, " as count", null, HazelcastDataType.INT);
        select.setFetchColumns(Arrays.asList(asText));
        select.setFromList(fromTables);
        select.setSelectType(SelectType.ALL);
        select.setJoinClauses(joinStatements);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + HazelcastVisitor.asString(select) + ") as res";
        if (options.logEachSelect()) {
            logger.writeCurrent(unoptimizedQueryString);
        }
        errors.add("canceling statement due to statement timeout");
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGet(state);
        } catch (Exception e) {
            throw new AssertionError(unoptimizedQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        if (rs.next()) {
            secondCount += rs.getLong(1);
        }
        rs.close();
        return secondCount;
    }

    private int getOptimizedQueryCount(List<HazelcastExpression> randomTables, List<HazelcastColumn> columns,
                                       HazelcastExpression randomWhereCondition, List<HazelcastJoin> joinStatements) throws SQLException {
        HazelcastSelect select = new HazelcastSelect();
        HazelcastColumnValue allColumns = new HazelcastColumnValue(Randomly.fromList(columns), null);
        select.setFetchColumns(Arrays.asList(allColumns));
        select.setFromList(randomTables);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new HazelcastExpressionGenerator(state).setColumns(columns).generateOrderBy());
        }
        select.setSelectType(SelectType.ALL);
        select.setJoinClauses(joinStatements);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = HazelcastVisitor.asString(select);
            if (options.logEachSelect()) {
                logger.writeCurrent(optimizedQueryString);
            }
            try (ResultSet rs = stat.executeQuery(optimizedQueryString)) {
                while (rs.next()) {
                    firstCount++;
                }
            }
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        return firstCount;
    }

}
