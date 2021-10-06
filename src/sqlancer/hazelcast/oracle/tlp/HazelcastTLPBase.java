package sqlancer.hazelcast.oracle.tlp;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastSchema;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTable;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTables;
import sqlancer.hazelcast.HazelcastSchema.HazelcastColumn;
import sqlancer.hazelcast.ast.*;
import sqlancer.hazelcast.ast.HazelcastSelect.ForClause;
import sqlancer.hazelcast.ast.HazelcastSelect.HazelcastFromTable;
import sqlancer.hazelcast.ast.HazelcastSelect.HazelcastSubquery;
import sqlancer.hazelcast.gen.HazelcastCommon;
import sqlancer.hazelcast.gen.HazelcastExpressionGenerator;
import sqlancer.hazelcast.oracle.HazelcastNoRECOracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HazelcastTLPBase extends TernaryLogicPartitioningOracleBase<HazelcastExpression, HazelcastGlobalState>
        implements TestOracle {

    protected HazelcastSchema s;
    protected HazelcastTables targetTables;
    protected HazelcastExpressionGenerator gen;
    protected HazelcastSelect select;

    public HazelcastTLPBase(HazelcastGlobalState state) {
        super(state);
        this.errors = HazelcastCommon.knownErrors;
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        List<HazelcastTable> tables = targetTables.getTables();
//        List<HazelcastJoin> joins = getJoinStatements(state, targetTables.getColumns(), tables);
        generateSelectBase(tables, null);
    }

    protected List<HazelcastJoin> getJoinStatements(HazelcastGlobalState globalState, List<HazelcastColumn> columns,
                                                    List<HazelcastTable> tables) {
        // TODO joins
        return HazelcastNoRECOracle.getJoinStatements(state, columns, tables);
    }

    protected void generateSelectBase(List<HazelcastTable> tables, List<HazelcastJoin> joins) {
        List<HazelcastExpression> tableList = tables.stream().map(t -> new HazelcastFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList());
        gen = new HazelcastExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new HazelcastSelect();
        select.setFetchColumns(generateFetchColumns());
        select.setFromList(tableList);
        select.setWhereClause(null);
//        select.setJoinClauses(joins);
        if (Randomly.getBoolean()) {
            select.setForClause(ForClause.getRandom());
        }
    }

    List<HazelcastExpression> generateFetchColumns() {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return Arrays.asList(new HazelcastColumnValue(HazelcastColumn.createDummy("*"), null));
        }
        List<HazelcastExpression> fetchColumns = new ArrayList<>();
        List<HazelcastColumn> targetColumns = Randomly.nonEmptySubset(targetTables.getColumns());
        for (HazelcastColumn c : targetColumns) {
            fetchColumns.add(new HazelcastColumnValue(c, null));
        }
        return fetchColumns;
    }

    @Override
    protected ExpressionGenerator<HazelcastExpression> getGen() {
        return gen;
    }

    public static HazelcastSubquery createSubquery(HazelcastGlobalState globalState, String name, HazelcastTables tables) {
        List<HazelcastExpression> columns = new ArrayList<>();
        HazelcastExpressionGenerator gen = new HazelcastExpressionGenerator(globalState).setColumns(tables.getColumns());
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            columns.add(gen.generateExpression(0));
        }
        HazelcastSelect select = new HazelcastSelect();
        select.setFromList(tables.getTables().stream().map(t -> new HazelcastFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList()));
        select.setFetchColumns(columns);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(0, HazelcastDataType.BOOLEAN));
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(HazelcastConstants.createLongConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(
                        HazelcastConstants.createLongConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setForClause(ForClause.getRandom());
        }
        return new HazelcastSubquery(select, name);
    }

}
