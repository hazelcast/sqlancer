package sqlancer.hazelcast.gen;

import sqlancer.Randomly;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.ast.HazelcastConstant;
import sqlancer.hazelcast.ast.HazelcastConstants;
import sqlancer.hazelcast.ast.HazelcastExpression;
import sqlancer.hazelcast.ast.HazelcastSelect;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTables;
import sqlancer.hazelcast.ast.HazelcastSelect.ForClause;
import sqlancer.hazelcast.ast.HazelcastSelect.HazelcastFromTable;
import sqlancer.hazelcast.ast.HazelcastSelect.SelectType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class HazelcastRandomQueryGenerator {

    private HazelcastRandomQueryGenerator() {
    }

    public static HazelcastSelect createRandomQuery(int nrColumns, HazelcastGlobalState globalState) {
        List<HazelcastExpression> columns = new ArrayList<>();
        HazelcastTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        HazelcastExpressionGenerator gen = new HazelcastExpressionGenerator(globalState).setColumns(tables.getColumns());
        for (int i = 0; i < nrColumns; i++) {
            columns.add(gen.generateExpression(0));
        }
        HazelcastSelect select = new HazelcastSelect();
        select.setSelectType(SelectType.getRandom());
        if (select.getSelectOption() == SelectType.DISTINCT && Randomly.getBoolean()) {
            select.setDistinctOnClause(gen.generateExpression(0));
        }
        select.setFromList(tables.getTables().stream().map(t -> new HazelcastFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList()));
        select.setFetchColumns(columns);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(0, HazelcastDataType.BOOLEAN));
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateHavingClause());
            }
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
        return select;
    }

}
