package sqlancer.hazelcast.oracle;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastSchema.HazelcastColumn;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.HazelcastSchema.HazelcastRowValue;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTables;
import sqlancer.hazelcast.HazelcastVisitor;
import sqlancer.hazelcast.ast.*;
import sqlancer.hazelcast.ast.HazelcastPostfixOperation.PostfixOperator;
import sqlancer.hazelcast.ast.HazelcastSelect.HazelcastFromTable;
import sqlancer.hazelcast.gen.HazelcastCommon;
import sqlancer.hazelcast.gen.HazelcastExpressionGenerator;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class HazelcastPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<HazelcastGlobalState, HazelcastRowValue, HazelcastExpression, SQLConnection> {

    public HazelcastPivotedQuerySynthesisOracle(HazelcastGlobalState globalState) throws SQLException {
        super(globalState);
        this.errors = HazelcastCommon.knownErrors;
    }

    @Override
    public SQLQueryAdapter getRectifiedQuery() throws SQLException {
        HazelcastTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();

        HazelcastSelect selectStatement = new HazelcastSelect();
        selectStatement.setSelectType(Randomly.fromOptions(HazelcastSelect.SelectType.values()));
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

        List<HazelcastColumn> fetchColumns = pivotRow.getTable().getUsedColumns();
        selectStatement.setFromList(pivotRow.getTable().getUsedTables()
                .stream().map(t -> new HazelcastFromTable(t, true))
                .collect(Collectors.toList()));
        selectStatement.setFetchColumns(fetchColumns.stream()
                .map(c -> new HazelcastColumnValue(getFetchValueAliasedColumn(c), pivotRow.getValues().get(c)))
                .collect(Collectors.toList()));
        HazelcastExpression whereClause = generateRectifiedExpression(fetchColumns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        List<HazelcastExpression> groupByClause = generateGroupByClause(fetchColumns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        HazelcastExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            HazelcastExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<HazelcastExpression> orderBy = new HazelcastExpressionGenerator(globalState).setColumns(fetchColumns)
                .generateOrderBy();
        selectStatement.setOrderByExpressions(orderBy);
        String selectQuery = HazelcastVisitor.asString(selectStatement);
        return new SQLQueryAdapter(selectQuery, HazelcastCommon.knownErrors);
    }

    /*
     * Prevent name collisions by aliasing the column.
     */
    private HazelcastColumn getFetchValueAliasedColumn(HazelcastColumn c) {
        HazelcastColumn aliasedColumn = new HazelcastColumn(c.getName() + " AS " + c.getTable().getName() + c.getName(),
                c.getType());
        aliasedColumn.setTable(c.getTable());
        return aliasedColumn;
    }

    private List<HazelcastExpression> generateGroupByClause(List<HazelcastColumn> columns, HazelcastRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> HazelcastColumnValue.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private HazelcastConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return HazelcastConstants.createLongConstant((int) Short.MAX_VALUE);
        } else {
            return null;
        }
    }

    private HazelcastExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return HazelcastConstants.createLongConstant(0);
        } else {
            return null;
        }
    }

    private HazelcastExpression generateRectifiedExpression(List<HazelcastColumn> columns, HazelcastRowValue rw) {
        HazelcastExpression expr = new HazelcastExpressionGenerator(globalState).setColumns(columns).setRowValue(rw)
                .generateExpressionWithExpectedResult(HazelcastDataType.BOOLEAN);
        HazelcastExpression result;
        if (expr.getExpectedValue().isNull()) {
            result = HazelcastPostfixOperation.create(expr, PostfixOperator.IS_NULL);
        } else {
            result = HazelcastPostfixOperation.create(expr,
                    expr.getExpectedValue().cast(HazelcastDataType.BOOLEAN).asBoolean() ? PostfixOperator.IS_TRUE
                            : PostfixOperator.IS_FALSE);
        }
        rectifiedPredicates.add(result);
        return result;
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> query) {
        StringBuilder sb = new StringBuilder();
        sb.append(query.getUnterminatedQueryString());
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected String getExpectedValues(HazelcastExpression expr) {
        return HazelcastVisitor.asExpectedValues(expr);
    }

}