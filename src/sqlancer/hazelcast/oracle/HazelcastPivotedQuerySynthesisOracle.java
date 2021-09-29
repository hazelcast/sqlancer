package sqlancer.hazelcast.oracle;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastSchema;
import sqlancer.hazelcast.HazelcastVisitor;
import sqlancer.hazelcast.ast.*;
import sqlancer.hazelcast.gen.HazelcastCommon;
import sqlancer.hazelcast.gen.HazelcastExpressionGenerator;
import sqlancer.hazelcast.HazelcastSchema.HazelcastColumn;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.HazelcastSchema.HazelcastRowValue;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTables;
import sqlancer.hazelcast.ast.HazelcastPostfixOperation.PostfixOperator;
import sqlancer.hazelcast.ast.HazelcastSelect.HazelcastFromTable;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class HazelcastPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<HazelcastGlobalState, HazelcastRowValue, HazelcastExpression, SQLConnection> {

    private List<HazelcastColumn> fetchColumns;

    public HazelcastPivotedQuerySynthesisOracle(HazelcastGlobalState globalState) throws SQLException {
        super(globalState);
        HazelcastCommon.addCommonExpressionErrors(errors);
        HazelcastCommon.addCommonFetchErrors(errors);
    }

    @Override
    public SQLQueryAdapter getRectifiedQuery() throws SQLException {
        HazelcastTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();

        HazelcastSelect selectStatement = new HazelcastSelect();
        selectStatement.setSelectType(Randomly.fromOptions(HazelcastSelect.SelectType.values()));
        List<HazelcastColumn> columns = randomFromTables.getColumns();
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());
        HazelcastSchema.HazelcastTable usedTable = pivotRow.getUsedTable();

        fetchColumns = columns.stream().filter(c -> c.getTable().equals(usedTable)).collect(Collectors.toList());
        selectStatement.setFromList(Collections.singletonList(new HazelcastFromTable(usedTable, true)));
        selectStatement.setFetchColumns(fetchColumns.stream()
                .map(c -> new HazelcastColumnValue(getFetchValueAliasedColumn(c), pivotRow.getValues().get(c)))
                .collect(Collectors.toList()));
        HazelcastExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        List<HazelcastExpression> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        HazelcastExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            HazelcastExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<HazelcastExpression> orderBy = new HazelcastExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBy();
        selectStatement.setOrderByExpressions(orderBy);
        String selectQuery = HazelcastVisitor.asString(selectStatement);
        return new SQLQueryAdapter(selectQuery);
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
            return HazelcastConstants.createIntConstant(Byte.MAX_VALUE);
        } else {
            return null;
        }
    }

    private HazelcastExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return HazelcastConstants.createIntConstant(0);
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
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> query) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append(query.getUnterminatedQueryString());
        int i = 0;
        for (HazelcastColumn c : fetchColumns) {
            if (i++ != 0) {
                sb.append(" AND ");
            }
            sb.append(c.getTable().getName());
            if (pivotRow.getValues().get(c).isNull()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" = ");
                sb.append(pivotRow.getValues().get(c).getTextRepresentation());
            }
        }
        String resultingQueryString = sb.toString();
        return new SQLQueryAdapter(resultingQueryString, errors);
    }

    @Override
    protected String getExpectedValues(HazelcastExpression expr) {
        return HazelcastVisitor.asExpectedValues(expr);
    }

}
// SELECT t7_s7oI4z3nz6.__key AS t7_s7oI4z3nz6__key,
//        t7_s7oI4z3nz6.c0 AS t7_s7oI4z3nz6c0,
//        t7_s7oI4z3nz6.c1 AS t7_s7oI4z3nz6c1,
//        t7_s7oI4z3nz6.c2 AS t7_s7oI4z3nz6c2,
//        t7_s7oI4z3nz6.c3 AS t7_s7oI4z3nz6c3,
//        t7_s7oI4z3nz6.c4 AS t7_s7oI4z3nz6c4
//        FROM t7_s7oI4z3nz6 WHERE (t7_s7oI4z3nz6.c1) ISNULL GROUP BY t7_s7oI4z3nz6.__key, t7_s7oI4z3nz6.c0, t7_s7oI4z3nz6.c1, t7_s7oI4z3nz6.c2, t7_s7oI4z3nz6.c3, t7_s7oI4z3nz6.c4, t4_NzYq4kEuYZ.__key, t4_NzYq4kEuYZ.c0 ORDER BY t7_s7oI4z3nz6.c1 DESC, t7_s7oI4z3nz6.c1 DESCt7_s7oI4z3nz6 = 539184101 AND t7_s7oI4z3nz6 IS NULL AND t7_s7oI4z3nz6 IS NULL AND t7_s7oI4z3nz6 IS NULL AND t7_s7oI4z3nz6 IS NULL AND t7_s7oI4z3nz6 = FALSE;
