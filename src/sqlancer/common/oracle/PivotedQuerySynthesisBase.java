package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.schema.AbstractRowValue;

public abstract class PivotedQuerySynthesisBase<S extends GlobalState<?, ?>, R extends AbstractRowValue<?, ?, ?>, E>
        implements TestOracle {

    protected final ExpectedErrors errors = new ExpectedErrors();

    /**
     * The predicates used in WHERE and JOIN clauses, which yield TRUE for the pivot row
     */
    protected final List<E> rectifiedPredicates = new ArrayList<>();
    protected final S globalState;
    protected R pivotRow;

    public PivotedQuerySynthesisBase(S globalState) {
        this.globalState = globalState;
    }

    @Override
    public final void check() throws SQLException {
        rectifiedPredicates.clear();
        Query pivotRowQuery = getRectifiedQuery();
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(pivotRowQuery.getQueryString());
        }
        Query isContainedQuery = getContainmentCheckQuery(pivotRowQuery);
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(isContainedQuery.getQueryString());
        }
        globalState.getState().getLocalState().log(isContainedQuery.getQueryString());
        // combines step 6 and 7 described in the PQS paper
        boolean pivotRowIsContained = containsRows(isContainedQuery);
        if (!pivotRowIsContained) {
            reportMissingPivotRow(pivotRowQuery);
        }
    }

    /**
     * Checks whether the result set contains at least a single row.
     *
     * @param query
     *            the query for which to check whether its result set contains any rows
     *
     * @return true if at least one row is contained, false otherwise
     *
     * @throws SQLException
     */
    private boolean containsRows(Query query) throws SQLException {
        try (SQLancerResultSet result = query.executeAndGet(globalState)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            return !result.isClosed();
        }
    }

    protected void reportMissingPivotRow(Query query) {
        globalState.getState().getLocalState().log("-- " + "pivot row values:\n");
        String expectedPivotRowString = pivotRow.asStringGroupedByTables();
        globalState.getState().getLocalState().log(expectedPivotRowString);

        StringBuilder sb = new StringBuilder("-- rectified predicates and their expected values:\n");
        for (E rectifiedPredicate : rectifiedPredicates) {
            sb.append("--");
            sb.append(getExpectedValues(rectifiedPredicate).replace("\n", "\n-- "));
        }
        globalState.getState().getLocalState().log(sb.toString());
        throw new AssertionError(query);
    }

    /**
     * Gets a query that checks whether the pivot row is contained in the result. If the pivot row is contained, the
     * query will fetch at least one row. If the pivot row is not contained, no rows will be fetched. This corresponds
     * to step 7 described in the PQS paper.
     *
     * @param pivotRowQuery
     *            the query that is guaranteed to fetch the pivot row, potentially among other rows
     *
     * @return a query that checks whether the pivot row is contained in pivotRowQuery
     *
     * @throws SQLException
     */
    protected abstract Query getContainmentCheckQuery(Query pivotRowQuery) throws SQLException;

    /**
     * Obtains a rectified query (i.e., a query that is guaranteed to fetch the pivot row. This corresponds to steps 2-5
     * of the PQS paper.
     *
     * @return the rectified query
     *
     * @throws SQLException
     */
    protected abstract Query getRectifiedQuery() throws SQLException;

    /**
     * Prints the value to which the expression is expected to evaluate, and then recursively prints the subexpressions'
     * expected values.
     *
     * @param expr
     *            the expression whose expected value should be printed
     *
     * @return a string representing the expected value of the expression and its subexpressions
     */
    protected abstract String getExpectedValues(E expr);

}
