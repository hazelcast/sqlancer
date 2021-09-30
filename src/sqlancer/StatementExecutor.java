package sqlancer;

import java.util.ArrayList;
import java.util.List;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.QueryException;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.hazelcast.HazelcastGlobalState;

import static sqlancer.hazelcast.gen.HazelcastCommon.findRootCause;

public class StatementExecutor<G extends GlobalState<?, ?, ?>, A extends AbstractAction<G>> {

    private final G globalState;
    private final A[] actions;
    private final ActionMapper<G, A> mapping;
    private final AfterQueryAction queryConsumer;

    @FunctionalInterface
    public interface AfterQueryAction {
        void notify(Query<?> q) throws Exception;
    }

    @FunctionalInterface
    public interface ActionMapper<T, A> {
        int map(T globalState, A action);
    }

    public StatementExecutor(G globalState, A[] actions, ActionMapper<G, A> mapping, AfterQueryAction queryConsumer) {
        this.globalState = globalState;
        this.actions = actions.clone();
        this.mapping = mapping;
        this.queryConsumer = queryConsumer;
    }

    @SuppressWarnings("unchecked")
    public void executeStatements() throws Exception {
        Randomly r = globalState.getRandomly();
        int[] nrRemaining = new int[actions.length];
        List<A> availableActions = new ArrayList<>();
        int total = 0;
        for (int i = 0; i < actions.length; i++) {
            A action = actions[i];
            int nrPerformed = mapping.map(globalState, action);
            if (nrPerformed != 0) {
                availableActions.add(action);
            }
            nrRemaining[i] = nrPerformed;
            total += nrPerformed;
        }
        while (total != 0) {
            A nextAction = null;
            int selection = r.getInteger(0, total);
            int previousRange = 0;
            int i;
            for (i = 0; i < nrRemaining.length; i++) {
                if (previousRange <= selection && selection < previousRange + nrRemaining[i]) {
                    nextAction = actions[i];
                    break;
                } else {
                    previousRange += nrRemaining[i];
                }
            }
            assert nextAction != null;
            assert nrRemaining[i] > 0;
            nrRemaining[i]--;
            @SuppressWarnings("rawtypes")
            Query query = null;
            try {
                boolean success;
                int nrTries = 0;
                do {
                    query = nextAction.getQuery(globalState);
                    try {
                        HazelcastGlobalState.executeStatement(query.getQueryString(), query.getExpectedErrors());
                        success = true;
                    } catch (HazelcastSqlException e) {
                        System.out.println("Problems in query : " + query.getQueryString());
                        success = false;
                        ExpectedErrors expectedErrors = query.getExpectedErrors();
                        Throwable rootCause = findRootCause(e);
                        if (!expectedErrors.errorIsExpected(rootCause.getMessage())) {
                            e.printStackTrace();
                        }
                    }
                } while (nextAction.canBeRetried() && !success
                        && nrTries++ < globalState.getOptions().getNrStatementRetryCount());
            } catch (IgnoreMeException e) {
            }
            if (query != null && query.couldAffectSchema()) {
                globalState.updateSchema();
                queryConsumer.notify(query);
            }
            total--;
        }
    }
}
