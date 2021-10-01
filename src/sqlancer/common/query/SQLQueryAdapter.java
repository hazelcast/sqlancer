package sqlancer.common.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.hazelcast.sql.impl.QueryException;
import sqlancer.GlobalState;
import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.SQLConnection;

public class SQLQueryAdapter extends Query<SQLConnection> {

    private final String query;
    private final ExpectedErrors expectedErrors;
    private final boolean couldAffectSchema;

    public SQLQueryAdapter(String query) {
        this(query, new ExpectedErrors());
    }

    public SQLQueryAdapter(String query, boolean couldAffectSchema) {
        this(query, new ExpectedErrors(), couldAffectSchema);
    }

    public SQLQueryAdapter(String query, ExpectedErrors expectedErrors) {
        this(query, expectedErrors, false);
    }

    public SQLQueryAdapter(String query, ExpectedErrors expectedErrors, boolean couldAffectSchema) {
        this.query = canonicalizeString(query);
        this.expectedErrors = expectedErrors;
        this.couldAffectSchema = couldAffectSchema;
        checkQueryString();
    }

    private String canonicalizeString(String s) {
        if (s.endsWith(";")) {
            return s;
        } else if (!s.contains("--")) {
            return s + ";";
        } else {
            // query contains a comment
            return s;
        }
    }

    private void checkQueryString() {
        if (query.contains("CREATE TABLE") && !couldAffectSchema) {
            throw new AssertionError("CREATE TABLE statements should set couldAffectSchema to true");
        }
        if (query.equals(";")) {
            throw new IgnoreMeException();
        }
    }

    @Override
    public String getQueryString() {
        return query;
    }

    @Override
    public String getUnterminatedQueryString() {
        String result;
        if (query.endsWith(";")) {
            result = query.substring(0, query.length() - 1);
        } else {
            result = query;
        }
        assert !result.endsWith(";");
        return result;
    }

    @Override
    public <G extends GlobalState<?, ?, SQLConnection>> boolean execute(G globalState, String... fills)
            throws SQLException {
        Statement s;
        if (fills.length > 0) {
            s = globalState.getConnection().prepareStatement(fills[0]);
            for (int i = 1; i < fills.length; i++) {
                ((PreparedStatement) s).setString(i, fills[i]);
            }
        } else {
            s = globalState.getConnection().createStatement();
        }
        try {
            if (fills.length > 0) {
                ((PreparedStatement) s).execute();
            } else {
                s.execute(query);
            }
            Main.nrSuccessfulActions.addAndGet(1);
            return true;
        } catch (Exception e) {
            Main.nrUnsuccessfulActions.addAndGet(1);
            System.out.println("Problems in query : " + query);
            checkException(e);
            return false;
        }
    }

    public void checkException(Exception e) throws AssertionError {
        if (!expectedErrors.errorIsExpected(e.getMessage())) {
            Throwable rootCause = findRootCause(e);
            if (!expectedErrors.errorIsExpected(rootCause.getMessage())) {
                e.printStackTrace();
                throw new AssertionError(e.getMessage(), e);
            }
        }
    }

    @Override
    public <G extends GlobalState<?, ?, SQLConnection>> SQLancerResultSet executeAndGet(G globalState, String... fills)
            throws SQLException {
        Statement s;
        if (fills.length > 0) {
            s = globalState.getConnection().prepareStatement(fills[0]);
            for (int i = 1; i < fills.length; i++) {
                ((PreparedStatement) s).setString(i, fills[i]);
            }
        } else {
            s = globalState.getConnection().createStatement();
        }
        ResultSet result;
        System.out.println(query);
        try {
            if (fills.length > 0) {
                result = ((PreparedStatement) s).executeQuery();
            } else {
                result = s.executeQuery(query);
            }
            Main.nrSuccessfulActions.addAndGet(1);
            if (result == null) {
                return null;
            }
            return new SQLancerResultSet(result);
        } catch (SQLException e) {
            System.out.println("Problems in query : " + query);
            s.close();
            Main.nrUnsuccessfulActions.addAndGet(1);
            checkException(e);
        }
        return null;
    }

    @Override
    public boolean couldAffectSchema() {
        return couldAffectSchema;
    }

    @Override
    public ExpectedErrors getExpectedErrors() {
        return expectedErrors;
    }

    @Override
    public String getLogString() {
        return getQueryString();
    }

    private Throwable findRootCause(Throwable e) {
        while (e.getCause() != null) {
            if (e instanceof QueryException) {
                return e;
            }
            e = e.getCause();
        }
        return e;
    }
}
