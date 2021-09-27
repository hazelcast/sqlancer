package sqlancer.hazelcast.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.hazelcast.HazelcastGlobalState;

public final class HazelcastDiscardGenerator {

    private HazelcastDiscardGenerator() {
    }

    // TODO: is not supported, actually.
    public static SQLQueryAdapter create(HazelcastGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("DISCARD ");
        String what = Randomly.fromOptions("PLANS", "SEQUENCES");
        sb.append(what);
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("cannot run inside a transaction block")) {
            @Override
            public boolean couldAffectSchema() {
                return canDiscardTemporaryTables(what);
            }
        };
    }

    private static boolean canDiscardTemporaryTables(String what) {
        return what.contentEquals("TEMPORARY") || what.contentEquals("TEMP") || what.contentEquals("ALL");
    }
}
