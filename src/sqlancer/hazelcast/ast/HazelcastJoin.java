package sqlancer.hazelcast.ast;

import sqlancer.Randomly;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

public class HazelcastJoin implements HazelcastExpression {

    public enum HazelcastJoinType {
        INNER, LEFT, RIGHT, FULL, CROSS;

        public static HazelcastJoinType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    private final HazelcastExpression tableReference;
    private final HazelcastExpression onClause;
    private final HazelcastJoinType type;

    public HazelcastJoin(HazelcastExpression tableReference, HazelcastExpression onClause, HazelcastJoinType type) {
        this.tableReference = tableReference;
        this.onClause = onClause;
        this.type = type;
    }

    public HazelcastExpression getTableReference() {
        return tableReference;
    }

    public HazelcastExpression getOnClause() {
        return onClause;
    }

    public HazelcastJoinType getType() {
        return type;
    }

    @Override
    public HazelcastDataType getExpressionType() {
        throw new AssertionError();
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        throw new AssertionError();
    }

}
