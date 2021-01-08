package sqlancer.hazelcast.ast;

import sqlancer.Randomly;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

public class HazelcastOrderByTerm implements HazelcastExpression {

    private final HazelcastOrder order;
    private final HazelcastExpression expr;

    public enum HazelcastOrder {
        ASC, DESC;

        public static HazelcastOrder getRandomOrder() {
            return Randomly.fromOptions(HazelcastOrder.values());
        }
    }

    public HazelcastOrderByTerm(HazelcastExpression expr, HazelcastOrder order) {
        this.expr = expr;
        this.order = order;
    }

    public HazelcastOrder getOrder() {
        return order;
    }

    public HazelcastExpression getExpr() {
        return expr;
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        throw new AssertionError(this);
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return null;
    }

}
