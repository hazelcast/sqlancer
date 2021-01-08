package sqlancer.hazelcast.ast;

import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

public class HazelcastCollate implements HazelcastExpression {

    private final HazelcastExpression expr;
    private final String collate;

    public HazelcastCollate(HazelcastExpression expr, String collate) {
        this.expr = expr;
        this.collate = collate;
    }

    public String getCollate() {
        return collate;
    }

    public HazelcastExpression getExpr() {
        return expr;
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return expr.getExpressionType();
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        return null;
    }

}
