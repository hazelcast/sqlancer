package sqlancer.hazelcast.ast;

import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

public class HazelcastPostfixText implements HazelcastExpression {

    private final HazelcastExpression expr;
    private final String text;
    private final HazelcastConstant expectedValue;
    private final HazelcastDataType type;

    public HazelcastPostfixText(HazelcastExpression expr, String text, HazelcastConstant expectedValue,
                                HazelcastDataType type) {
        this.expr = expr;
        this.text = text;
        this.expectedValue = expectedValue;
        this.type = type;
    }

    public HazelcastExpression getExpr() {
        return expr;
    }

    public String getText() {
        return text;
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        return expectedValue;
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return type;
    }
}
