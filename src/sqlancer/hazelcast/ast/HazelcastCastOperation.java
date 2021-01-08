package sqlancer.hazelcast.ast;

import sqlancer.hazelcast.HazelcastCompoundDataType;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

public class HazelcastCastOperation implements HazelcastExpression {

    private final HazelcastExpression expression;
    private final HazelcastCompoundDataType type;

    public HazelcastCastOperation(HazelcastExpression expression, HazelcastCompoundDataType type) {
        if (expression == null) {
            throw new AssertionError();
        }
        this.expression = expression;
        this.type = type;
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return type.getDataType();
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        HazelcastConstant expectedValue = expression.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return expectedValue.cast(type.getDataType());
    }

    public HazelcastExpression getExpression() {
        return expression;
    }

    public HazelcastDataType getType() {
        return type.getDataType();
    }

    public HazelcastCompoundDataType getCompoundType() {
        return type;
    }

}
