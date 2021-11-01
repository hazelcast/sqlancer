package sqlancer.hazelcast.ast;

import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

import java.util.List;

public class HazelcastInOperation implements HazelcastExpression {

    private final HazelcastExpression expr;
    private final List<HazelcastExpression> listElements;
    private final boolean isTrue;

    public HazelcastInOperation(HazelcastExpression expr, List<HazelcastExpression> listElements, boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public HazelcastExpression getExpr() {
        return expr;
    }

    public List<HazelcastExpression> getListElements() {
        return listElements;
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        HazelcastConstant leftValue = expr.getExpectedValue();
        if (leftValue == null) {
            return null;
        }
        if (leftValue.isNull()) {
            return HazelcastConstants.createNullConstant();
        }
        boolean isNull = false;
        for (HazelcastExpression expr : getListElements()) {
            HazelcastConstant rightExpectedValue = expr.getExpectedValue();
            if (rightExpectedValue == null) {
                return null;
            }
            if (rightExpectedValue.isNull()) {
                isNull = true;
            } else if (rightExpectedValue.isEquals(this.expr.getExpectedValue()).isBoolean()
                    && rightExpectedValue.isEquals(this.expr.getExpectedValue()).asBoolean()) {
                return HazelcastConstants.createBooleanConstant(isTrue);
            }
        }

        if (isNull) {
            return HazelcastConstants.createNullConstant();
        } else {
            return HazelcastConstants.createBooleanConstant(!isTrue);
        }
    }

    public boolean isTrue() {
        return isTrue;
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return HazelcastDataType.BOOLEAN;
    }
}
