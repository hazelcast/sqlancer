package sqlancer.hazelcast.ast;

import sqlancer.LikeImplementationHelper;
import sqlancer.common.ast.BinaryNode;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

public class HazelcastLikeOperation extends BinaryNode<HazelcastExpression> implements HazelcastExpression {

    public HazelcastLikeOperation(HazelcastExpression left, HazelcastExpression right) {
        super(left, right);
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return HazelcastDataType.BOOLEAN;
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        HazelcastConstant leftVal = getLeft().getExpectedValue();
        HazelcastConstant rightVal = getRight().getExpectedValue();
        if (leftVal == null || rightVal == null) {
            return null;
        }
        if (leftVal.isNull() || rightVal.isNull()) {
            return HazelcastConstant.createNullConstant();
        } else {
            boolean val = LikeImplementationHelper.match(leftVal.asString(), rightVal.asString(), 0, 0, true);
            return HazelcastConstant.createBooleanConstant(val);
        }
    }

    @Override
    public String getOperatorRepresentation() {
        return "LIKE";
    }

}
