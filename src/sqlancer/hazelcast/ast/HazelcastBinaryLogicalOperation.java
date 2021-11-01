package sqlancer.hazelcast.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.ast.HazelcastBinaryLogicalOperation.BinaryLogicalOperator;

public class HazelcastBinaryLogicalOperation extends BinaryOperatorNode<HazelcastExpression, BinaryLogicalOperator>
        implements HazelcastExpression {

    public enum BinaryLogicalOperator implements Operator {
        AND {
            @Override
            public HazelcastConstant apply(HazelcastConstant left, HazelcastConstant right) {
                HazelcastConstant leftBool = left.cast(HazelcastDataType.BOOLEAN);
                HazelcastConstant rightBool = right.cast(HazelcastDataType.BOOLEAN);
                if (leftBool.isNull()) {
                    if (rightBool.isNull()) {
                        return HazelcastConstants.createNullConstant();
                    } else {
                        if (rightBool.asBoolean()) {
                            return HazelcastConstants.createNullConstant();
                        } else {
                            return HazelcastConstants.createFalse();
                        }
                    }
                } else if (!leftBool.asBoolean()) {
                    return HazelcastConstants.createFalse();
                }
                assert leftBool.asBoolean();
                if (rightBool.isNull()) {
                    return HazelcastConstants.createNullConstant();
                } else {
                    return HazelcastConstants.createBooleanConstant(rightBool.isBoolean() && rightBool.asBoolean());
                }
            }
        },
        OR {
            @Override
            public HazelcastConstant apply(HazelcastConstant left, HazelcastConstant right) {
                HazelcastConstant leftBool = left.cast(HazelcastDataType.BOOLEAN);
                HazelcastConstant rightBool = right.cast(HazelcastDataType.BOOLEAN);
                if (leftBool.isBoolean() && leftBool.asBoolean()) {
                    return HazelcastConstants.createTrue();
                }
                if (rightBool.isBoolean() && rightBool.asBoolean()) {
                    return HazelcastConstants.createTrue();
                }
                if (leftBool.isNull() || rightBool.isNull()) {
                    return HazelcastConstants.createNullConstant();
                }
                return HazelcastConstants.createFalse();
            }
        };

        public abstract HazelcastConstant apply(HazelcastConstant left, HazelcastConstant right);

        public static BinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public HazelcastBinaryLogicalOperation(HazelcastExpression left, HazelcastExpression right, BinaryLogicalOperator op) {
        super(left, right, op);
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return HazelcastDataType.BOOLEAN;
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        HazelcastConstant leftExpectedValue = getLeft().getExpectedValue();
        HazelcastConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().apply(leftExpectedValue, rightExpectedValue);
    }

}
