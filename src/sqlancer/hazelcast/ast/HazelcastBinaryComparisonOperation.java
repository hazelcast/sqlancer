package sqlancer.hazelcast.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.ast.HazelcastBinaryComparisonOperation.HazelcastBinaryComparisonOperator;

public class HazelcastBinaryComparisonOperation extends BinaryOperatorNode<HazelcastExpression, HazelcastBinaryComparisonOperator> implements HazelcastExpression {

    public enum HazelcastBinaryComparisonOperator implements Operator {
        EQUALS("=") {
            @Override
            public HazelcastConstant getExpectedValue(HazelcastConstant leftVal, HazelcastConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public HazelcastConstant getExpectedValue(HazelcastConstant leftVal, HazelcastConstant rightVal) {
                HazelcastConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.isBoolean()) {
                    return HazelcastConstants.createBooleanConstant(!isEquals.asBoolean());
                }
                return isEquals;
            }
        },
        LESS("<") {

            @Override
            public HazelcastConstant getExpectedValue(HazelcastConstant leftVal, HazelcastConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }
        },
        LESS_EQUALS("<=") {

            @Override
            public HazelcastConstant getExpectedValue(HazelcastConstant leftVal, HazelcastConstant rightVal) {
                HazelcastConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan.isBoolean() && !lessThan.asBoolean()) {
                    return leftVal.isEquals(rightVal);
                } else {
                    return lessThan;
                }
            }
        },
        GREATER(">") {
            @Override
            public HazelcastConstant getExpectedValue(HazelcastConstant leftVal, HazelcastConstant rightVal) {
                HazelcastConstant equals = leftVal.isEquals(rightVal);
                if (equals.isBoolean() && equals.asBoolean()) {
                    return HazelcastConstants.createFalse();
                } else {
                    HazelcastConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return HazelcastConstants.createNullConstant();
                    }
                    return HazelcastPrefixOperation.PrefixOperator.NOT.getExpectedValue(applyLess);
                }
            }
        },
        GREATER_EQUALS(">=") {

            @Override
            public HazelcastConstant getExpectedValue(HazelcastConstant leftVal, HazelcastConstant rightVal) {
                HazelcastConstant equals = leftVal.isEquals(rightVal);
                if (equals.isBoolean() && equals.asBoolean()) {
                    return HazelcastConstants.createTrue();
                } else {
                    HazelcastConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return HazelcastConstants.createNullConstant();
                    }
                    return HazelcastPrefixOperation.PrefixOperator.NOT.getExpectedValue(applyLess);
                }
            }

        };

        private final String textRepresentation;

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        HazelcastBinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public abstract HazelcastConstant getExpectedValue(HazelcastConstant leftVal, HazelcastConstant rightVal);

        public static HazelcastBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(
                    HazelcastBinaryComparisonOperator.EQUALS,
                    HazelcastBinaryComparisonOperator.GREATER,
                    HazelcastBinaryComparisonOperator.GREATER_EQUALS,
                    HazelcastBinaryComparisonOperator.LESS,
                    HazelcastBinaryComparisonOperator.LESS_EQUALS,
                    HazelcastBinaryComparisonOperator.NOT_EQUALS
                    );
        }

    }

    public HazelcastBinaryComparisonOperation(HazelcastExpression left, HazelcastExpression right,
                                              HazelcastBinaryComparisonOperator op) {
        super(left, right, op);
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        HazelcastConstant leftExpectedValue = getLeft().getExpectedValue();
        HazelcastConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().getExpectedValue(leftExpectedValue, rightExpectedValue);
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return HazelcastDataType.BOOLEAN;
    }

}
