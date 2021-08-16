package sqlancer.hazelcast.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.ast.HazelcastBinaryArithmeticOperation.HazelcastBinaryOperator;

import java.util.function.BinaryOperator;

public class HazelcastBinaryArithmeticOperation extends BinaryOperatorNode<HazelcastExpression, HazelcastBinaryOperator>
        implements HazelcastExpression {

    public enum HazelcastBinaryOperator implements Operator {

        ADDITION("+") {
            @Override
            public HazelcastConstant apply(HazelcastConstant left, HazelcastConstant right) {
                return applyBitOperation(left, right, (l, r) -> l + r);
            }

        },
        SUBTRACTION("-") {
            @Override
            public HazelcastConstant apply(HazelcastConstant left, HazelcastConstant right) {
                return applyBitOperation(left, right, (l, r) -> l - r);
            }
        },
        MULTIPLICATION("*") {
            @Override
            public HazelcastConstant apply(HazelcastConstant left, HazelcastConstant right) {
                return applyBitOperation(left, right, (l, r) -> l * r);
            }
        },
        DIVISION("/") {

            @Override
            public HazelcastConstant apply(HazelcastConstant left, HazelcastConstant right) {
                return applyBitOperation(left, right, (l, r) -> r == 0 ? -1 : l / r);

            }

        },
        MODULO("%") {
            @Override
            public HazelcastConstant apply(HazelcastConstant left, HazelcastConstant right) {
                return applyBitOperation(left, right, (l, r) -> r == 0 ? -1 : l % r);

            }
        };
//        EXPONENTIATION("^") {
//            @Override
//            public HazelcastConstant apply(HazelcastConstant left, HazelcastConstant right) {
//                return null;
//            }
//        };

        private String textRepresentation;

        private static HazelcastConstant applyBitOperation(HazelcastConstant left, HazelcastConstant right,
                                                           BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return HazelcastConstant.createNullConstant();
            } else {
                long leftVal = left.cast(HazelcastDataType.INT).asInt();
                long rightVal = right.cast(HazelcastDataType.INT).asInt();
                long value = op.apply(leftVal, rightVal);
                return HazelcastConstant.createIntConstant(value);
            }
        }

        HazelcastBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract HazelcastConstant apply(HazelcastConstant left, HazelcastConstant right);

        public static HazelcastBinaryOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public HazelcastBinaryArithmeticOperation(HazelcastExpression left, HazelcastExpression right,
                                              HazelcastBinaryOperator op) {
        super(left, right, op);
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        HazelcastConstant leftExpected = getLeft().getExpectedValue();
        HazelcastConstant rightExpected = getRight().getExpectedValue();
        if (leftExpected == null || rightExpected == null) {
            return null;
        }
        return getOp().apply(leftExpected, rightExpected);
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return HazelcastDataType.INT;
    }

}
