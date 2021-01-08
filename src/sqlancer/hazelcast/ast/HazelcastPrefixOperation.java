package sqlancer.hazelcast.ast;

import sqlancer.IgnoreMeException;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

public class HazelcastPrefixOperation implements HazelcastExpression {

    public enum PrefixOperator implements Operator {
        NOT("NOT", HazelcastDataType.BOOLEAN) {

            @Override
            public HazelcastDataType getExpressionType() {
                return HazelcastDataType.BOOLEAN;
            }

            @Override
            protected HazelcastConstant getExpectedValue(HazelcastConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return HazelcastConstant.createNullConstant();
                } else {
                    return HazelcastConstant
                            .createBooleanConstant(!expectedValue.cast(HazelcastDataType.BOOLEAN).asBoolean());
                }
            }
        },
        UNARY_PLUS("+", HazelcastDataType.INT) {

            @Override
            public HazelcastDataType getExpressionType() {
                return HazelcastDataType.INT;
            }

            @Override
            protected HazelcastConstant getExpectedValue(HazelcastConstant expectedValue) {
                // TODO: actual converts to double precision
                return expectedValue;
            }

        },
        UNARY_MINUS("-", HazelcastDataType.INT) {

            @Override
            public HazelcastDataType getExpressionType() {
                return HazelcastDataType.INT;
            }

            @Override
            protected HazelcastConstant getExpectedValue(HazelcastConstant expectedValue) {
                if (expectedValue.isNull()) {
                    // TODO
                    throw new IgnoreMeException();
                }
                if (expectedValue.isInt() && expectedValue.asInt() == Long.MIN_VALUE) {
                    throw new IgnoreMeException();
                }
                try {
                    return HazelcastConstant.createIntConstant(-expectedValue.asInt());
                } catch (UnsupportedOperationException e) {
                    return null;
                }
            }

        };

        private String textRepresentation;
        private HazelcastDataType[] dataTypes;

        PrefixOperator(String textRepresentation, HazelcastDataType... dataTypes) {
            this.textRepresentation = textRepresentation;
            this.dataTypes = dataTypes.clone();
        }

        public abstract HazelcastDataType getExpressionType();

        protected abstract HazelcastConstant getExpectedValue(HazelcastConstant expectedValue);

        @Override
        public String getTextRepresentation() {
            return toString();
        }

    }

    private final HazelcastExpression expr;
    private final PrefixOperator op;

    public HazelcastPrefixOperation(HazelcastExpression expr, PrefixOperator op) {
        this.expr = expr;
        this.op = op;
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return op.getExpressionType();
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        HazelcastConstant expectedValue = expr.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return op.getExpectedValue(expectedValue);
    }

    public HazelcastDataType[] getInputDataTypes() {
        return op.dataTypes;
    }

    public String getTextRepresentation() {
        return op.textRepresentation;
    }

    public HazelcastExpression getExpression() {
        return expr;
    }

}
