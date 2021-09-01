package sqlancer.hazelcast.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

public class HazelcastPostfixOperation implements HazelcastExpression {

    private final HazelcastExpression expr;
    private final PostfixOperator op;
    private final String operatorTextRepresentation;

    public enum PostfixOperator implements Operator {
        IS_NULL("IS NULL", "ISNULL") {
            @Override
            public HazelcastConstant apply(HazelcastConstant expectedValue) {
                return HazelcastConstant.createBooleanConstant(expectedValue.isNull());
            }

            @Override
            public HazelcastDataType[] getInputDataTypes() {
                return HazelcastDataType.values();
            }

        },
//        IS_UNKNOWN("IS UNKNOWN") {
//            @Override
//            public HazelcastConstant apply(HazelcastConstant expectedValue) {
//                return HazelcastConstant.createBooleanConstant(expectedValue.isNull());
//            }
//
//            @Override
//            public HazelcastDataType[] getInputDataTypes() {
//                return new HazelcastDataType[] { HazelcastDataType.BOOLEAN };
//            }
//        },

        IS_NOT_NULL("IS NOT NULL", "NOTNULL") {

            @Override
            public HazelcastConstant apply(HazelcastConstant expectedValue) {
                return HazelcastConstant.createBooleanConstant(!expectedValue.isNull());
            }

            @Override
            public HazelcastDataType[] getInputDataTypes() {
                return HazelcastDataType.values();
            }

        },
        IS_NOT_UNKNOWN("IS NOT UNKNOWN") {
            @Override
            public HazelcastConstant apply(HazelcastConstant expectedValue) {
                return HazelcastConstant.createBooleanConstant(!expectedValue.isNull());
            }

            @Override
            public HazelcastDataType[] getInputDataTypes() {
                return new HazelcastDataType[] { HazelcastDataType.BOOLEAN };
            }
        },
        IS_TRUE("IS TRUE") {

            @Override
            public HazelcastConstant apply(HazelcastConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return HazelcastConstant.createFalse();
                } else {
                    return HazelcastConstant
                            .createBooleanConstant(expectedValue.cast(HazelcastDataType.BOOLEAN).asBoolean());
                }
            }

            @Override
            public HazelcastDataType[] getInputDataTypes() {
                return new HazelcastDataType[] { HazelcastDataType.BOOLEAN };
            }

        },
        IS_FALSE("IS FALSE") {

            @Override
            public HazelcastConstant apply(HazelcastConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return HazelcastConstant.createFalse();
                } else {
                    return HazelcastConstant
                            .createBooleanConstant(!expectedValue.cast(HazelcastDataType.BOOLEAN).asBoolean());
                }
            }

            @Override
            public HazelcastDataType[] getInputDataTypes() {
                return new HazelcastDataType[] { HazelcastDataType.BOOLEAN };
            }

        };

        private String[] textRepresentations;

        PostfixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract HazelcastConstant apply(HazelcastConstant expectedValue);

        public abstract HazelcastDataType[] getInputDataTypes();

        public static PostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public HazelcastPostfixOperation(HazelcastExpression expr, PostfixOperator op) {
        this.expr = expr;
        this.operatorTextRepresentation = Randomly.fromOptions(op.textRepresentations);
        this.op = op;
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return HazelcastDataType.BOOLEAN;
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        HazelcastConstant expectedValue = expr.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return op.apply(expectedValue);
    }

    public String getOperatorTextRepresentation() {
        return operatorTextRepresentation;
    }

    public static HazelcastExpression create(HazelcastExpression expr, PostfixOperator op) {
        return new HazelcastPostfixOperation(expr, op);
    }

    public HazelcastExpression getExpression() {
        return expr;
    }

}
