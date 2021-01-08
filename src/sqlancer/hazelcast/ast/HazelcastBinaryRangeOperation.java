package sqlancer.hazelcast.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

public class HazelcastBinaryRangeOperation extends BinaryNode<HazelcastExpression> implements HazelcastExpression {

    private final String op;

    public enum HazelcastBinaryRangeOperator implements Operator {
        UNION("*"), INTERSECTION("*"), DIFFERENCE("-");

        private final String textRepresentation;

        HazelcastBinaryRangeOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static HazelcastBinaryRangeOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum HazelcastBinaryRangeComparisonOperator {
        CONTAINS_RANGE_OR_ELEMENT("@>"), RANGE_OR_ELEMENT_IS_CONTAINED("<@"), OVERLAP("&&"), STRICT_LEFT_OF("<<"),
        STRICT_RIGHT_OF(">>"), NOT_RIGHT_OF("&<"), NOT_LEFT_OF(">&"), ADJACENT("-|-");

        private final String textRepresentation;

        HazelcastBinaryRangeComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static HazelcastBinaryRangeComparisonOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public HazelcastBinaryRangeOperation(HazelcastBinaryRangeComparisonOperator op, HazelcastExpression left,
                                         HazelcastExpression right) {
        super(left, right);
        this.op = op.getTextRepresentation();
    }

    public HazelcastBinaryRangeOperation(HazelcastBinaryRangeOperator op, HazelcastExpression left,
                                         HazelcastExpression right) {
        super(left, right);
        this.op = op.getTextRepresentation();
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return HazelcastDataType.BOOLEAN;
    }

    @Override
    public String getOperatorRepresentation() {
        return op;
    }

}
