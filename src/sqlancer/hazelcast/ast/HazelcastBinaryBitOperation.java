package sqlancer.hazelcast.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.ast.HazelcastBinaryBitOperation.HazelcastBinaryBitOperator;

public class HazelcastBinaryBitOperation extends BinaryOperatorNode<HazelcastExpression, HazelcastBinaryBitOperator>
        implements HazelcastExpression {

    public enum HazelcastBinaryBitOperator implements Operator {
        CONCATENATION("||"), //
        BITWISE_AND("&"), //
        BITWISE_OR("|"), //
        BITWISE_XOR("#"), //
        BITWISE_SHIFT_LEFT("<<"), //
        BITWISE_SHIFT_RIGHT(">>");

        private String text;

        HazelcastBinaryBitOperator(String text) {
            this.text = text;
        }

        public static HazelcastBinaryBitOperator getRandom() {
            return Randomly.fromOptions(HazelcastBinaryBitOperator.values());
        }

        @Override
        public String getTextRepresentation() {
            return text;
        }

    }

    public HazelcastBinaryBitOperation(HazelcastBinaryBitOperator op, HazelcastExpression left, HazelcastExpression right) {
        super(left, right, op);
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return HazelcastDataType.BIT;
    }

}
