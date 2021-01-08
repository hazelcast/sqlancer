package sqlancer.hazelcast.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

public class HazelcastPOSIXRegularExpression implements HazelcastExpression {

    private HazelcastExpression string;
    private HazelcastExpression regex;
    private POSIXRegex op;

    public enum POSIXRegex implements Operator {
        MATCH_CASE_SENSITIVE("~"), MATCH_CASE_INSENSITIVE("~*"), NOT_MATCH_CASE_SENSITIVE("!~"),
        NOT_MATCH_CASE_INSENSITIVE("!~*");

        private String repr;

        POSIXRegex(String repr) {
            this.repr = repr;
        }

        public String getStringRepresentation() {
            return repr;
        }

        public static POSIXRegex getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public HazelcastPOSIXRegularExpression(HazelcastExpression string, HazelcastExpression regex, POSIXRegex op) {
        this.string = string;
        this.regex = regex;
        this.op = op;
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return HazelcastDataType.BOOLEAN;
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        return null;
    }

    public HazelcastExpression getRegex() {
        return regex;
    }

    public HazelcastExpression getString() {
        return string;
    }

    public POSIXRegex getOp() {
        return op;
    }

}
