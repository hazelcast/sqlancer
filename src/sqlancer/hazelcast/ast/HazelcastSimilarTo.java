package sqlancer.hazelcast.ast;

import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

public class HazelcastSimilarTo implements HazelcastExpression {

    private final HazelcastExpression string;
    private final HazelcastExpression similarTo;
    private final HazelcastExpression escapeCharacter;

    public HazelcastSimilarTo(HazelcastExpression string, HazelcastExpression similarTo,
                              HazelcastExpression escapeCharacter) {
        this.string = string;
        this.similarTo = similarTo;
        this.escapeCharacter = escapeCharacter;
    }

    public HazelcastExpression getString() {
        return string;
    }

    public HazelcastExpression getSimilarTo() {
        return similarTo;
    }

    public HazelcastExpression getEscapeCharacter() {
        return escapeCharacter;
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return HazelcastDataType.BOOLEAN;
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        return null;
    }

}
