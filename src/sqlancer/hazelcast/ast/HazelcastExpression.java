package sqlancer.hazelcast.ast;


import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

public interface HazelcastExpression {

    default HazelcastDataType getExpressionType() {
        return null;
    }

    default HazelcastConstant getExpectedValue() {
        return null;
    }
}
