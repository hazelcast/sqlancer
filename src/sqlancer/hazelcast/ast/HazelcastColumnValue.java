package sqlancer.hazelcast.ast;

import sqlancer.hazelcast.HazelcastSchema.HazelcastColumn;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

public class HazelcastColumnValue implements HazelcastExpression {

    private final HazelcastColumn c;
    private final HazelcastConstant expectedValue;

    public HazelcastColumnValue(HazelcastColumn c, HazelcastConstant expectedValue) {
        this.c = c;
        this.expectedValue = expectedValue;
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return c.getType();
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        return expectedValue;
    }

    public static HazelcastColumnValue create(HazelcastColumn c, HazelcastConstant expected) {
        return new HazelcastColumnValue(c, expected);
    }

    public HazelcastColumn getColumn() {
        return c;
    }

}
