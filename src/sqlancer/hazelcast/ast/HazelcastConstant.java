package sqlancer.hazelcast.ast;

import sqlancer.IgnoreMeException;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

import java.math.BigDecimal;

// TODO: [sasha] Refactor this...
public abstract class HazelcastConstant implements HazelcastExpression {

    public abstract String getTextRepresentation();

    public abstract String getUnquotedTextRepresentation();

    public String asString() {
        throw new UnsupportedOperationException(this.toString());
    }

    public boolean isString() {
        return false;
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        return this;
    }

    public boolean isNull() {
        return false;
    }

    public boolean asBoolean() {
        throw new UnsupportedOperationException(this.toString());
    }

    public long asInt() {
        throw new UnsupportedOperationException(this.toString());
    }

    public double asFloat() {
        throw new UnsupportedOperationException(this.toString());
    }

    public boolean isBoolean() {
        return false;
    }

    public abstract HazelcastConstant isEquals(HazelcastConstant rightVal);

    public boolean isInt() {
        return false;
    }

    public boolean isFloat() {
        return false;
    }

    protected abstract HazelcastConstant isLessThan(HazelcastConstant rightVal);

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract HazelcastConstant cast(HazelcastDataType type);
}
