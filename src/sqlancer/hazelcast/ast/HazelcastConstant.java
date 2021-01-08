package sqlancer.hazelcast.ast;

import sqlancer.IgnoreMeException;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

import java.math.BigDecimal;

public abstract class HazelcastConstant implements HazelcastExpression {

    public abstract String getTextRepresentation();

    public abstract String getUnquotedTextRepresentation();

    public static class BooleanConstant extends HazelcastConstant {

        private final boolean value;

        public BooleanConstant(boolean value) {
            this.value = value;
        }

        @Override
        public String getTextRepresentation() {
            return value ? "TRUE" : "FALSE";
        }

        @Override
        public HazelcastDataType getExpressionType() {
            return HazelcastDataType.BOOLEAN;
        }

        @Override
        public boolean asBoolean() {
            return value;
        }

        @Override
        public boolean isBoolean() {
            return true;
        }

        @Override
        public HazelcastConstant isEquals(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return HazelcastConstant.createBooleanConstant(value == rightVal.asBoolean());
            } else if (rightVal.isString()) {
                return HazelcastConstant
                        .createBooleanConstant(value == rightVal.cast(HazelcastDataType.BOOLEAN).asBoolean());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected HazelcastConstant isLessThan(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstant.createNullConstant();
            } else if (rightVal.isString()) {
                return isLessThan(rightVal.cast(HazelcastDataType.BOOLEAN));
            } else {
                assert rightVal.isBoolean();
                return HazelcastConstant.createBooleanConstant((value ? 1 : 0) < (rightVal.asBoolean() ? 1 : 0));
            }
        }

        @Override
        public HazelcastConstant cast(HazelcastDataType type) {
            switch (type) {
            case BOOLEAN:
                return this;
            case INT:
                return HazelcastConstant.createIntConstant(value ? 1 : 0);
            case TEXT:
                return HazelcastConstant.createTextConstant(value ? "true" : "false");
            default:
                return null;
            }
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static class PostgresNullConstant extends HazelcastConstant {

        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

        @Override
        public HazelcastDataType getExpressionType() {
            return null;
        }

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public HazelcastConstant isEquals(HazelcastConstant rightVal) {
            return HazelcastConstant.createNullConstant();
        }

        @Override
        protected HazelcastConstant isLessThan(HazelcastConstant rightVal) {
            return HazelcastConstant.createNullConstant();
        }

        @Override
        public HazelcastConstant cast(HazelcastDataType type) {
            return HazelcastConstant.createNullConstant();
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static class StringConstant extends HazelcastConstant {

        private final String value;

        public StringConstant(String value) {
            this.value = value;
        }

        @Override
        public String getTextRepresentation() {
            return String.format("'%s'", value.replace("'", "''"));
        }

        @Override
        public HazelcastConstant isEquals(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return cast(HazelcastDataType.INT).isEquals(rightVal.cast(HazelcastDataType.INT));
            } else if (rightVal.isBoolean()) {
                return cast(HazelcastDataType.BOOLEAN).isEquals(rightVal.cast(HazelcastDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return HazelcastConstant.createBooleanConstant(value.contentEquals(rightVal.asString()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected HazelcastConstant isLessThan(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return cast(HazelcastDataType.INT).isLessThan(rightVal.cast(HazelcastDataType.INT));
            } else if (rightVal.isBoolean()) {
                return cast(HazelcastDataType.BOOLEAN).isLessThan(rightVal.cast(HazelcastDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return HazelcastConstant.createBooleanConstant(value.compareTo(rightVal.asString()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public HazelcastConstant cast(HazelcastDataType type) {
            if (type == HazelcastDataType.TEXT) {
                return this;
            }
            String s = value.trim();
            switch (type) {
            case BOOLEAN:
                try {
                    return HazelcastConstant.createBooleanConstant(Long.parseLong(s) != 0);
                } catch (NumberFormatException e) {
                }
                switch (s.toUpperCase()) {
                case "T":
                case "TR":
                case "TRU":
                case "TRUE":
                case "1":
                case "YES":
                case "YE":
                case "Y":
                case "ON":
                    return HazelcastConstant.createTrue();
                case "F":
                case "FA":
                case "FAL":
                case "FALS":
                case "FALSE":
                case "N":
                case "NO":
                case "OF":
                case "OFF":
                default:
                    return HazelcastConstant.createFalse();
                }
            case INT:
                try {
                    return HazelcastConstant.createIntConstant(Long.parseLong(s));
                } catch (NumberFormatException e) {
                    return HazelcastConstant.createIntConstant(-1);
                }
            case TEXT:
                return this;
            default:
                return null;
            }
        }

        @Override
        public HazelcastDataType getExpressionType() {
            return HazelcastDataType.TEXT;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public String asString() {
            return value;
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return value;
        }

    }

    public static class IntConstant extends HazelcastConstant {

        private final long val;

        public IntConstant(long val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public HazelcastDataType getExpressionType() {
            return HazelcastDataType.INT;
        }

        @Override
        public long asInt() {
            return val;
        }

        @Override
        public boolean isInt() {
            return true;
        }

        @Override
        public HazelcastConstant isEquals(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return cast(HazelcastDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isInt()) {
                return HazelcastConstant.createBooleanConstant(val == rightVal.asInt());
            } else if (rightVal.isString()) {
                return HazelcastConstant.createBooleanConstant(val == rightVal.cast(HazelcastDataType.INT).asInt());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected HazelcastConstant isLessThan(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return HazelcastConstant.createBooleanConstant(val < rightVal.asInt());
            } else if (rightVal.isBoolean()) {
                throw new AssertionError(rightVal);
            } else if (rightVal.isString()) {
                return HazelcastConstant.createBooleanConstant(val < rightVal.cast(HazelcastDataType.INT).asInt());
            } else {
                throw new IgnoreMeException();
            }

        }

        @Override
        public HazelcastConstant cast(HazelcastDataType type) {
            switch (type) {
            case BOOLEAN:
                return HazelcastConstant.createBooleanConstant(val != 0);
            case INT:
                return this;
            case TEXT:
                return HazelcastConstant.createTextConstant(String.valueOf(val));
            default:
                return null;
            }
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static HazelcastConstant createNullConstant() {
        return new PostgresNullConstant();
    }

    public String asString() {
        throw new UnsupportedOperationException(this.toString());
    }

    public boolean isString() {
        return false;
    }

    public static HazelcastConstant createIntConstant(long val) {
        return new IntConstant(val);
    }

    public static HazelcastConstant createBooleanConstant(boolean val) {
        return new BooleanConstant(val);
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

    public static HazelcastConstant createFalse() {
        return createBooleanConstant(false);
    }

    public static HazelcastConstant createTrue() {
        return createBooleanConstant(true);
    }

    public long asInt() {
        throw new UnsupportedOperationException(this.toString());
    }

    public boolean isBoolean() {
        return false;
    }

    public abstract HazelcastConstant isEquals(HazelcastConstant rightVal);

    public boolean isInt() {
        return false;
    }

    protected abstract HazelcastConstant isLessThan(HazelcastConstant rightVal);

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract HazelcastConstant cast(HazelcastDataType type);

    public static HazelcastConstant createTextConstant(String string) {
        return new StringConstant(string);
    }

    public abstract static class PostgresConstantBase extends HazelcastConstant {

        @Override
        public String getUnquotedTextRepresentation() {
            return null;
        }

        @Override
        public HazelcastConstant isEquals(HazelcastConstant rightVal) {
            return null;
        }

        @Override
        protected HazelcastConstant isLessThan(HazelcastConstant rightVal) {
            return null;
        }

        @Override
        public HazelcastConstant cast(HazelcastDataType type) {
            return null;
        }
    }

    public static class DecimalConstant extends PostgresConstantBase {

        private final BigDecimal val;

        public DecimalConstant(BigDecimal val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public HazelcastDataType getExpressionType() {
            return HazelcastDataType.DECIMAL;
        }

    }

    public static class InetConstant extends PostgresConstantBase {

        private final String val;

        public InetConstant(String val) {
            this.val = "'" + val + "'";
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public HazelcastDataType getExpressionType() {
            return HazelcastDataType.INET;
        }

    }

    public static class FloatConstant extends PostgresConstantBase {

        private final float val;

        public FloatConstant(float val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            if (Double.isFinite(val)) {
                return String.valueOf(val);
            } else {
                return "'" + val + "'";
            }
        }

        @Override
        public HazelcastDataType getExpressionType() {
            return HazelcastDataType.FLOAT;
        }

    }

    public static class DoubleConstant extends PostgresConstantBase {

        private final double val;

        public DoubleConstant(double val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            if (Double.isFinite(val)) {
                return String.valueOf(val);
            } else {
                return "'" + val + "'";
            }
        }

        @Override
        public HazelcastDataType getExpressionType() {
            return HazelcastDataType.FLOAT;
        }

    }

    public static class BitConstant extends PostgresConstantBase {

        private final long val;

        public BitConstant(long val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.format("B'%s'", Long.toBinaryString(val));
        }

        @Override
        public HazelcastDataType getExpressionType() {
            return HazelcastDataType.BIT;
        }

    }

    public static class RangeConstant extends PostgresConstantBase {

        private final long left;
        private final boolean leftIsInclusive;
        private final long right;
        private final boolean rightIsInclusive;

        public RangeConstant(long left, boolean leftIsInclusive, long right, boolean rightIsInclusive) {
            this.left = left;
            this.leftIsInclusive = leftIsInclusive;
            this.right = right;
            this.rightIsInclusive = rightIsInclusive;
        }

        @Override
        public String getTextRepresentation() {
            StringBuilder sb = new StringBuilder();
            sb.append("'");
            if (leftIsInclusive) {
                sb.append("[");
            } else {
                sb.append("(");
            }
            sb.append(left);
            sb.append(",");
            sb.append(right);
            if (rightIsInclusive) {
                sb.append("]");
            } else {
                sb.append(")");
            }
            sb.append("'");
            sb.append("::int4range");
            return sb.toString();
        }

        @Override
        public HazelcastDataType getExpressionType() {
            return HazelcastDataType.RANGE;
        }

    }

    public static HazelcastConstant createDecimalConstant(BigDecimal bigDecimal) {
        return new DecimalConstant(bigDecimal);
    }

    public static HazelcastConstant createFloatConstant(float val) {
        return new FloatConstant(val);
    }

    public static HazelcastConstant createDoubleConstant(double val) {
        return new DoubleConstant(val);
    }

    public static HazelcastConstant createRange(long left, boolean leftIsInclusive, long right,
                                                boolean rightIsInclusive) {
        long realLeft;
        long realRight;
        if (left > right) {
            realRight = left;
            realLeft = right;
        } else {
            realLeft = left;
            realRight = right;
        }
        return new RangeConstant(realLeft, leftIsInclusive, realRight, rightIsInclusive);
    }

    public static HazelcastExpression createBitConstant(long integer) {
        return new BitConstant(integer);
    }

    public static HazelcastExpression createInetConstant(String val) {
        return new InetConstant(val);
    }

}
