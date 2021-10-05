package sqlancer.hazelcast.ast;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public abstract class HazelcastConstants {

    public static Set<Long> usedKeyCache = new HashSet<>();

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
                return HazelcastConstants.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return HazelcastConstants.createBooleanConstant(value == rightVal.asBoolean());
            } else if (rightVal.isString()) {
                return HazelcastConstants
                        .createBooleanConstant(value == rightVal.cast(HazelcastDataType.BOOLEAN).asBoolean());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected HazelcastConstant isLessThan(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstants.createNullConstant();
            } else if (rightVal.isString()) {
                return isLessThan(rightVal.cast(HazelcastDataType.BOOLEAN));
            } else {
                assert rightVal.isBoolean();
                return HazelcastConstants.createBooleanConstant((value ? 1 : 0) < (rightVal.asBoolean() ? 1 : 0));
            }
        }

        @Override
        public HazelcastConstant cast(HazelcastDataType type) {
            switch (type) {
                case BOOLEAN:
                    return this;
                case VARCHAR:
                    return createVarcharConstant(getTextRepresentation());
            }
            return null;
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static class HzNullConstant extends HazelcastConstant {

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
            return HazelcastConstants.createNullConstant();
        }

        @Override
        protected HazelcastConstant isLessThan(HazelcastConstant rightVal) {
            return HazelcastConstants.createNullConstant();
        }

        @Override
        public HazelcastConstant cast(HazelcastDataType type) {
            return HazelcastConstants.createNullConstant();
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
                return HazelcastConstants.createNullConstant();
            } else if (rightVal.isInt()) {
                return cast(HazelcastDataType.INTEGER).isEquals(rightVal.cast(HazelcastDataType.INTEGER));
            } else if (rightVal.isBoolean()) {
                return cast(HazelcastDataType.BOOLEAN).isEquals(rightVal.cast(HazelcastDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return HazelcastConstants.createBooleanConstant(value.contentEquals(rightVal.asString()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected HazelcastConstant isLessThan(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstants.createNullConstant();
            } else if (rightVal.isInt()) {
                return cast(HazelcastDataType.INTEGER).isLessThan(rightVal.cast(HazelcastDataType.INTEGER));
            } else if (rightVal.isBoolean()) {
                return cast(HazelcastDataType.BOOLEAN).isLessThan(rightVal.cast(HazelcastDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return HazelcastConstants.createBooleanConstant(value.compareTo(rightVal.asString()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public HazelcastConstant cast(HazelcastDataType type) {
            String s = value.trim();
            switch (type) {
                case BOOLEAN:
                    try {
                        return HazelcastConstants.createBooleanConstant(Long.parseLong(s) != 0);
                    } catch (NumberFormatException e) {
                    }
                    switch (s.toUpperCase()) {
                        case "TRUE":
                            return HazelcastConstants.createTrue();
                        case "FALSE":
                        default:
                            return HazelcastConstants.createFalse();
                    }
                case INTEGER:
                    try {
                        long asInt = Long.parseLong(s);
                        return HazelcastConstants.createLongConstant(asInt);
                    } catch (NumberFormatException e) {
                        return null;
                    }
                case VARCHAR:
                    return this;
                default:
                    return null;
            }
        }

        @Override
        public HazelcastDataType getExpressionType() {
            return HazelcastDataType.VARCHAR;
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

//    public static class SmallIntConstant extends HazelcastConstant {
//        private final short val;
//
//        public SmallIntConstant(short val) {
//            this.val = val;
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return String.valueOf(val);
//        }
//
//        @Override
//        public HazelcastDataType getExpressionType() {
//            return HazelcastDataType.SMALLINT;
//        }
//
//        @Override
//        public long asInt() {
//            return val;
//        }
//
//        @Override
//        public boolean isInt() {
//            return true;
//        }
//
//        @Override
//        public HazelcastConstant isEquals(HazelcastConstant rightVal) {
//            if (rightVal.isNull()) {
//                return HazelcastConstants.createNullConstant();
//            } else if (rightVal.isBoolean()) {
//                return cast(HazelcastDataType.BOOLEAN).isEquals(rightVal);
//            } else if (rightVal.isInt()) {
//                return HazelcastConstants.createBooleanConstant(val == rightVal.asInt());
//            } else if (rightVal.isString()) {
//                return HazelcastConstants.createBooleanConstant(val == rightVal.cast(HazelcastDataType.INTEGER).asInt());
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        protected HazelcastConstant isLessThan(HazelcastConstant rightVal) {
//            if (rightVal.isNull()) {
//                return HazelcastConstants.createNullConstant();
//            } else if (rightVal.isInt()) {
//                return HazelcastConstants.createBooleanConstant(val < rightVal.asInt());
//            } else if (rightVal.isBoolean()) {
//                throw new AssertionError(rightVal);
//            } else if (rightVal.isString()) {
//                return HazelcastConstants.createBooleanConstant(val < rightVal.cast(HazelcastDataType.INTEGER).asInt());
//            } else {
//                throw new IgnoreMeException();
//            }
//
//        }
//
//        @Override
//        public HazelcastConstant cast(HazelcastDataType type) {
//            switch (type) {
//                case BOOLEAN:
//                    return HazelcastConstants.createBooleanConstant(val != 0);
//                case SMALLINT:
//                    return createSmallIntConstant(val);
//                case INTEGER:
//                    return HazelcastConstants.createIntConstant(val);
//                case VARCHAR:
//                    return HazelcastConstants.createVarcharConstant(String.valueOf(val));
//                default:
//                    return null;
//            }
//        }
//
//        @Override
//        public String getUnquotedTextRepresentation() {
//            return getTextRepresentation();
//        }
//
//    }

    public static class IntConstant extends HazelcastConstant {
        private final long val;

        public IntConstant(long val) {
            this.val = val;
        }

        public IntConstant(int val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public HazelcastDataType getExpressionType() {
            return HazelcastDataType.INTEGER;
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
                return HazelcastConstants.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return cast(HazelcastDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isInt()) {
                return HazelcastConstants.createBooleanConstant(val == rightVal.asInt());
            } else if (rightVal.isString()) {
                return HazelcastConstants.createBooleanConstant(val == rightVal.cast(HazelcastDataType.INTEGER).asInt());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected HazelcastConstant isLessThan(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstants.createNullConstant();
            } else if (rightVal.isInt()) {
                return HazelcastConstants.createBooleanConstant(val < rightVal.asInt());
            } else if (rightVal.isBoolean()) {
                throw new AssertionError(rightVal);
            } else if (rightVal.isString()) {
                return HazelcastConstants.createBooleanConstant(val < rightVal.cast(HazelcastDataType.INTEGER).asInt());
            } else {
                throw new IgnoreMeException();
            }

        }

        @Override
        public HazelcastConstant cast(HazelcastDataType type) {
            switch (type) {
                case BOOLEAN:
                    return HazelcastConstants.createBooleanConstant(val != 0);
                case INTEGER:
                    return this;
                case VARCHAR:
                    return HazelcastConstants.createVarcharConstant(String.valueOf(val));
                default:
                    return null;
            }
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static class DecimalConstant extends HazelcastConstant {

        private final BigDecimal val;

        public DecimalConstant(BigDecimal val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

        @Override
        public HazelcastConstant isEquals(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstants.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return cast(HazelcastDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isInt()) {
                return HazelcastConstants.createBooleanConstant(val.compareTo(new BigDecimal(rightVal.asInt())) == 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected HazelcastConstant isLessThan(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstants.createNullConstant();
            } else if (rightVal.isInt()) {
                return HazelcastConstants.createBooleanConstant(val.compareTo(new BigDecimal(rightVal.asInt())) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public HazelcastConstant cast(HazelcastDataType type) {
            switch (type) {
                case BOOLEAN:
                    return HazelcastConstants.createBooleanConstant(val.intValue() != 0);
                case INTEGER:
                    return HazelcastConstants.createIntConstant(val.intValue());
                default:
                    return null;
            }

        }

        @Override
        public HazelcastDataType getExpressionType() {
            return HazelcastDataType.DECIMAL;
        }
    }

    public static class FloatConstant extends HazelcastConstant {

        private final float val;

        public FloatConstant(float val) {
            this.val = val;
        }

        @Override
        public double asFloat() {
            return val;
        }

        @Override
        public long asInt() {
            return new Float(val).longValue();
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
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

        @Override
        public HazelcastConstant isEquals(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstants.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return cast(HazelcastDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isInt()) {
                return HazelcastConstants.createBooleanConstant(asInt() == rightVal.asInt());
            } else if (rightVal.isFloat()) {
                return HazelcastConstants.createBooleanConstant(Double.compare(asFloat(), rightVal.asFloat()) == 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected HazelcastConstant isLessThan(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstants.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return cast(HazelcastDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isInt()) {
                return HazelcastConstants.createBooleanConstant(asInt() < rightVal.asInt());
            } else if (rightVal.isFloat()) {
                return HazelcastConstants.createBooleanConstant(Double.compare(asFloat(), rightVal.asFloat()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public HazelcastConstant cast(HazelcastDataType type) {
            switch (type) {
                case BOOLEAN:
                    return HazelcastConstants.createBooleanConstant(val != 0);
                case INTEGER:
                    return HazelcastConstants.createIntConstant(new Float(val).intValue());
                default:
                    return null;
            }

        }

        @Override
        public HazelcastDataType getExpressionType() {
            return HazelcastDataType.FLOAT;
        }

        @Override
        public boolean isFloat() {
            return true;
        }
    }

    public static class DoubleConstant extends HazelcastConstant {

        private final double val;

        public DoubleConstant(double val) {
            this.val = val;
        }

        @Override
        public double asFloat() {
            return val;
        }

        @Override
        public long asInt() {
            return new Double(val).longValue();
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
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

        @Override
        public HazelcastConstant isEquals(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstants.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return cast(HazelcastDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isInt()) {
                return HazelcastConstants.createBooleanConstant(asInt() == rightVal.asInt());
            } else if (rightVal.isFloat()) {
                return HazelcastConstants.createBooleanConstant(Double.compare(asFloat(), rightVal.asFloat()) == 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected HazelcastConstant isLessThan(HazelcastConstant rightVal) {
            if (rightVal.isNull()) {
                return HazelcastConstants.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return cast(HazelcastDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isInt()) {
                return HazelcastConstants.createBooleanConstant(asInt() < rightVal.asInt());
            } else if (rightVal.isFloat()) {
                return HazelcastConstants.createBooleanConstant(Double.compare(asFloat(), rightVal.asFloat()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public HazelcastConstant cast(HazelcastDataType type) {
            switch (type) {
                case BOOLEAN:
                    return HazelcastConstants.createBooleanConstant(val != 0);
                case INTEGER:
                    return HazelcastConstants.createIntConstant(new Float(val).intValue());
                default:
                    return null;
            }

        }

        @Override
        public HazelcastDataType getExpressionType() {
            return HazelcastDataType.DOUBLE;
        }

        @Override
        public boolean isFloat() {
            return true;
        }

    }

    public static HazelcastConstant createNullConstant() {
        return new HzNullConstant();
    }

    public static HazelcastConstant createSmallIntConstant(short val) {
//        return new SmallIntConstant(val);
        return new IntConstant(val);
    }

    public static HazelcastConstant createIntConstant(int val) {
        return new IntConstant(val);
    }

    public static HazelcastConstant createIntConstant(long val) {
        return createLongConstant(val);
    }

    public static HazelcastConstant createLongConstant(long val) {
        long nextInt = val;
        // To reduce count of duplicated keys and to enhance entropy, we will use keys cache :)
        while (usedKeyCache.contains(nextInt)) {
            long next = (int) Randomly.getNotCachedInteger(0, (Integer.MAX_VALUE));
            nextInt = next;
        }
        usedKeyCache.add(nextInt);
        return new IntConstant(nextInt);
    }

    public static HazelcastConstant createBooleanConstant(boolean val) {
        return new BooleanConstant(val);
    }

    public static HazelcastConstant createFalse() {
        return createBooleanConstant(false);
    }

    public static HazelcastConstant createTrue() {
        return createBooleanConstant(true);
    }

    public static HazelcastConstant createVarcharConstant(String string) {
        return new StringConstant(string);
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

}
