package sqlancer.hazelcast.ast;

import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

public class HazelcastFunction implements HazelcastExpression {

    private final String func;
    private final HazelcastExpression[] args;
    private final HazelcastDataType returnType;
    private HazelcastFunctionWithResult functionWithKnownResult;

    public HazelcastFunction(HazelcastFunctionWithResult func, HazelcastDataType returnType, HazelcastExpression... args) {
        functionWithKnownResult = func;
        this.func = func.getName();
        this.returnType = returnType;
        this.args = args.clone();
    }

    public HazelcastFunction(HazelcastFunctionWithUnknownResult f, HazelcastDataType returnType,
                             HazelcastExpression... args) {
        this.func = f.getName();
        this.returnType = returnType;
        this.args = args.clone();
    }

    public String getFunctionName() {
        return func;
    }

    public HazelcastExpression[] getArguments() {
        return args.clone();
    }

    public enum HazelcastFunctionWithResult {
        ABS(1, "abs") {
            @Override
            public HazelcastConstant apply(HazelcastConstant[] evaluatedArgs, HazelcastExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return HazelcastConstants.createNullConstant();
                } else {
                    return HazelcastConstants
                            .createLongConstant(Math.abs(evaluatedArgs[0].cast(HazelcastDataType.INTEGER).asInt()));
                }
            }

            @Override
            public boolean supportsReturnType(HazelcastDataType type) {
                return type == HazelcastDataType.INTEGER;
            }

            @Override
            public HazelcastDataType[] getInputTypesForReturnType(HazelcastDataType returnType, int nrArguments) {
                return new HazelcastDataType[]{returnType};
            }

        },
        SIGN(1, "sign") {
            @Override
            public HazelcastConstant apply(HazelcastConstant[] evaluatedArgs, HazelcastExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return HazelcastConstants.createNullConstant();
                } else {
                    return HazelcastConstants
                            .createLongConstant(Math.abs(evaluatedArgs[0].cast(HazelcastDataType.INTEGER).asInt()));
                }
            }

            @Override
            public boolean supportsReturnType(HazelcastDataType type) {
                return type == HazelcastDataType.INTEGER;
            }

            @Override
            public HazelcastDataType[] getInputTypesForReturnType(HazelcastDataType returnType, int nrArguments) {
                return new HazelcastDataType[]{returnType};
            }

        },
        LOWER(1, "lower") {
            @Override
            public HazelcastConstant apply(HazelcastConstant[] evaluatedArgs, HazelcastExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return HazelcastConstants.createNullConstant();
                } else {
                    String text = evaluatedArgs[0].asString();
                    return HazelcastConstants.createVarcharConstant(text.toLowerCase());
                }
            }

            @Override
            public boolean supportsReturnType(HazelcastDataType type) {
                return type == HazelcastDataType.VARCHAR;
            }

            @Override
            public HazelcastDataType[] getInputTypesForReturnType(HazelcastDataType returnType, int nrArguments) {
                return new HazelcastDataType[]{HazelcastDataType.VARCHAR};
            }

        },
        LENGTH(1, "length") {
            @Override
            public HazelcastConstant apply(HazelcastConstant[] evaluatedArgs, HazelcastExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return HazelcastConstants.createNullConstant();
                }
                String text = evaluatedArgs[0].asString();
                return HazelcastConstants.createLongConstant(text.length());
            }

            @Override
            public boolean supportsReturnType(HazelcastDataType type) {
                return type == HazelcastDataType.INTEGER;
            }

            @Override
            public HazelcastDataType[] getInputTypesForReturnType(HazelcastDataType returnType, int nrArguments) {
                return new HazelcastDataType[]{HazelcastDataType.VARCHAR};
            }
        },
        UPPER(1, "upper") {
            @Override
            public HazelcastConstant apply(HazelcastConstant[] evaluatedArgs, HazelcastExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return HazelcastConstants.createNullConstant();
                } else {
                    String text = evaluatedArgs[0].asString();
                    return HazelcastConstants.createVarcharConstant(text.toUpperCase());
                }
            }

            @Override
            public boolean supportsReturnType(HazelcastDataType type) {
                return type == HazelcastDataType.VARCHAR;
            }

            @Override
            public HazelcastDataType[] getInputTypesForReturnType(HazelcastDataType returnType, int nrArguments) {
                return new HazelcastDataType[]{HazelcastDataType.VARCHAR};
            }

        };
//        NULL_IF(2, "nullif") {
//            @Override
//            public HazelcastConstant apply(HazelcastConstant[] evaluatedArgs, HazelcastExpression[] args) {
//                HazelcastConstant equals = evaluatedArgs[0].isEquals(evaluatedArgs[1]);
//                if (equals.isBoolean() && equals.asBoolean()) {
//                    return HazelcastConstants.createNullConstant();
//                } else {
//                    // TODO: SELECT (nullif('1', FALSE)); yields '1', but should yield TRUE
//                    return evaluatedArgs[0];
//                }
//            }
//
//            @Override
//            public boolean supportsReturnType(HazelcastDataType type) {
//                return true;
//            }
//
//            @Override
//            public HazelcastDataType[] getInputTypesForReturnType(HazelcastDataType returnType, int nrArguments) {
//                return getType(nrArguments, returnType);
//            }
//
//            @Override
//            public boolean checkArguments(HazelcastExpression[] constants) {
//                for (HazelcastExpression e : constants) {
//                    if (!(e instanceof HazelcastConstants.HzNullConstant)) {
//                        return true;
//                    }
//                }
//                return false;
//            }
//
//        };

        private String functionName;
        final int nrArgs;
        private final boolean variadic;

        public HazelcastDataType[] getRandomTypes(int nr) {
            HazelcastDataType[] types = new HazelcastDataType[nr];
            for (int i = 0; i < types.length; i++) {
                types[i] = HazelcastDataType.getRandomType();
            }
            return types;
        }

        HazelcastFunctionWithResult(int nrArgs, String functionName) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = false;
        }

        /**
         * Gets the number of arguments if the function is non-variadic. If the function is variadic, the minimum number
         * of arguments is returned.
         *
         * @return the number of arguments
         */
        public int getNrArgs() {
            return nrArgs;
        }

        public abstract HazelcastConstant apply(HazelcastConstant[] evaluatedArgs, HazelcastExpression... args);

        @Override
        public String toString() {
            return functionName;
        }

        public boolean isVariadic() {
            return variadic;
        }

        public String getName() {
            return functionName;
        }

        public abstract boolean supportsReturnType(HazelcastDataType type);

        public abstract HazelcastDataType[] getInputTypesForReturnType(HazelcastDataType returnType, int nrArguments);

        public boolean checkArguments(HazelcastExpression... constants) {
            return true;
        }

    }

    @Override
    public HazelcastConstant getExpectedValue() {
        if (functionWithKnownResult == null) {
            return null;
        }
        HazelcastConstant[] constants = new HazelcastConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i] == null) {
                return null;
            }
        }
        return functionWithKnownResult.apply(constants, args);
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return returnType;
    }

}
