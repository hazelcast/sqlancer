package sqlancer.hazelcast.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.hazelcast.HazelcastCompoundDataType;
import sqlancer.hazelcast.HazelcastGlobalState;
import sqlancer.hazelcast.HazelcastProvider;
import sqlancer.hazelcast.HazelcastSchema.HazelcastColumn;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.HazelcastSchema.HazelcastRowValue;
import sqlancer.hazelcast.ast.HazelcastAggregate;
import sqlancer.hazelcast.ast.HazelcastAggregate.HazelcastAggregateFunction;
import sqlancer.hazelcast.ast.HazelcastBetweenOperation;
import sqlancer.hazelcast.ast.HazelcastBinaryArithmeticOperation;
import sqlancer.hazelcast.ast.HazelcastBinaryArithmeticOperation.HazelcastBinaryOperator;
import sqlancer.hazelcast.ast.HazelcastBinaryComparisonOperation;
import sqlancer.hazelcast.ast.HazelcastBinaryLogicalOperation;
import sqlancer.hazelcast.ast.HazelcastBinaryLogicalOperation.BinaryLogicalOperator;
import sqlancer.hazelcast.ast.HazelcastCastOperation;
import sqlancer.hazelcast.ast.HazelcastCollate;
import sqlancer.hazelcast.ast.HazelcastColumnValue;
import sqlancer.hazelcast.ast.HazelcastConcatOperation;
import sqlancer.hazelcast.ast.HazelcastConstant;
import sqlancer.hazelcast.ast.HazelcastExpression;
import sqlancer.hazelcast.ast.HazelcastFunction;
import sqlancer.hazelcast.ast.HazelcastFunction.HazelcastFunctionWithResult;
import sqlancer.hazelcast.ast.HazelcastFunctionWithUnknownResult;
import sqlancer.hazelcast.ast.HazelcastInOperation;
import sqlancer.hazelcast.ast.HazelcastLikeOperation;
import sqlancer.hazelcast.ast.HazelcastOrderByTerm;
import sqlancer.hazelcast.ast.HazelcastOrderByTerm.HazelcastOrder;
import sqlancer.hazelcast.ast.HazelcastPOSIXRegularExpression;
import sqlancer.hazelcast.ast.HazelcastPOSIXRegularExpression.POSIXRegex;
import sqlancer.hazelcast.ast.HazelcastPostfixOperation;
import sqlancer.hazelcast.ast.HazelcastPostfixOperation.PostfixOperator;
import sqlancer.hazelcast.ast.HazelcastPrefixOperation;
import sqlancer.hazelcast.ast.HazelcastPrefixOperation.PrefixOperator;
import sqlancer.hazelcast.ast.HazelcastSimilarTo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HazelcastExpressionGenerator implements ExpressionGenerator<HazelcastExpression> {

    private final int maxDepth;

    private final Randomly r;

    private List<HazelcastColumn> columns;

    private HazelcastRowValue rw;

    private boolean expectedResult;

    private HazelcastGlobalState globalState;

    private boolean allowAggregateFunctions;

    private final Map<String, Character> functionsAndTypes;

    private final List<Character> allowedFunctionTypes;

    public HazelcastExpressionGenerator(HazelcastGlobalState globalState) {
        this.r = globalState.getRandomly();
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
        this.globalState = globalState;
        this.functionsAndTypes = globalState.getFunctionsAndTypes();
        this.allowedFunctionTypes = globalState.getAllowedFunctionTypes();
    }

    public HazelcastExpressionGenerator setColumns(List<HazelcastColumn> columns) {
        this.columns = columns;
        return this;
    }

    public HazelcastExpressionGenerator setRowValue(HazelcastRowValue rw) {
        this.rw = rw;
        return this;
    }

    public HazelcastExpression generateExpression(int depth) {
        return generateExpression(depth, HazelcastDataType.getRandomType());
    }

    public List<HazelcastExpression> generateOrderBy() {
        List<HazelcastExpression> orderBys = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            orderBys.add(new HazelcastOrderByTerm(HazelcastColumnValue.create(Randomly.fromList(columns), null),
                    HazelcastOrder.getRandomOrder()));
        }
        return orderBys;
    }

    private enum BooleanExpression {
        POSTFIX_OPERATOR, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, FUNCTION, CAST, LIKE, BETWEEN, IN_OPERATION,
        SIMILAR_TO, POSIX_REGEX
    }

    private HazelcastExpression generateFunctionWithUnknownResult(int depth, HazelcastDataType type) {
        List<HazelcastFunctionWithUnknownResult> supportedFunctions = HazelcastFunctionWithUnknownResult
                .getSupportedFunctions(type);
        // filters functions by allowed type (STABLE 's', IMMUTABLE 'i', VOLATILE 'v')
        supportedFunctions = supportedFunctions.stream()
                .filter(f -> allowedFunctionTypes.contains(functionsAndTypes.get(f.getName())))
                .collect(Collectors.toList());
        if (supportedFunctions.isEmpty()) {
            throw new IgnoreMeException();
        }
        HazelcastFunctionWithUnknownResult randomFunction = Randomly.fromList(supportedFunctions);
        return new HazelcastFunction(randomFunction, type, randomFunction.getArguments(type, this, depth + 1));
    }

    private HazelcastExpression generateFunctionWithKnownResult(int depth, HazelcastDataType type) {
        List<HazelcastFunctionWithResult> functions = Stream.of(HazelcastFunctionWithResult.values())
                .filter(f -> f.supportsReturnType(type)).collect(Collectors.toList());
        // filters functions by allowed type (STABLE 's', IMMUTABLE 'i', VOLATILE 'v')
        functions = functions.stream().filter(f -> allowedFunctionTypes.contains(functionsAndTypes.get(f.getName())))
                .collect(Collectors.toList());
        if (functions.isEmpty()) {
            throw new IgnoreMeException();
        }
        HazelcastFunctionWithResult randomFunction = Randomly.fromList(functions);
        int nrArgs = randomFunction.getNrArgs();
        if (randomFunction.isVariadic()) {
            nrArgs += Randomly.smallNumber();
        }
        HazelcastDataType[] argTypes = randomFunction.getInputTypesForReturnType(type, nrArgs);
        HazelcastExpression[] args = new HazelcastExpression[nrArgs];
        do {
            for (int i = 0; i < args.length; i++) {
                args[i] = generateExpression(depth + 1, argTypes[i]);
            }
        } while (!randomFunction.checkArguments(args));
        return new HazelcastFunction(randomFunction, type, args);
    }

    private HazelcastExpression generateBooleanExpression(int depth) {
        List<BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(
                BooleanExpression.LIKE, BooleanExpression.NOT, BooleanExpression.BETWEEN));
        if (HazelcastProvider.generateOnlyKnown) {
            validOptions.remove(BooleanExpression.SIMILAR_TO);
            validOptions.remove(BooleanExpression.POSIX_REGEX);
        }
        BooleanExpression option = Randomly.fromList(validOptions);
        switch (option) {
            case POSTFIX_OPERATOR:
                PostfixOperator random = PostfixOperator.getRandom();
                return HazelcastPostfixOperation
                        .create(generateExpression(depth + 1, Randomly.fromOptions(random.getInputDataTypes())), random);
            case IN_OPERATION:
                return inOperation(depth + 1);
            case NOT:
                return new HazelcastPrefixOperation(generateExpression(depth + 1, HazelcastDataType.BOOLEAN),
                        PrefixOperator.NOT);
            case BINARY_LOGICAL_OPERATOR:
                HazelcastExpression first = generateExpression(depth + 1, HazelcastDataType.BOOLEAN);
                int nr = Randomly.smallNumber() + 1;
                for (int i = 0; i < nr; i++) {
                    first = new HazelcastBinaryLogicalOperation(first,
                            generateExpression(depth + 1, HazelcastDataType.BOOLEAN), BinaryLogicalOperator.getRandom());
                }
                return first;
            case BINARY_COMPARISON:
                HazelcastDataType dataType = getMeaningfulType();
                return generateComparison(depth, dataType);
            case CAST:
                return new HazelcastCastOperation(generateExpression(depth + 1),
                        getCompoundDataType(HazelcastDataType.BOOLEAN));
            case FUNCTION:
                return generateFunction(depth + 1, HazelcastDataType.BOOLEAN);
            case LIKE:
                return new HazelcastLikeOperation(generateExpression(depth + 1, HazelcastDataType.TEXT),
                        generateExpression(depth + 1, HazelcastDataType.TEXT));
            case BETWEEN:
                HazelcastDataType type = getMeaningfulType();
                return new HazelcastBetweenOperation(generateExpression(depth + 1, type),
                        generateExpression(depth + 1, type), generateExpression(depth + 1, type), Randomly.getBoolean());
            case SIMILAR_TO:
                assert !expectedResult;
                // TODO also generate the escape character
                return new HazelcastSimilarTo(generateExpression(depth + 1, HazelcastDataType.TEXT),
                        generateExpression(depth + 1, HazelcastDataType.TEXT), null);
            case POSIX_REGEX:
                assert !expectedResult;
                return new HazelcastPOSIXRegularExpression(generateExpression(depth + 1, HazelcastDataType.TEXT),
                        generateExpression(depth + 1, HazelcastDataType.TEXT), POSIXRegex.getRandom());
            default:
                throw new AssertionError();
        }
    }

    private HazelcastDataType getMeaningfulType() {
        // make it more likely that the expression does not only consist of constant
        // expressions
        if (Randomly.getBooleanWithSmallProbability() || columns == null || columns.isEmpty()) {
            return HazelcastDataType.getRandomType();
        } else {
            return Randomly.fromList(columns).getType();
        }
    }

    private HazelcastExpression generateFunction(int depth, HazelcastDataType type) {
        if (HazelcastProvider.generateOnlyKnown || Randomly.getBoolean()) {
            return generateFunctionWithKnownResult(depth, type);
        } else {
            return generateFunctionWithUnknownResult(depth, type);
        }
    }

    private HazelcastExpression generateComparison(int depth, HazelcastDataType dataType) {
        HazelcastExpression leftExpr = generateExpression(depth + 1, dataType);
        HazelcastExpression rightExpr = generateExpression(depth + 1, dataType);
        return getComparison(leftExpr, rightExpr);
    }

    private HazelcastExpression getComparison(HazelcastExpression leftExpr, HazelcastExpression rightExpr) {
        HazelcastBinaryComparisonOperation op = new HazelcastBinaryComparisonOperation(leftExpr, rightExpr,
                HazelcastBinaryComparisonOperation.HazelcastBinaryComparisonOperator.getRandom());
        if (HazelcastProvider.generateOnlyKnown && op.getLeft().getExpressionType() == HazelcastDataType.TEXT
                && op.getRight().getExpressionType() == HazelcastDataType.TEXT) {
            return new HazelcastCollate(op, "C");
        }
        return op;
    }

    private HazelcastExpression inOperation(int depth) {
        HazelcastDataType type = HazelcastDataType.getRandomType();
        HazelcastExpression leftExpr = generateExpression(depth + 1, type);
        List<HazelcastExpression> rightExpr = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            rightExpr.add(generateExpression(depth + 1, type));
        }
        return new HazelcastInOperation(leftExpr, rightExpr, Randomly.getBoolean());
    }

    public static HazelcastExpression generateExpression(HazelcastGlobalState globalState, HazelcastDataType type) {
        return new HazelcastExpressionGenerator(globalState).generateExpression(0, type);
    }

    public HazelcastExpression generateExpression(int depth, HazelcastDataType originalType) {
        HazelcastDataType dataType = originalType;
        if (dataType == HazelcastDataType.FLOAT) {
            dataType = HazelcastDataType.INT;
        }
        if (!filterColumns(dataType).isEmpty() && Randomly.getBoolean()) {
            return potentiallyWrapInCollate(dataType, createColumnOfType(dataType));
        }
        HazelcastExpression exprInternal = generateExpressionInternal(depth, dataType);
        return potentiallyWrapInCollate(dataType, exprInternal);
    }

    private HazelcastExpression potentiallyWrapInCollate(HazelcastDataType dataType, HazelcastExpression exprInternal) {
        if (dataType == HazelcastDataType.TEXT && HazelcastProvider.generateOnlyKnown) {
            return new HazelcastCollate(exprInternal, "C");
        } else {
            return exprInternal;
        }
    }

    private HazelcastExpression generateExpressionInternal(int depth, HazelcastDataType dataType) throws AssertionError {
        if (allowAggregateFunctions && Randomly.getBoolean()) {
            allowAggregateFunctions = false; // aggregate function calls cannot be nested
            return getAggregate(dataType);
        }
        if (Randomly.getBooleanWithRatherLowProbability() || depth > maxDepth) {
            // generic expression
            if (Randomly.getBoolean() || depth > maxDepth) {
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    return generateConstant(r, dataType);
                } else {
                    if (filterColumns(dataType).isEmpty()) {
                        return generateConstant(r, dataType);
                    } else {
                        return createColumnOfType(dataType);
                    }
                }
            } else {
                if (Randomly.getBoolean()) {
                    return new HazelcastCastOperation(generateExpression(depth + 1), getCompoundDataType(dataType));
                } else {
                    return generateFunctionWithUnknownResult(depth, dataType);
                }
            }
        } else {
            switch (dataType) {
                case BOOLEAN:
                    return generateBooleanExpression(depth);
                case INT:
                    return generateIntExpression(depth);
                case SMALLINT:
                    return generateIntExpression(depth);
                case TEXT:
                    return generateTextExpression(depth);
                case DECIMAL:
                case FLOAT:
                    return generateIntExpression(depth);
                default:
                    throw new AssertionError(dataType);
            }
        }
    }

    private static HazelcastCompoundDataType getCompoundDataType(HazelcastDataType type) {
        switch (type) {
            case BOOLEAN:
            case DECIMAL: // TODO
            case FLOAT:
            case INT:
//        case RANGE:
//        case REAL:
//        case INET:
            case TEXT: // TODO
                return HazelcastCompoundDataType.create(type);
//        case BIT:
//            if (Randomly.getBoolean() || HazelcastProvider.generateOnlyKnown /*
//                                                                             * The PQS implementation does not check for
//                                                                             * size specifications
//                                                                             */) {
//                return HazelcastCompoundDataType.create(type);
//            } else {
//                return HazelcastCompoundDataType.create(type, (int) Randomly.getNotCachedInteger(1, 1000));
//            }
            default:
                throw new AssertionError(type);
        }

    }

    private enum RangeExpression {
        BINARY_OP;
    }

//    private HazelcastExpression generateRangeExpression(int depth) {
//        RangeExpression option;
//        List<RangeExpression> validOptions = new ArrayList<>(Arrays.asList(RangeExpression.values()));
//        option = Randomly.fromList(validOptions);
//        switch (option) {
//        case BINARY_OP:
//            return new HazelcastBinaryRangeOperation(HazelcastBinaryRangeOperator.getRandom(),
//                    generateExpression(depth + 1, HazelcastDataType.RANGE),
//                    generateExpression(depth + 1, HazelcastDataType.RANGE));
//        default:
//            throw new AssertionError(option);
//        }
//    }

    private enum TextExpression {
        CAST,
        FUNCTION,
        CONCAT,
        COLLATE
    }

    private HazelcastExpression generateTextExpression(int depth) {
        TextExpression option;
        //TODO: Enable CAST
        List<TextExpression> validOptions = new ArrayList<>(Arrays.asList(TextExpression.FUNCTION,
                TextExpression.CONCAT, TextExpression.COLLATE));
        if (expectedResult) {
            validOptions.remove(TextExpression.COLLATE);
        }
        if (!globalState.getDmbsSpecificOptions().testCollations) {
            validOptions.remove(TextExpression.COLLATE);
        }
        option = Randomly.fromList(validOptions);

        switch (option) {
            case CAST:
                return new HazelcastCastOperation(generateExpression(depth + 1), getCompoundDataType(HazelcastDataType.TEXT));
            case FUNCTION:
                return generateFunction(depth + 1, HazelcastDataType.TEXT);
            case CONCAT:
                return generateConcat(depth);
            case COLLATE:
                assert !expectedResult;
                return new HazelcastCollate(generateExpression(depth + 1, HazelcastDataType.TEXT), globalState == null
                        ? Randomly.fromOptions("C", "POSIX", "de_CH.utf8", "es_CR.utf8") : globalState.getRandomCollate());
            default:
                throw new AssertionError();
        }
    }

    private HazelcastExpression generateConcat(int depth) {
        HazelcastExpression left = generateExpression(depth + 1, HazelcastDataType.TEXT);
        HazelcastExpression right = generateExpression(depth + 1);
        return new HazelcastConcatOperation(left, right);
    }

    private enum BitExpression {
        BINARY_OPERATION
    }

    ;

    private enum IntExpression {
        UNARY_OPERATION, FUNCTION, CAST, BINARY_ARITHMETIC_EXPRESSION
    }

    private HazelcastExpression generateIntExpression(int depth) {
        IntExpression option;
        option = Randomly.fromOptions(IntExpression.BINARY_ARITHMETIC_EXPRESSION, IntExpression.UNARY_OPERATION, IntExpression.FUNCTION);
        switch (option) {
            case CAST:
                return new HazelcastCastOperation(generateExpression(depth + 1), getCompoundDataType(HazelcastDataType.INT));
            case UNARY_OPERATION:
                HazelcastExpression intExpression = generateExpression(depth + 1, HazelcastDataType.INT);
                return new HazelcastPrefixOperation(intExpression,
                        Randomly.getBoolean() ? PrefixOperator.UNARY_PLUS : PrefixOperator.UNARY_MINUS);
            case FUNCTION:
                return generateFunction(depth + 1, HazelcastDataType.INT);
            case BINARY_ARITHMETIC_EXPRESSION:
                return new HazelcastBinaryArithmeticOperation(generateExpression(depth + 1, HazelcastDataType.INT),
                        generateExpression(depth + 1, HazelcastDataType.INT), HazelcastBinaryOperator.getRandom());
            default:
                throw new AssertionError();
        }
    }

    private HazelcastExpression createColumnOfType(HazelcastDataType type) {
        List<HazelcastColumn> columns = filterColumns(type);
        HazelcastColumn fromList = Randomly.fromList(columns);
        HazelcastConstant value = rw == null ? null : rw.getValues().get(fromList);
        return HazelcastColumnValue.create(fromList, value);
    }

    final List<HazelcastColumn> filterColumns(HazelcastDataType type) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList());
        }
    }

    public HazelcastExpression generateExpressionWithExpectedResult(HazelcastDataType type) {
        this.expectedResult = true;
        HazelcastExpressionGenerator gen = new HazelcastExpressionGenerator(globalState).setColumns(columns)
                .setRowValue(rw);
        HazelcastExpression expr;
        do {
            expr = gen.generateExpression(type);
        } while (expr.getExpectedValue() == null);
        return expr;
    }

    public static HazelcastExpression generateConstant(Randomly r, HazelcastDataType type) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return HazelcastConstant.createNullConstant();
        }
        // if (Randomly.getBooleanWithSmallProbability()) {
        // return HazelcastConstant.createTextConstant(r.getString());
        // }
        switch (type) {
            case INT:
                return HazelcastConstant.createIntConstant(r.getInteger());
            case SMALLINT:
                return HazelcastConstant.createIntConstant(r.getSmallInt());
            case BOOLEAN:
//                if (Randomly.getBooleanWithSmallProbability() && !HazelcastProvider.generateOnlyKnown) {
//                    return HazelcastConstant
//                            .createTextConstant(Randomly.fromOptions("TR", "TRUE", "FA", "FALSE", "0", "1", "ON", "off"));
//                } else {
                return HazelcastConstant.createBooleanConstant(Randomly.getBoolean());
//                }
            case TEXT:
                return HazelcastConstant.createTextConstant(r.getString());
            case DECIMAL:
                return HazelcastConstant.createDecimalConstant(r.getRandomBigDecimal());
            case FLOAT:
                return HazelcastConstant.createFloatConstant((float) r.getDouble());
            default:
                throw new AssertionError(type);
        }
    }

    private static String getRandomInet(Randomly r) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            if (i != 0) {
                sb.append('.');
            }
            sb.append(r.getInteger() & 255);
        }
        return sb.toString();
    }

    public static HazelcastExpression generateExpression(HazelcastGlobalState globalState, List<HazelcastColumn> columns,
                                                         HazelcastDataType type) {
        return new HazelcastExpressionGenerator(globalState).setColumns(columns).generateExpression(0, type);
    }

    public static HazelcastExpression generateExpression(HazelcastGlobalState globalState, List<HazelcastColumn> columns) {
        return new HazelcastExpressionGenerator(globalState).setColumns(columns).generateExpression(0);

    }

    public List<HazelcastExpression> generateExpressions(int nr) {
        List<HazelcastExpression> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(0));
        }
        return expressions;
    }

    public HazelcastExpression generateExpression(HazelcastDataType dataType) {
        return generateExpression(0, dataType);
    }

    public HazelcastExpressionGenerator setGlobalState(HazelcastGlobalState globalState) {
        this.globalState = globalState;
        return this;
    }

    public HazelcastExpression generateHavingClause() {
        this.allowAggregateFunctions = true;
        HazelcastExpression expression = generateExpression(HazelcastDataType.BOOLEAN);
        this.allowAggregateFunctions = false;
        return expression;
    }

    public HazelcastExpression generateAggregate() {
        return getAggregate(HazelcastDataType.getRandomType());
    }

    private HazelcastExpression getAggregate(HazelcastDataType dataType) {
        List<HazelcastAggregateFunction> aggregates = HazelcastAggregateFunction.getAggregates(dataType);
        HazelcastAggregateFunction agg = Randomly.fromList(aggregates);
        return generateArgsForAggregate(dataType, agg);
    }

    public HazelcastAggregate generateArgsForAggregate(HazelcastDataType dataType, HazelcastAggregateFunction agg) {
        List<HazelcastDataType> types = agg.getTypes(dataType);
        List<HazelcastExpression> args = new ArrayList<>();
        for (HazelcastDataType argType : types) {
            args.add(generateExpression(argType));
        }
        return new HazelcastAggregate(args, agg);
    }

    public HazelcastExpressionGenerator allowAggregates(boolean value) {
        allowAggregateFunctions = value;
        return this;
    }

    @Override
    public HazelcastExpression generatePredicate() {
        return generateExpression(HazelcastDataType.BOOLEAN);
    }

    @Override
    public HazelcastExpression negatePredicate(HazelcastExpression predicate) {
        return new HazelcastPrefixOperation(predicate, PrefixOperator.NOT);
    }

    @Override
    public HazelcastExpression isNull(HazelcastExpression expr) {
        return new HazelcastPostfixOperation(expr, PostfixOperator.IS_NULL);
    }

}
