package sqlancer.hazelcast.ast;

import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.ast.HazelcastBinaryLogicalOperation.BinaryLogicalOperator;
import sqlancer.hazelcast.ast.HazelcastBinaryComparisonOperation.HazelcastBinaryComparisonOperator;

public final class HazelcastBetweenOperation implements HazelcastExpression {

    private final HazelcastExpression expr;
    private final HazelcastExpression left;
    private final HazelcastExpression right;
    private final boolean isSymmetric;

    public HazelcastBetweenOperation(HazelcastExpression expr, HazelcastExpression left, HazelcastExpression right,
                                     boolean symmetric) {
        this.expr = expr;
        this.left = left;
        this.right = right;
        isSymmetric = symmetric;
    }

    public HazelcastExpression getExpr() {
        return expr;
    }

    public HazelcastExpression getLeft() {
        return left;
    }

    public HazelcastExpression getRight() {
        return right;
    }

    public boolean isSymmetric() {
        return isSymmetric;
    }

    @Override
    public HazelcastConstant getExpectedValue() {
        HazelcastBinaryComparisonOperation leftComparison = new HazelcastBinaryComparisonOperation(left, expr,
                HazelcastBinaryComparisonOperator.LESS_EQUALS);
        HazelcastBinaryComparisonOperation rightComparison = new HazelcastBinaryComparisonOperation(expr, right,
                HazelcastBinaryComparisonOperator.LESS_EQUALS);
        HazelcastBinaryLogicalOperation andOperation = new HazelcastBinaryLogicalOperation(leftComparison,
                rightComparison, BinaryLogicalOperator.AND);
        if (isSymmetric) {
            HazelcastBinaryComparisonOperation leftComparison2 = new HazelcastBinaryComparisonOperation(right, expr,
                    HazelcastBinaryComparisonOperator.LESS_EQUALS);
            HazelcastBinaryComparisonOperation rightComparison2 = new HazelcastBinaryComparisonOperation(expr, left,
                    HazelcastBinaryComparisonOperator.LESS_EQUALS);
            HazelcastBinaryLogicalOperation andOperation2 = new HazelcastBinaryLogicalOperation(leftComparison2,
                    rightComparison2, BinaryLogicalOperator.AND);
            HazelcastBinaryLogicalOperation orOp = new HazelcastBinaryLogicalOperation(andOperation, andOperation2,
                    BinaryLogicalOperator.OR);
            return orOp.getExpectedValue();
        } else {
            return andOperation.getExpectedValue();
        }
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return HazelcastDataType.BOOLEAN;
    }

}
