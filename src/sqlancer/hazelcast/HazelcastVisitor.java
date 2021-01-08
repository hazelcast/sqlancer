package sqlancer.hazelcast;

import sqlancer.hazelcast.ast.*;
import sqlancer.hazelcast.gen.HazelcastExpressionGenerator;
import sqlancer.hazelcast.HazelcastSchema.HazelcastColumn;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.ast.HazelcastSelect.HazelcastFromTable;
import sqlancer.hazelcast.ast.HazelcastSelect.HazelcastSubquery;

import java.util.List;

public interface HazelcastVisitor {

    void visit(HazelcastConstant constant);

    void visit(HazelcastPostfixOperation op);

    void visit(HazelcastColumnValue c);

    void visit(HazelcastPrefixOperation op);

    void visit(HazelcastSelect op);

    void visit(HazelcastOrderByTerm op);

    void visit(HazelcastFunction f);

    void visit(HazelcastCastOperation cast);

    void visit(HazelcastBetweenOperation op);

    void visit(HazelcastInOperation op);

    void visit(HazelcastPostfixText op);

    void visit(HazelcastAggregate op);

    void visit(HazelcastSimilarTo op);

    void visit(HazelcastCollate op);

    void visit(HazelcastPOSIXRegularExpression op);

    void visit(HazelcastFromTable from);

    void visit(HazelcastSubquery subquery);

    void visit(HazelcastBinaryLogicalOperation op);

    void visit(HazelcastLikeOperation op);

    default void visit(HazelcastExpression expression) {
        if (expression instanceof HazelcastConstant) {
            visit((HazelcastConstant) expression);
        } else if (expression instanceof HazelcastPostfixOperation) {
            visit((HazelcastPostfixOperation) expression);
        } else if (expression instanceof HazelcastColumnValue) {
            visit((HazelcastColumnValue) expression);
        } else if (expression instanceof HazelcastPrefixOperation) {
            visit((HazelcastPrefixOperation) expression);
        } else if (expression instanceof HazelcastSelect) {
            visit((HazelcastSelect) expression);
        } else if (expression instanceof HazelcastOrderByTerm) {
            visit((HazelcastOrderByTerm) expression);
        } else if (expression instanceof HazelcastFunction) {
            visit((HazelcastFunction) expression);
        } else if (expression instanceof HazelcastCastOperation) {
            visit((HazelcastCastOperation) expression);
        } else if (expression instanceof HazelcastBetweenOperation) {
            visit((HazelcastBetweenOperation) expression);
        } else if (expression instanceof HazelcastInOperation) {
            visit((HazelcastInOperation) expression);
        } else if (expression instanceof HazelcastAggregate) {
            visit((HazelcastAggregate) expression);
        } else if (expression instanceof HazelcastPostfixText) {
            visit((HazelcastPostfixText) expression);
        } else if (expression instanceof HazelcastSimilarTo) {
            visit((HazelcastSimilarTo) expression);
        } else if (expression instanceof HazelcastPOSIXRegularExpression) {
            visit((HazelcastPOSIXRegularExpression) expression);
        } else if (expression instanceof HazelcastCollate) {
            visit((HazelcastCollate) expression);
        } else if (expression instanceof HazelcastFromTable) {
            visit((HazelcastFromTable) expression);
        } else if (expression instanceof HazelcastSubquery) {
            visit((HazelcastSubquery) expression);
        } else if (expression instanceof HazelcastLikeOperation) {
            visit((HazelcastLikeOperation) expression);
        } else {
            throw new AssertionError(expression);
        }
    }

    static String asString(HazelcastExpression expr) {
        HazelcastToStringVisitor visitor = new HazelcastToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(HazelcastExpression expr) {
        HazelcastExpectedValueVisitor v = new HazelcastExpectedValueVisitor();
        v.visit(expr);
        return v.get();
    }

    static String getExpressionAsString(HazelcastGlobalState globalState, HazelcastDataType type,
                                        List<HazelcastColumn> columns) {
        HazelcastExpression expression = HazelcastExpressionGenerator.generateExpression(globalState, columns, type);
        HazelcastToStringVisitor visitor = new HazelcastToStringVisitor();
        visitor.visit(expression);
        return visitor.get();
    }

}
