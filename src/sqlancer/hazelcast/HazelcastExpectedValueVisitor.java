package sqlancer.hazelcast;

import sqlancer.hazelcast.ast.*;
import sqlancer.hazelcast.ast.HazelcastSelect.HazelcastFromTable;
import sqlancer.hazelcast.ast.HazelcastSelect.HazelcastSubquery;

public final class HazelcastExpectedValueVisitor implements HazelcastVisitor {

    private final StringBuilder sb = new StringBuilder();
    private static final int NR_TABS = 0;

    private void print(HazelcastExpression expr) {
        HazelcastToStringVisitor v = new HazelcastToStringVisitor();
        v.visit(expr);
        for (int i = 0; i < NR_TABS; i++) {
            sb.append("\t");
        }
        sb.append(v.get());
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append("\n");
    }

    // @Override
    // public void visit(PostgresExpression expr) {
    // nrTabs++;
    // try {
    // super.visit(expr);
    // } catch (IgnoreMeException e) {
    //
    // }
    // nrTabs--;
    // }

    @Override
    public void visit(HazelcastConstant constant) {
        print(constant);
    }

    @Override
    public void visit(HazelcastPostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(HazelcastColumnValue c) {
        print(c);
    }

    @Override
    public void visit(HazelcastPrefixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(HazelcastSelect op) {
        visit(op.getWhereClause());
    }

    @Override
    public void visit(HazelcastOrderByTerm op) {

    }

    @Override
    public void visit(HazelcastFunction f) {
        print(f);
        for (int i = 0; i < f.getArguments().length; i++) {
            visit(f.getArguments()[i]);
        }
    }

    @Override
    public void visit(HazelcastCastOperation cast) {
        print(cast);
        visit(cast.getExpression());
    }

    @Override
    public void visit(HazelcastBetweenOperation op) {
        print(op);
        visit(op.getExpr());
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(HazelcastInOperation op) {
        print(op);
        visit(op.getExpr());
        for (HazelcastExpression right : op.getListElements()) {
            visit(right);
        }
    }

    @Override
    public void visit(HazelcastPostfixText op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(HazelcastAggregate op) {
        print(op);
        for (HazelcastExpression expr : op.getArgs()) {
            visit(expr);
        }
    }

    @Override
    public void visit(HazelcastPOSIXRegularExpression op) {
        print(op);
        visit(op.getString());
        visit(op.getRegex());
    }

    @Override
    public void visit(HazelcastFromTable from) {
        print(from);
    }

    @Override
    public void visit(HazelcastSubquery subquery) {
        print(subquery);
    }

    @Override
    public void visit(HazelcastBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(HazelcastLikeOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

}
