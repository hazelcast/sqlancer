package sqlancer.hazelcast.ast;

import sqlancer.common.visitor.UnaryOperation;

public class HazelcastAlias implements UnaryOperation<HazelcastExpression>, HazelcastExpression {

    private final HazelcastExpression expr;
    private final String alias;

    public HazelcastAlias(HazelcastExpression expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    @Override
    public HazelcastExpression getExpression() {
        return expr;
    }

    @Override
    public String getOperatorRepresentation() {
        return " as " + alias;
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

    @Override
    public boolean omitBracketsWhenPrinting() {
        return true;
    }

}
