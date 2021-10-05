package sqlancer.hazelcast;

import sqlancer.Randomly;
import sqlancer.common.visitor.BinaryOperation;
import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.hazelcast.ast.*;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.ast.HazelcastJoin.HazelcastJoinType;
import sqlancer.hazelcast.ast.HazelcastSelect.HazelcastFromTable;
import sqlancer.hazelcast.ast.HazelcastSelect.HazelcastSubquery;

import java.util.Optional;

public final class HazelcastToStringVisitor extends ToStringVisitor<HazelcastExpression> implements HazelcastVisitor {

    @Override
    public void visitSpecific(HazelcastExpression expr) {
        HazelcastVisitor.super.visit(expr);
    }

    @Override
    public void visit(HazelcastConstant constant) {
        sb.append(constant.getTextRepresentation());
    }

    @Override
    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(HazelcastPostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getOperatorTextRepresentation());
    }

    @Override
    public void visit(HazelcastColumnValue c) {
        sb.append(c.getColumn().getFullQualifiedName());
    }

    @Override
    public void visit(HazelcastPrefixOperation op) {
        sb.append(op.getTextRepresentation());
        sb.append(" (");
        visit(op.getExpression());
        sb.append(")");
    }

    @Override
    public void visit(HazelcastFromTable from) {
        sb.append(from.getTable().getName());
//        if (!from.isOnly() && Randomly.getBoolean()) {
//            sb.append("*");
//        }
    }

    @Override
    public void visit(HazelcastSubquery subquery) {
        sb.append("(");
        visit(subquery.getSelect());
        sb.append(") AS ");
        sb.append(subquery.getName());
    }

    @Override
    public void visit(HazelcastSelect s) {
        sb.append("SELECT ");
        if (s.getFetchColumns() == null) {
            sb.append("*");
        } else {
            visit(s.getFetchColumns());
        }
        sb.append(" FROM ");
        visit(s.getFromList());

        for (HazelcastJoin j : s.getJoinClauses()) {
            sb.append(" ");
            //noinspection SwitchStatementWithTooFewBranches
            switch (j.getType()) {
                case INNER:
                    if (Randomly.getBoolean()) {
                        sb.append("INNER ");
                    }
                    sb.append("JOIN");
                    break;
//                case LEFT:
//                    sb.append("LEFT OUTER JOIN");
//                    break;
//                case RIGHT:
//                    sb.append("RIGHT OUTER JOIN");
//                    break;
//                case FULL:
//                    sb.append("FULL OUTER JOIN");
//                    break;
//                case CROSS:
//                    sb.append("CROSS JOIN");
//                    break;
                default:
                    throw new AssertionError(j.getType());
            }
            sb.append(" ");
            visit(j.getTableReference());
            if (j.getType() != HazelcastJoinType.CROSS) {
                sb.append(" ON ");
                visit(j.getOnClause());
            }
        }

        if (s.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(s.getWhereClause());
        }
        if (s.getGroupByExpressions().size() > 0) {
            sb.append(" GROUP BY ");
            visit(s.getGroupByExpressions());
        }
        if (s.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(s.getHavingClause());

        }
        if (!s.getOrderByExpressions().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(s.getOrderByExpressions());
        }
        if (s.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(s.getLimitClause());
        }

        if (s.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(s.getOffsetClause());
        }
    }

    @Override
    public void visit(HazelcastOrderByTerm op) {
        visit(op.getExpr());
        sb.append(" ");
        sb.append(op.getOrder());
    }

    @Override
    public void visit(HazelcastFunction f) {
        sb.append(f.getFunctionName());
        sb.append("(");
        int i = 0;
        for (HazelcastExpression arg : f.getArguments()) {
            if (i++ != 0) {
                sb.append(", ");
            }
            visit(arg);
        }
        sb.append(")");
    }

    @Override
    public void visit(HazelcastCastOperation cast) {
        sb.append("CAST(");
        visit(cast.getExpression());
        sb.append(" AS ");
        appendType(cast);
        sb.append(")");
    }

    private void appendType(HazelcastCastOperation cast) {
        HazelcastCompoundDataType compoundType = cast.getCompoundType();
        switch (compoundType.getDataType()) {
            case BOOLEAN:
                sb.append("BOOLEAN");
                break;
//            case TINYINT:
//                sb.append("TINYINT");
//                break;
//            case SMALLINT:
//                sb.append("SMALLINT");
//                break;
            case INTEGER: // TODO support also other int types
                sb.append("INTEGER");
                break;
            case VARCHAR:
                sb.append("VARCHAR");
                break;
            case FLOAT:
                sb.append("FLOAT");
                break;
            case DOUBLE:
                sb.append("DOUBLE");
                break;
            case DECIMAL:
                sb.append("DECIMAL");
                break;
            default:
                throw new AssertionError(cast.getType());
        }
        Optional<Integer> size = compoundType.getSize();
        if (size.isPresent()) {
            sb.append("(");
            sb.append(size.get());
            sb.append(")");
        }
    }

    @Override
    public void visit(HazelcastBetweenOperation op) {
        sb.append("(");
        visit(op.getExpr());
        if (HazelcastProvider.generateOnlyKnown && op.getExpr().getExpressionType() == HazelcastDataType.VARCHAR
                && op.getLeft().getExpressionType() == HazelcastDataType.VARCHAR) {
            sb.append(" COLLATE \"C\"");
        }
        sb.append(") BETWEEN ");
        if (op.isSymmetric()) {
            sb.append("SYMMETRIC ");
        }
        sb.append("(");
        visit(op.getLeft());
        sb.append(") AND (");
        visit(op.getRight());
        if (HazelcastProvider.generateOnlyKnown && op.getExpr().getExpressionType() == HazelcastDataType.VARCHAR
                && op.getRight().getExpressionType() == HazelcastDataType.VARCHAR) {
            sb.append(" COLLATE \"C\"");
        }
        sb.append(")");
    }

    @Override
    public void visit(HazelcastInOperation op) {
        sb.append("(");
        visit(op.getExpr());
        sb.append(")");
        if (!op.isTrue()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        visit(op.getListElements());
        sb.append(")");
    }

    @Override
    public void visit(HazelcastPostfixText op) {
        visit(op.getExpr());
        sb.append(op.getText());
    }

    @Override
    public void visit(HazelcastAggregate op) {
        sb.append(op.getFunction());
        sb.append("(");
        visit(op.getArgs());
        sb.append(")");
    }

    @Override
    public void visit(HazelcastPOSIXRegularExpression op) {
        visit(op.getString());
        sb.append(op.getOp().getStringRepresentation());
        visit(op.getRegex());
    }

    @Override
    public void visit(HazelcastBinaryLogicalOperation op) {
        super.visit((BinaryOperation<HazelcastExpression>) op);
    }

    @Override
    public void visit(HazelcastLikeOperation op) {
        super.visit((BinaryOperation<HazelcastExpression>) op);
    }

}
