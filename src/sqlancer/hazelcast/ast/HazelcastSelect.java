package sqlancer.hazelcast.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.SelectBase;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;
import sqlancer.hazelcast.HazelcastSchema.HazelcastTable;

import java.util.Collections;
import java.util.List;

public class HazelcastSelect extends SelectBase<HazelcastExpression> implements HazelcastExpression {

    private SelectType selectOption = SelectType.ALL;
    private List<HazelcastJoin> joinClauses = Collections.emptyList();
    private HazelcastExpression distinctOnClause;
    private ForClause forClause;

    public enum ForClause {
        UPDATE("UPDATE"), NO_KEY_UPDATE("NO KEY UPDATE"), SHARE("SHARE"), KEY_SHARE("KEY SHARE");

        private final String textRepresentation;

        ForClause(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static ForClause getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public static class HazelcastFromTable implements HazelcastExpression {
        private final HazelcastTable t;
        private final boolean only;

        public HazelcastFromTable(HazelcastTable t, boolean only) {
            this.t = t;
            this.only = only;
        }

        public HazelcastTable getTable() {
            return t;
        }

        public boolean isOnly() {
            return only;
        }

        @Override
        public HazelcastDataType getExpressionType() {
            return null;
        }
    }

    public static class HazelcastSubquery implements HazelcastExpression {
        private final HazelcastSelect s;
        private final String name;

        public HazelcastSubquery(HazelcastSelect s, String name) {
            this.s = s;
            this.name = name;
        }

        public HazelcastSelect getSelect() {
            return s;
        }

        public String getName() {
            return name;
        }

        @Override
        public HazelcastDataType getExpressionType() {
            return null;
        }
    }

    public enum SelectType {
        DISTINCT, ALL;

        public static SelectType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public void setSelectType(SelectType fromOptions) {
        this.setSelectOption(fromOptions);
    }

    public void setDistinctOnClause(HazelcastExpression distinctOnClause) {
        if (selectOption != SelectType.DISTINCT) {
            throw new IllegalArgumentException();
        }
        this.distinctOnClause = distinctOnClause;
    }

    public SelectType getSelectOption() {
        return selectOption;
    }

    public void setSelectOption(SelectType fromOptions) {
        this.selectOption = fromOptions;
    }

    @Override
    public HazelcastDataType getExpressionType() {
        return null;
    }

    public void setJoinClauses(List<HazelcastJoin> joinStatements) {
        this.joinClauses = joinStatements;

    }

    public List<HazelcastJoin> getJoinClauses() {
        return joinClauses;
    }

    public HazelcastExpression getDistinctOnClause() {
        return distinctOnClause;
    }

    public void setForClause(ForClause forClause) {
        this.forClause = forClause;
    }

    public ForClause getForClause() {
        return forClause;
    }

}
