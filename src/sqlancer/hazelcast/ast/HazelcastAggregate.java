package sqlancer.hazelcast.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.FunctionNode;
import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HazelcastAggregate extends FunctionNode<HazelcastAggregate.HazelcastAggregateFunction, HazelcastExpression>
        implements HazelcastExpression {

    public enum HazelcastAggregateFunction {
        AVG(HazelcastDataType.INTEGER, HazelcastDataType.FLOAT, HazelcastDataType.DOUBLE, HazelcastDataType.DECIMAL),
        COUNT(HazelcastDataType.INTEGER),
        MAX,
        MIN,
        SUM(HazelcastDataType.INTEGER, HazelcastDataType.FLOAT, HazelcastDataType.DOUBLE, HazelcastDataType.DECIMAL);

        private HazelcastDataType[] supportedReturnTypes;

        HazelcastAggregateFunction(HazelcastDataType... supportedReturnTypes) {
            this.supportedReturnTypes = supportedReturnTypes.clone();
        }

        public List<HazelcastDataType> getTypes(HazelcastDataType returnType) {
            return Arrays.asList(returnType);
        }

        public boolean supportsReturnType(HazelcastDataType returnType) {
            return Arrays.asList(supportedReturnTypes).stream().anyMatch(t -> t == returnType)
                    || supportedReturnTypes.length == 0;
        }

        public static List<HazelcastAggregateFunction> getAggregates(HazelcastDataType type) {
            return Arrays.asList(values()).stream().filter(p -> p.supportsReturnType(type))
                    .collect(Collectors.toList());
        }

        public HazelcastDataType getRandomReturnType() {
            if (supportedReturnTypes.length == 0) {
                return Randomly.fromOptions(HazelcastDataType.getRandomType());
            } else {
                return Randomly.fromOptions(supportedReturnTypes);
            }
        }

    }

    public HazelcastAggregate(List<HazelcastExpression> args, HazelcastAggregateFunction func) {
        super(func, args);
    }

}
