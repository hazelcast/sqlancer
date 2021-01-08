package sqlancer.hazelcast;

import sqlancer.hazelcast.HazelcastSchema.HazelcastDataType;

import java.util.Optional;

public final class HazelcastCompoundDataType {

    private final HazelcastDataType dataType;
    private final HazelcastCompoundDataType elemType;
    private final Integer size;

    private HazelcastCompoundDataType(HazelcastDataType dataType, HazelcastCompoundDataType elemType, Integer size) {
        this.dataType = dataType;
        this.elemType = elemType;
        this.size = size;
    }

    public HazelcastDataType getDataType() {
        return dataType;
    }

    public HazelcastCompoundDataType getElemType() {
        if (elemType == null) {
            throw new AssertionError();
        }
        return elemType;
    }

    public Optional<Integer> getSize() {
        if (size == null) {
            return Optional.empty();
        } else {
            return Optional.of(size);
        }
    }

    public static HazelcastCompoundDataType create(HazelcastDataType type, int size) {
        return new HazelcastCompoundDataType(type, null, size);
    }

    public static HazelcastCompoundDataType create(HazelcastDataType type) {
        return new HazelcastCompoundDataType(type, null, null);
    }
}
