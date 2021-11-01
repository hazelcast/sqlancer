package sqlancer.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public final class HazelcastInstanceManager {

    public static volatile HazelcastInstance HAZELCAST_INSTANCE;

    private HazelcastInstanceManager() {
    }

    public static HazelcastInstance getInstance() {
        if (HAZELCAST_INSTANCE == null) {
            System.out.println("Create hazelcast instance");
            HAZELCAST_INSTANCE = Hazelcast.getOrCreateHazelcastInstance(config());
        }
        return HAZELCAST_INSTANCE;
    }

    private static Config config() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true);
        String randomName = String.valueOf(System.currentTimeMillis());
        config.setInstanceName(randomName);
        return config;
    }
}
