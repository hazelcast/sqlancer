package sqlancer.dbms;

import com.hazelcast.core.HazelcastInstance;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sqlancer.Main;
import sqlancer.hazelcast.HazelcastInstanceManager;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestHazelcast {

    boolean hazelcastIsAvailable = true;
    private static HazelcastInstance member;

    @BeforeAll
    public static void initHazelcast() {
        member = HazelcastInstanceManager.getInstance();
    }

    @Test
    public void testHazelcastPQS() {
        assumeTrue(hazelcastIsAvailable);
        assertEquals(0,
                Main.executeMain(
                        "--random-seed", String.valueOf(ThreadLocalRandom.current().nextLong()),
                        "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4",
                        "--num-queries", TestConfig.NUM_QUERIES,
                        "hazelcast", " --oracle", "PQS",
                        "--test-collations", "false")
        );

    }

    @Test
    public void testHazelcastTLP() {
        assumeTrue(hazelcastIsAvailable);
        assertEquals(0,
                Main.executeMain(
                        "--random-seed", String.valueOf(ThreadLocalRandom.current().nextLong()),
                        "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4",
                        "--num-queries", TestConfig.NUM_QUERIES,
                        "hazelcast", " --oracle", "WHERE",
                        "--test-collations", "false")
        );

    }

}
