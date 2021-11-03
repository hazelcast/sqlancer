package sqlancer.dbms;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.logging.LogListener;
import com.hazelcast.logging.LoggingService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sqlancer.Main;
import sqlancer.hazelcast.HazelcastInstanceManager;

import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestHazelcast {

    boolean hazelcastIsAvailable = true;
    private static HazelcastInstance member;

    @BeforeAll
    public static void initHazelcast() {
        LogListener listener = new LogListener() {
            public void log(LogEvent logEvent) {
            }
        };

        member = HazelcastInstanceManager.getInstance();
        LoggingService loggingService = member.getLoggingService();
        loggingService.addLogListener(Level.OFF, listener);
    }

    @Test
    public void testHazelcastPQS() {
        assumeTrue(hazelcastIsAvailable);
        assertEquals(0,
                Main.executeMain(
                        "--random-seed", String.valueOf(ThreadLocalRandom.current().nextLong()),
                        "--timeout-seconds", TestConfig.JOB_SECONDS,
                        "--num-threads", "16",
                        "--num-queries", TestConfig.NUM_QUERIES,
                        "hazelcast",
                        " --oracle", "PQS",
                        "--test-collations", "false")
        );

    }

    @Test
    public void testHazelcastTLP() {
        assumeTrue(hazelcastIsAvailable);
        assertEquals(0,
                Main.executeMain(
                        "--random-seed", String.valueOf(ThreadLocalRandom.current().nextLong()),
                        "--timeout-seconds", TestConfig.JOB_SECONDS,
                        "--num-threads", "16",
//                        "--num-queries", TestConfig.NUM_QUERIES,
                        "--max-num-inserts", "2500",
                        "hazelcast",
                        " --oracle", "WHERE",
                        "--test-collations", "false")
        );

    }

}
