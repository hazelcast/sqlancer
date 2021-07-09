package sqlancer.dbms;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sqlancer.Main;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestHazelcast {

    boolean hazalcastIsAvailable = true;
    private static HazelcastInstance member;

    @BeforeAll
    public static void initHazelcast() {
        member = Hazelcast.bootstrappedInstance();
    }

    @Test
    public void testHazelcast() {
        assumeTrue(hazalcastIsAvailable);
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--num-queries", TestConfig.NUM_QUERIES, "hazelcast", "--test-collations",
                        "false" }));


//        assertEquals(0,
//                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
//                        "--num-threads", "1", "--print-statements", "true", "--print-succeeding-statements", "true",
//                        "--num-queries", TestConfig.NUM_QUERIES, "hazelcast", "--test-collations", "false"}));
    }

    @AfterAll
    public static void tearDown(){
        System.out.println("Shut down");
        member.shutdown();
    }

}
