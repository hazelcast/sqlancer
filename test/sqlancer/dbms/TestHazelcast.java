package sqlancer.dbms;

import org.junit.jupiter.api.Test;
import sqlancer.Main;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestHazelcast {

    boolean hazalcastIsAvailable = true;

    @Test
    public void testHazelcast() {
        assumeTrue(hazalcastIsAvailable);
        assertEquals(0,
                Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "4", "--num-queries", TestConfig.NUM_QUERIES, "hazelcast", "--test-collations",
                        "false" }));
    }

}
