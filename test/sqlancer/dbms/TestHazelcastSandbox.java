package sqlancer.dbms;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestHazelcastSandbox {

    public static final String MAP_NAME = "test_map";

    boolean hazalcastIsAvailable = true;
    private static HazelcastInstance member;

    @BeforeAll
    public static void initHazelcast() {
        member = Hazelcast.newHazelcastInstance();
    }

    @Test
    public void testHazelcast() {
        assumeTrue(hazalcastIsAvailable);
//        IMap<String, String> testMap = member.getMap(MAP_NAME);
        addIndexing();


        IMap<String, Pojo> testMap = member.getMap("test_map");

        member.getSql().execute("CREATE MAPPING test_map (" +
                "name VARCHAR, " +
                "age INT) " +
                "TYPE IMap " +
                "OPTIONS ( " +
                "'keyFormat'='bigint'," +
                "'valueFormat'='json'" +
                ")");

        for(int x = 0; x < 10; x++) {
            testMap.put("Object" + x, new Pojo(x));
        }

//        for(int x = 0; x < 10; x++) {
//            testMap.put("Object" + x, "Name" + x);
//        }

        String query = "SELECT * " +
                "FROM information_schema.columns";
        SqlResult sqlResult = member.getSql().execute(query);
        printSqlResult(sqlResult);

    }

    private void printSqlResult(SqlResult sqlResult) {
        Iterator<SqlRow> iterator = sqlResult.iterator();
        while(iterator.hasNext()) {
            iterator.next().getMetadata();
        }
    }

    protected void addIndexing() {
        IndexConfig indexConfig = new IndexConfig().setName("Index_" + UuidUtil.newUnsecureUuidString())
                .setType(IndexType.SORTED);

        for (String fieldName : getFieldNamesForIndexing()) {
            indexConfig.addAttribute(fieldName);
        }

        member.getMap(MAP_NAME).addIndex(indexConfig);
    }

    protected static List<String> getFieldNamesForIndexing() {
        return Arrays.asList(
                "name",
                "age"
        );
    }

}
