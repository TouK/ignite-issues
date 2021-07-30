package pl.touk.ignite.issues;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.*;
import pl.touk.ignite.issues.testutil.IgniteTestUtil;
import pl.touk.ignite.issues.testutil.PortFinder;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class TimestampIssueTest {

    static final String DATE_NOW_STRING = "2021-07-30T08:55:07";
    static final String DATE_NOW_STRING_H2 = "2021-07-30 08:55:07";
    static long NOW_MILLIS = LocalDateTime.parse(DATE_NOW_STRING).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();

    static final String CACHE_NAME = "MY_CACHE";
    static final String TABLE_NAME = "MY_CACHE_TABLE";
    static final String KEY_TYPE = "MY_CACHE_KEY_TYPE";
    static final String VALUE_TYPE = "MY_CACHE_VALUE_TYPE";
    static final String KEY_FIRST_FIELD_NAME = "MY_CACHE_FIRST_KEY";
    static final String KEY_SECOND_FIELD_NAME = "MY_CACHE_SECOND_KEY";
    static final String VALUE_FIELD_NAME = "MY_CACHE_VALUE";
    static final String KEY_FIRST_1 = "key1";
    static final java.sql.Timestamp KEY_SECOND_1 = new java.sql.Timestamp(NOW_MILLIS);
    static final String VALUE_1 = "value1";
    static final String KEY_FIRST_2 = "key2";
    static final String VALUE_2 = "value2";



    static int ignitePort;
    static int clientConnectorPort;
    static Ignite igniteServer;
    static Connection sqlConn;
    static IgniteCache<BinaryObject, BinaryObject> thickClientCache;

    @Test
    public void testWithQueryParam() throws SQLException {
        PreparedStatement query1 = sqlConn.prepareStatement(
        "SELECT " + VALUE_FIELD_NAME + " FROM " + TABLE_NAME + " WHERE " + KEY_FIRST_FIELD_NAME + " = ? AND " + KEY_SECOND_FIELD_NAME + " = ?"
        );
        query1.setString(1, KEY_FIRST_1);
        query1.setTimestamp(2, new java.sql.Timestamp(NOW_MILLIS));
        ResultSet result1 = query1.executeQuery();
        assert (result1.next());
        assertEquals(VALUE_1, result1.getString(VALUE_FIELD_NAME));

        PreparedStatement query2 = sqlConn.prepareStatement(
                "SELECT " + VALUE_FIELD_NAME + " FROM " + TABLE_NAME
                        + " WHERE " + KEY_FIRST_FIELD_NAME + " = ? AND " + KEY_SECOND_FIELD_NAME + " = ?"
        );
        query2.setString(1, KEY_FIRST_2);
        query2.setTimestamp(2, new java.sql.Timestamp(NOW_MILLIS));
        ResultSet result2 = query2.executeQuery();
        assert (result2.next());
        assertEquals(VALUE_2, result2.getString(VALUE_FIELD_NAME));
    }


    @Test
    public void testWithHardcodedTimestamp() throws SQLException {
        PreparedStatement query1 = sqlConn.prepareStatement(
        "SELECT " + VALUE_FIELD_NAME +
                " FROM " + TABLE_NAME +
                " WHERE " + KEY_FIRST_FIELD_NAME + " = ? AND " + KEY_SECOND_FIELD_NAME + " = TIMESTAMP '" + DATE_NOW_STRING_H2 + "'"
        );
        query1.setString(1, KEY_FIRST_1);
        ResultSet resultSet1 = query1.executeQuery();
        // !!!!
        assert (resultSet1.next());
        assertEquals(VALUE_1, resultSet1.getString(VALUE_FIELD_NAME));

        PreparedStatement query2 = sqlConn.prepareStatement(
                "SELECT " + VALUE_FIELD_NAME +
                        " FROM " + TABLE_NAME +
                        " WHERE " + KEY_FIRST_FIELD_NAME + " = ? AND " + KEY_SECOND_FIELD_NAME + " = TIMESTAMP '" + DATE_NOW_STRING_H2 + "'"
        );
        query2.setString(1, KEY_FIRST_2);
        ResultSet resultSet2 = query2.executeQuery();
        // !!!!
        assert (resultSet2.next());
        assertEquals(VALUE_2, resultSet1.getString(VALUE_FIELD_NAME));
    }

    @BeforeClass
    public static void setupIgnite() throws IOException, SQLException {
        ignitePort = PortFinder.getAvailablePort();
        clientConnectorPort = PortFinder.getAvailablePort();
        CacheConfiguration<BinaryObject, BinaryObject> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration
                .setName(CACHE_NAME)
                .setSqlSchema("PUBLIC");

        LinkedHashMap<String, String> queryEntityFields = new LinkedHashMap<>();
        queryEntityFields.put(KEY_FIRST_FIELD_NAME, "java.lang.String");
        queryEntityFields.put(KEY_SECOND_FIELD_NAME, "java.sql.Timestamp");
        queryEntityFields.put(VALUE_FIELD_NAME, "java.lang.String");

        QueryEntity queryEntity = new QueryEntity()
                .setTableName(TABLE_NAME)
                .setKeyType(KEY_TYPE)
                .setValueType(VALUE_TYPE)
                .setKeyFields(new HashSet<>(Arrays.asList(KEY_FIRST_FIELD_NAME, KEY_SECOND_FIELD_NAME)))
                .setFields(queryEntityFields);

        cacheConfiguration.setQueryEntities(Collections.singletonList(queryEntity));
        igniteServer = IgniteTestUtil.startServer(ignitePort, clientConnectorPort, cacheConfiguration);

        thickClientCache = igniteServer.cache(CACHE_NAME).withKeepBinary();

        sqlConn = DriverManager.getConnection("jdbc:ignite:thin://localhost:" + clientConnectorPort);

        BinaryObjectBuilder keyBuilder = igniteServer.binary().builder(KEY_TYPE);
        BinaryObjectBuilder valueBuilder = igniteServer.binary().builder(VALUE_TYPE);
        BinaryObject binaryKey = keyBuilder
                .setField(KEY_FIRST_FIELD_NAME, KEY_FIRST_1)
                .setField(KEY_SECOND_FIELD_NAME, KEY_SECOND_1)
                .build();
        BinaryObject binaryValue = valueBuilder
                .setField(KEY_FIRST_FIELD_NAME, KEY_FIRST_1)
                .setField(KEY_SECOND_FIELD_NAME, KEY_SECOND_1)
                .setField(VALUE_FIELD_NAME, VALUE_1)
                .build();

        thickClientCache.put(binaryKey, binaryValue);

        PreparedStatement insert2EntryStatement = sqlConn.prepareStatement("INSERT INTO " + TABLE_NAME + " VALUES (?, ? , ?)");
        insert2EntryStatement.setString(1, KEY_FIRST_2);
        insert2EntryStatement.setTimestamp(2, new java.sql.Timestamp(NOW_MILLIS));
        insert2EntryStatement.setString(3, VALUE_2);
        insert2EntryStatement.execute();
    }
}
