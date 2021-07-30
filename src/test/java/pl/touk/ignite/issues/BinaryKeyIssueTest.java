package pl.touk.ignite.issues;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.junit.*;
import pl.touk.ignite.issues.testutil.IgniteTestUtil;
import pl.touk.ignite.issues.testutil.PortFinder;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class BinaryKeyIssueTest {

    private static final String CACHE_NAME = "MY_CACHE";
    private static final String TABLE_NAME = "MY_CACHE_TABLE";
    private static final String KEY_TYPE = "MY_CACHE_KEY_TYPE";
    private static final String VALUE_TYPE = "MY_CACHE_VALUE_TYPE";
    private static final String KEY_FIELD_NAME = "MY_CACHE_KEY";
    private static final String VALUE_FIELD_NAME = "MY_CACHE_VALUE";
    private static final String KEY_1 = "key1";
    private static final String VALUE_1 = "value1";

    private static int ignitePort;
    private static int clientConnectorPort;
    private static Ignite igniteServer;
    private static Connection sqlConn;
    private static IgniteCache<BinaryObject, BinaryObject> thickClientCache;

    @Test
    public void testGetThick() {
        BinaryObjectBuilder keyBuilder = igniteServer.binary().builder(KEY_TYPE);
        BinaryObject binaryKey = keyBuilder
                .setField(KEY_FIELD_NAME, KEY_1)
                .build();
        assertEquals(VALUE_1, thickClientCache.get(binaryKey).field(VALUE_FIELD_NAME));
    }

    @Test
    public void testGetThinJdbc() throws SQLException {
        PreparedStatement query = sqlConn.prepareStatement("SELECT " + VALUE_FIELD_NAME + " FROM " + TABLE_NAME + " WHERE " + KEY_FIELD_NAME + " = ?");
        query.setString(1, KEY_1);
        ResultSet resultSet = query.executeQuery();
        assert (resultSet.next());
        assertEquals(VALUE_1, resultSet.getString(VALUE_FIELD_NAME));
    }

    @Test
    public void testGetThinWithThickKey() {
        ClientConfiguration clientConfig = new ClientConfiguration().setAddresses("127.0.0.1:" + clientConnectorPort);
        IgniteClient igniteClient = Ignition.startClient(clientConfig);
        ClientCache<BinaryObject, BinaryObject> clientCache = igniteClient.cache(CACHE_NAME).withKeepBinary();

        // using: org.apache.ignite.internal.processors.cache.binary.IgniteBinaryImpl
        BinaryObjectBuilder thickKeyBuilder = igniteServer.binary().builder(KEY_TYPE);
        BinaryObject binaryKey = thickKeyBuilder
                .setField(KEY_FIELD_NAME, KEY_1)
                .build();

        BinaryObject binaryObject = clientCache.get(binaryKey);
        assertEquals(VALUE_1, binaryObject.field(VALUE_FIELD_NAME));
    }

    @Test
    public void testGetThinWithThinKey() {
        ClientConfiguration clientConfig = new ClientConfiguration().setAddresses("127.0.0.1:" + clientConnectorPort);
        IgniteClient igniteClient = Ignition.startClient(clientConfig);
        ClientCache<BinaryObject, BinaryObject> clientCache = igniteClient.cache(CACHE_NAME).withKeepBinary();

        // using: org.apache.ignite.internal.client.thin.ClientBinary
        BinaryObjectBuilder thinKeyBuilder = igniteClient.binary().builder(KEY_TYPE);
        BinaryObject binaryKey = thinKeyBuilder
                .setField(KEY_FIELD_NAME, KEY_1)
                .build();

        BinaryObject binaryObject = clientCache.get(binaryKey);
        assertEquals(VALUE_1, binaryObject.field(VALUE_FIELD_NAME));
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
        queryEntityFields.put(KEY_FIELD_NAME, "java.lang.String");
        queryEntityFields.put(VALUE_FIELD_NAME, "java.lang.String");

        QueryEntity queryEntity = new QueryEntity()
                .setTableName(TABLE_NAME)
                .setKeyType(KEY_TYPE)
                .setValueType(VALUE_TYPE)
                .setKeyFields(new HashSet<>(Arrays.asList(KEY_FIELD_NAME)))
                .setFields(queryEntityFields);

        cacheConfiguration.setQueryEntities(Collections.singletonList(queryEntity));
        igniteServer = IgniteTestUtil.startServer(ignitePort, clientConnectorPort, cacheConfiguration);

        thickClientCache = igniteServer.cache(CACHE_NAME).withKeepBinary();

        sqlConn = DriverManager.getConnection("jdbc:ignite:thin://localhost:" + clientConnectorPort);

        BinaryObjectBuilder keyBuilder = igniteServer.binary().builder(KEY_TYPE);
        BinaryObjectBuilder valueBuilder = igniteServer.binary().builder(VALUE_TYPE);
        BinaryObject binaryKey = keyBuilder
                .setField(KEY_FIELD_NAME, KEY_1)
                .build();
        BinaryObject binaryValue = valueBuilder
                .setField(KEY_FIELD_NAME, KEY_1)
                .setField(VALUE_FIELD_NAME, VALUE_1)
                .build();

        thickClientCache.put(binaryKey, binaryValue);
    }
}
