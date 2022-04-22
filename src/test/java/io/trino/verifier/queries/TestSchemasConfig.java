package io.trino.verifier.queries;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSchemasConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SchemasConfig.class)
                .setTrinoUrl(null)
                .setTrinoUsername("verifier")
                .setTrinoPassword(null)
                .setForceWritePartitioning(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("trino-url", "jdbc:trino://localhost:8080")
                .put("trino-username", "someusername")
                .put("trino-password", "somepassword")
                .put("force-write-partitioning", "false")
                .buildOrThrow();

        SchemasConfig expected = new SchemasConfig()
                .setTrinoUrl("jdbc:trino://localhost:8080")
                .setTrinoUsername("someusername")
                .setTrinoPassword("somepassword")
                .setForceWritePartitioning(false);

        assertFullMapping(properties, expected);
    }
}
