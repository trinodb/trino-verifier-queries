package io.trino.verifier.queries;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestSuitesConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SuitesConfig.class)
                .setVerifierDatabase(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("verifier-database", "database")
                .buildOrThrow();

        SuitesConfig expected = new SuitesConfig()
                .setVerifierDatabase("database");

        assertFullMapping(properties, expected);
    }
}
