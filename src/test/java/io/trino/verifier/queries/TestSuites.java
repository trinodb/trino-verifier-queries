package io.trino.verifier.queries;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestSuites
{
    private Suites suites;

    @BeforeClass
    public void setup()
    {
        suites = new Suites(new Queries());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        suites = null;
    }

    @Test
    public void testSuites()
    {
        for (String suiteId : suites.getAllIds()) {
            Suite suite = suites.get(suiteId, Optional.empty()).orElseThrow();
            assertEquals(suite.getId(), suiteId);

            suite = suites.get(suiteId, Optional.of("tiny")).orElseThrow();
            assertEquals(suite.getId(), suiteId);
        }
    }
}
