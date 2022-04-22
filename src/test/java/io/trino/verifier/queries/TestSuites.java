package io.trino.verifier.queries;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

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
            Suite suite = suites.get(suiteId).orElseThrow();
            assertEquals(suite.getId(), suiteId);
        }
    }
}
