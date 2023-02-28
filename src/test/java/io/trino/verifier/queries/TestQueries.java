package io.trino.verifier.queries;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static org.testng.Assert.assertEquals;

public class TestQueries
{
    private SqlParser parser;
    private Queries queries;

    @BeforeClass
    public void setup()
    {
        parser = new SqlParser();
        queries = new Queries();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        parser = null;
        queries = null;
    }

    @Test
    public void testQueries()
    {
        for (String queryId : queries.getAllIds()) {
            try {
                Query query = queries.get(queryId).orElseThrow();
                parse(query.getSql());
                if (query.getId().equals("test/q1")) {
                    assertEquals(query.getSessionProperties(), Map.of("testkey", "testvalue"));
                }
            }
            catch (RuntimeException e) {
                throw new AssertionError("Error parsing query: " + queryId, e);
            }
        }
    }

    private void parse(String sql)
    {
        parser.createStatement(sql, new ParsingOptions(AS_DECIMAL));
    }
}
