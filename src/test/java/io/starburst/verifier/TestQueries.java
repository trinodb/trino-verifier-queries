package io.starburst.verifier;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;

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
                parse(queries.get(queryId).orElseThrow().getSql());
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
