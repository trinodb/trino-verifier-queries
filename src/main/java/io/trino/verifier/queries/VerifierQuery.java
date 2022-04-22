package io.trino.verifier.queries;

import static java.util.Objects.requireNonNull;

public class VerifierQuery
{
    private final String suite;
    private final String name;
    private final String testCatalog;
    private final String testSchema;
    private final String testQuery;
    private final String controlCatalog;
    private final String controlSchema;
    private final String controlQuery;

    public VerifierQuery(
            String suite,
            String name,
            String testCatalog,
            String testSchema,
            String testQuery,
            String controlCatalog,
            String controlSchema,
            String controlQuery)
    {
        this.suite = requireNonNull(suite, "suite is null");
        this.name = requireNonNull(name, "name is null");
        this.testCatalog = requireNonNull(testCatalog, "testCatalog is null");
        this.testSchema = requireNonNull(testSchema, "testSchema is null");
        this.testQuery = requireNonNull(testQuery, "testQuery is null");
        this.controlCatalog = requireNonNull(controlCatalog, "controlCatalog is null");
        this.controlSchema = requireNonNull(controlSchema, "controlSchema is null");
        this.controlQuery = requireNonNull(controlQuery, "controlQuery is null");
    }

    public String getSuite()
    {
        return suite;
    }

    public String getName()
    {
        return name;
    }

    public String getTestCatalog()
    {
        return testCatalog;
    }

    public String getTestSchema()
    {
        return testSchema;
    }

    public String getTestQuery()
    {
        return testQuery;
    }

    public String getControlCatalog()
    {
        return controlCatalog;
    }

    public String getControlSchema()
    {
        return controlSchema;
    }

    public String getControlQuery()
    {
        return controlQuery;
    }
}
