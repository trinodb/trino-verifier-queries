package io.trino.verifier.queries;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class VerifierQuery
{
    private final String suite;
    private final String name;
    private final String testCatalog;
    private final String testSchema;
    private final String testQuery;
    private final Optional<String> testSessionPropertiesJson;
    private final String controlCatalog;
    private final String controlSchema;
    private final String controlQuery;
    private final Optional<String> controlSessionPropertiesJson;

    public VerifierQuery(
            String suite,
            String name,
            String testCatalog,
            String testSchema,
            String testQuery,
            Optional<String> testSessionPropertiesJson,
            String controlCatalog,
            String controlSchema,
            String controlQuery,
            Optional<String> controlSessionPropertiesJson)
    {
        this.suite = requireNonNull(suite, "suite is null");
        this.name = requireNonNull(name, "name is null");
        this.testCatalog = requireNonNull(testCatalog, "testCatalog is null");
        this.testSchema = requireNonNull(testSchema, "testSchema is null");
        this.testQuery = requireNonNull(testQuery, "testQuery is null");
        this.testSessionPropertiesJson = requireNonNull(testSessionPropertiesJson, "testSessionPropertiesJson is null");
        this.controlCatalog = requireNonNull(controlCatalog, "controlCatalog is null");
        this.controlSchema = requireNonNull(controlSchema, "controlSchema is null");
        this.controlQuery = requireNonNull(controlQuery, "controlQuery is null");
        this.controlSessionPropertiesJson = requireNonNull(controlSessionPropertiesJson, "controlSessionPropertiesJson is null");
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

    public Optional<String> getTestSessionPropertiesJson()
    {
        return testSessionPropertiesJson;
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

    public Optional<String> getControlSessionPropertiesJson()
    {
        return controlSessionPropertiesJson;
    }
}
