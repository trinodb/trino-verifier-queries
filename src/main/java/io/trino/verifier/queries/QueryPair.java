package io.trino.verifier.queries;

import static java.util.Objects.requireNonNull;

public class QueryPair
{
    private final String id;
    private final Query test;
    private final Query control;

    public QueryPair(String id, Query test, Query control)
    {
        this.id = requireNonNull(id, "id is null");
        this.test = requireNonNull(test, "test is null");
        this.control = requireNonNull(control, "control is null");
    }

    public String getId()
    {
        return id;
    }

    public Query getTest()
    {
        return test;
    }

    public Query getControl()
    {
        return control;
    }
}
