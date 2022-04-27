package io.trino.verifier.queries;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class Suite
{
    private final String id;
    private final List<QueryPair> queries;

    public Suite(String id, List<QueryPair> queries)
    {
        this.id = requireNonNull(id, "id is null");
        this.queries = List.copyOf(requireNonNull(queries, "queries is null"));
    }

    public String getId()
    {
        return id;
    }

    public List<QueryPair> getQueries()
    {
        return queries;
    }
}
